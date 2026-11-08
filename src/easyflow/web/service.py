from __future__ import annotations

import sqlite3
import threading
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

from easyflow._internal.dag_parser import (
    AsyncDagDiscovery,
    DagDiscoveryResult,
    DagSource,
    definition_hash_for_dag,
)
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.cron import cron_next_at_or_after
from easyflow._internal.schedules import (
    _expression,
    dag_timezone,
    next_logical_date,
    scheduler_intervals,
)
from easyflow._internal.state_store import SQLiteStateStore


class WebService:
    def __init__(self, dags_folder: str, database_url: str, log_root: str) -> None:
        self.dags_folder = Path(dags_folder).resolve()
        self.log_root = Path(log_root).resolve()
        self.store = SQLiteStateStore(database_url)
        self.store.init_db()
        self._lock = threading.Lock()
        self.discovery = AsyncDagDiscovery(self.dags_folder)

    def discover(self) -> DagDiscoveryResult:
        with self._lock:
            return self.discovery.scan(wait_seconds=0.5)

    def source(self, dag_id: str) -> DagSource:
        source = next(
            (item for item in self.discover().sources if item.dag_id == dag_id), None
        )
        if source is None:
            raise LookupError(f"找不到 DAG：{dag_id}")
        return source

    def rows(self, query: str, params: tuple = ()) -> list[dict]:
        with sqlite3.connect(self.store.path) as connection:
            connection.row_factory = sqlite3.Row
            return [dict(row) for row in connection.execute(query, params).fetchall()]

    def scheduler_running(self) -> bool:
        return self.scheduler_status()["running"]

    def scheduler_status(self) -> dict:
        rows = self.rows(
            "SELECT lease_expires_at, updated_at FROM scheduler_lease WHERE lease_id = 'singleton'"
        )
        running = bool(
            rows
            and datetime.fromisoformat(rows[0]["lease_expires_at"])
            > datetime.now(timezone.utc)
        )
        return {
            "running": running,
            "last_heartbeat_at": rows[0]["updated_at"] if rows else None,
            "lease_expires_at": rows[0]["lease_expires_at"] if rows else None,
        }

    def next_schedule(self, dag: object, paused: bool) -> dict:
        schedule = dag.schedule
        if schedule is None:
            return {"status": "manual", "at": None}
        if paused:
            return {"status": "paused", "at": None}
        now = datetime.now(timezone.utc)
        last = self.store.latest_automated_logical_date(dag.dag_id)
        if schedule == "@once" and last is not None:
            return {"status": "completed", "at": None}
        try:
            due = scheduler_intervals(
                dag, now, last_automated_date=last, max_intervals=1
            )
            if due:
                candidate = due[0].logical_date
            elif dag.end_date and now > dag.end_date.astimezone(timezone.utc):
                return {"status": "completed", "at": None}
            elif schedule == "@once":
                candidate = dag.start_date or now
            elif last is not None:
                candidate = next_logical_date(
                    schedule, last.astimezone(dag_timezone(dag))
                )
                if dag.start_date and candidate.astimezone(
                    timezone.utc
                ) < dag.start_date.astimezone(timezone.utc):
                    candidate = (
                        dag.start_date
                        if isinstance(schedule, timedelta)
                        else cron_next_at_or_after(
                            _expression(schedule), dag.start_date
                        )
                    )
            elif dag.start_date:
                candidate = (
                    dag.start_date
                    if isinstance(schedule, timedelta)
                    else cron_next_at_or_after(_expression(schedule), dag.start_date)
                )
            else:
                candidate = next_logical_date(
                    schedule, now.astimezone(dag_timezone(dag))
                )
            if dag.end_date and candidate.astimezone(
                timezone.utc
            ) > dag.end_date.astimezone(timezone.utc):
                return {"status": "completed", "at": None}
            return {
                "status": "due"
                if candidate.astimezone(timezone.utc) <= now
                else "planned",
                "at": candidate.isoformat(),
            }
        except (ValueError, OverflowError):
            return {"status": "unknown", "at": None}

    def runs(self, dag_id: str, limit: int = 100) -> list[dict]:
        return self.rows(
            "SELECT run_id, run_type, logical_date, state, queued_at, start_date, end_date "
            "FROM dag_run WHERE dag_id = ? ORDER BY queued_at DESC, rowid DESC LIMIT ?",
            (dag_id, limit),
        )

    def dag_info(self, source: DagSource) -> dict:
        dag = source.dag
        runs = self.runs(source.dag_id, 1)
        active = self.rows(
            "SELECT state, COUNT(*) AS count FROM dag_run "
            "WHERE dag_id = ? AND state IN ('running', 'queued') GROUP BY state",
            (source.dag_id,),
        )
        counts = {row["state"]: row["count"] for row in active}
        paused = self.store.is_dag_paused(source.dag_id)
        return {
            "dag_id": source.dag_id,
            "schedule": str(dag.schedule) if dag.schedule is not None else None,
            "paused": paused,
            "next_schedule": self.next_schedule(dag, paused),
            "running_count": counts.get("running", 0),
            "queued_count": counts.get("queued", 0),
            "task_count": len(dag.tasks),
            "latest_run": runs[0] if runs else None,
        }

    def detail(self, dag_id: str) -> dict:
        source = self.source(dag_id)
        result = self.dag_info(source)
        result["tasks"] = [
            {
                "task_id": task.task_id,
                "upstream": sorted(task.upstream_task_ids),
                "downstream": sorted(task.downstream_task_ids),
                "operator_type": getattr(
                    task, "_easyflow_operator_type", type(task).__name__
                ),
                "trigger_rule": task.trigger_rule.value,
                "retries": task.retries,
                "retry_delay": str(task.retry_delay) if task.retry_delay else None,
                "depends_on_past": task.depends_on_past,
            }
            for task in source.dag.topological_sort()
        ]
        result["description"] = source.dag.description
        result["start_date"] = (
            source.dag.start_date.isoformat() if source.dag.start_date else None
        )
        result["end_date"] = (
            source.dag.end_date.isoformat() if source.dag.end_date else None
        )
        result["timezone"] = str(dag_timezone(source.dag))
        result["catchup"] = bool(source.dag.catchup)
        result["max_active_runs"] = source.dag.max_active_runs or 1
        result["max_active_tasks"] = source.dag.max_active_tasks
        result["runs"] = self.runs(dag_id)
        result["scheduler_running"] = self.scheduler_running()
        return result

    def run_detail(self, dag_id: str, run_id: str) -> dict:
        rows = self.rows(
            "SELECT run_id, run_type, state, logical_date, data_interval_start, "
            "data_interval_end, queued_at, start_date, end_date, definition_hash "
            "FROM dag_run WHERE dag_id = ? AND run_id = ?",
            (dag_id, run_id),
        )
        if not rows:
            raise LookupError("运行不存在。")
        result = rows[0]
        details = self.rows(
            "SELECT task_id, state, try_number, queued_at, started_at, ended_at, "
            "next_retry_at, last_heartbeat_at, lease_expires_at "
            "FROM task_instance WHERE dag_id = ? AND run_id = ? AND map_index = -1",
            (dag_id, run_id),
        )
        result["tasks"] = {row["task_id"]: row["state"] for row in details}
        result["task_details"] = {row["task_id"]: row for row in details}
        result["definition_matches"] = None
        if result["definition_hash"]:
            try:
                source = self.source(dag_id)
                current_bytes = source.file_path.read_bytes()
                if hashlib.sha256(current_bytes).hexdigest() == getattr(
                    source.dag, "_easyflow_source_sha256", None
                ):
                    result["definition_matches"] = result[
                        "definition_hash"
                    ] == definition_hash_for_dag(source.dag, source.file_path)
            except (LookupError, OSError, ValueError):
                pass
        return result

    def source_code(self, dag_id: str, run_id: str | None) -> dict:
        source = self.source(dag_id)
        path = source.file_path.resolve()
        if not path.is_relative_to(self.dags_folder) or not path.is_file():
            raise ValueError("DAG 源码不在指定的 DAG 目录中或已不存在。")
        with path.open("rb") as handle:
            raw = handle.read(1_000_001)
        if len(raw) > 1_000_000:
            raise ValueError("DAG 源码超过 1 MB，无法在网页显示。")
        parsed_hash = getattr(source.dag, "_easyflow_source_sha256", None)
        parsed_matches = (
            parsed_hash == hashlib.sha256(raw).hexdigest() if parsed_hash else None
        )
        run_matches = None
        if run_id:
            record = self.store.get_dag_run(dag_id, run_id)
            if record is None:
                raise LookupError("运行不存在。")
            if record.definition_hash and parsed_matches:
                run_matches = record.definition_hash == definition_hash_for_dag(
                    source.dag, path
                )
        return {
            "path": str(path.relative_to(self.dags_folder)),
            "content": raw.decode("utf-8", errors="replace"),
            "parsed_matches": parsed_matches,
            "run_matches": run_matches,
        }

    def trigger(self, dag_id: str, conf: dict) -> str:
        source = self.source(dag_id)
        runner = PersistentDagRunner(self.store, log_root=self.log_root)
        return runner.create_manual_run(
            source.dag,
            file_path=source.file_path,
            logical_date=datetime.now(timezone.utc),
            conf=conf,
        )

    def task_log(
        self, dag_id: str, run_id: str, task_id: str, attempt: int | None
    ) -> dict:
        rows = self.rows(
            "SELECT try_number, state, log_path FROM task_try "
            "WHERE dag_id = ? AND run_id = ? AND task_id = ? ORDER BY try_number DESC",
            (dag_id, run_id, task_id),
        )
        selected = next(
            (row for row in rows if attempt is None or row["try_number"] == attempt),
            None,
        )
        content = None
        truncated = False
        if selected and selected["log_path"]:
            path = Path(selected["log_path"]).resolve()
            if not path.is_relative_to(self.log_root):
                raise ValueError("日志不在配置的日志目录中，请检查 --log-root。")
            if path.is_file():
                with path.open("rb") as handle:
                    handle.seek(0, 2)
                    length = handle.tell()
                    truncated = length > 1_000_000
                    handle.seek(max(0, length - 1_000_000))
                    content = handle.read().decode("utf-8", errors="replace")
        return {
            "attempts": [
                {"try_number": row["try_number"], "state": row["state"]} for row in rows
            ],
            "selected": selected["try_number"] if selected else None,
            "content": content,
            "truncated": truncated,
        }
