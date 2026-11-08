from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from easyflow._internal.dag_parser import (
    AsyncDagDiscovery,
    DagDiscoveryResult,
    DagSource,
)
from easyflow._internal.persistent_runner import PersistentDagRunner
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
        rows = self.rows(
            "SELECT lease_expires_at FROM scheduler_lease WHERE lease_id = 'singleton'"
        )
        return bool(
            rows
            and datetime.fromisoformat(rows[0]["lease_expires_at"])
            > datetime.now(timezone.utc)
        )

    def runs(self, dag_id: str, limit: int = 100) -> list[dict]:
        return self.rows(
            "SELECT run_id, run_type, logical_date, state, queued_at, start_date, end_date "
            "FROM dag_run WHERE dag_id = ? ORDER BY queued_at DESC, rowid DESC LIMIT ?",
            (dag_id, limit),
        )

    def dag_info(self, source: DagSource) -> dict:
        dag = source.dag
        runs = self.runs(source.dag_id, 1)
        return {
            "dag_id": source.dag_id,
            "schedule": str(dag.schedule) if dag.schedule is not None else None,
            "paused": self.store.is_dag_paused(source.dag_id),
            "task_count": len(dag.tasks),
            "latest_run": runs[0] if runs else None,
        }

    def detail(self, dag_id: str) -> dict:
        source = self.source(dag_id)
        result = self.dag_info(source)
        result["tasks"] = [
            {"task_id": task.task_id, "upstream": sorted(task.upstream_task_ids)}
            for task in source.dag.topological_sort()
        ]
        result["runs"] = self.runs(dag_id)
        result["scheduler_running"] = self.scheduler_running()
        return result

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
