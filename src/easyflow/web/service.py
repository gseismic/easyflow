from __future__ import annotations

import base64
import binascii
import hashlib
import json
import sqlite3
import threading
import time
from collections import OrderedDict
from concurrent.futures import Future
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
from easyflow.web.catalog import (
    matches_project,
    search_terms,
    validate_project_filter,
    workflow_metadata,
)

_NOT_LOADED = object()
_FACET_CACHE_SECONDS = 5
_FACET_CACHE_SIZE = 16


def _history_conditions(
    *,
    dag: str | None,
    dag_id: str | None,
    state: str | None,
    query: str | None,
    tags: list[str],
) -> tuple[list[str], list]:
    conditions = []
    params: list = []
    if dag:
        conditions.append("instr(casefold(r.dag_id), ?) > 0")
        params.append(dag.casefold())
    if dag_id:
        conditions.append("r.dag_id = ?")
        params.append(dag_id)
    if state:
        conditions.append("r.state = ?")
        params.append(state)
    if tags:
        conditions.append(
            "EXISTS (SELECT 1 FROM workflow_tags t "
            "JOIN selected_tags s ON s.tag = t.tag WHERE t.dag_id = r.dag_id)"
        )
    for term in search_terms(query):
        conditions.append(
            "instr(casefold(r.dag_id || ' ' || r.run_id) || ' ' || "
            "COALESCE(m.search_text, ''), ?) > 0"
        )
        params.append(term)
    return conditions, params


def _prepare_history_metadata(
    connection: sqlite3.Connection, metadata: list, tags: list[str]
) -> None:
    connection.create_function("casefold", 1, str.casefold, deterministic=True)
    connection.execute(
        "CREATE TEMP TABLE workflow_metadata (dag_id TEXT PRIMARY KEY, "
        "project_kind TEXT, project TEXT, tags TEXT, search_text TEXT, issue TEXT)"
    )
    connection.execute(
        "CREATE TEMP TABLE workflow_tags (dag_id TEXT, tag TEXT, PRIMARY KEY (dag_id, tag))"
    )
    connection.execute("CREATE TEMP TABLE selected_tags (tag TEXT PRIMARY KEY)")
    connection.executemany(
        "INSERT INTO selected_tags VALUES (?)", ((tag,) for tag in set(tags))
    )
    connection.executemany(
        "INSERT INTO workflow_metadata VALUES (?, ?, ?, ?, ?, ?)",
        (
            (
                item.dag_id,
                item.project_kind,
                item.project,
                json.dumps(item.tags, ensure_ascii=False),
                item.search_text,
                item.issue,
            )
            for item in metadata
        ),
    )
    connection.executemany(
        "INSERT INTO workflow_tags VALUES (?, ?)",
        ((item.dag_id, tag) for item in metadata for tag in item.tags),
    )


def _empty_facet_groups(metadata: list) -> dict[tuple[str, str | None], dict]:
    keys = {(item.project_kind, item.project) for item in metadata}
    keys.update({("unassigned", None), ("conflict", None), ("unknown", None)})
    return {
        key: {"project_kind": key[0], "project": key[1], "total": 0, "matched": 0}
        for key in keys
    }


def _facet_metadata_key(metadata: list, complete: bool) -> str:
    payload = [
        item.as_dict() for item in sorted(metadata, key=lambda item: item.dag_id)
    ]
    return hashlib.sha256(
        json.dumps([complete, payload], ensure_ascii=False, sort_keys=True).encode(
            "utf-8"
        )
    ).hexdigest()


def _history_cursor(row: dict) -> str:
    value = [row["queued_at"], row["dag_id"], row["run_id"]]
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _parse_history_cursor(cursor: str) -> tuple[str, str, str]:
    try:
        raw = base64.b64decode(
            cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True
        )
        value = json.loads(raw)
    except (ValueError, binascii.Error, UnicodeDecodeError, RecursionError) as exc:
        raise ValueError("运行历史的分页位置无效，请返回第一页。") from exc
    if (
        not isinstance(value, list)
        or len(value) != 3
        or not all(isinstance(item, str) and item for item in value)
    ):
        raise ValueError("运行历史的分页位置无效，请返回第一页。")
    return tuple(value)


class WebService:
    def __init__(self, dags_folder: str, database_url: str, log_root: str) -> None:
        self.dags_folder = Path(dags_folder).resolve()
        self.log_root = Path(log_root).resolve()
        self.store = SQLiteStateStore(database_url)
        self.store.init_db()
        self._lock = threading.Lock()
        self._facet_lock = threading.Lock()
        self._facet_cache: OrderedDict[tuple, tuple[float, dict]] = OrderedDict()
        self._facet_inflight: dict[tuple, Future] = {}
        self.discovery = AsyncDagDiscovery(self.dags_folder)

    def _cached_facets(self, key: tuple, calculate) -> dict:
        """短时复用同条件统计，并合并并发请求；返回计算时刻供界面标注。"""
        with self._facet_lock:
            entry = self._facet_cache.get(key)
            if entry and time.monotonic() - entry[0] < _FACET_CACHE_SECONDS:
                self._facet_cache.move_to_end(key)
                return entry[1]
            future = self._facet_inflight.get(key)
            leader = future is None
            if leader:
                future = Future()
                self._facet_inflight[key] = future
        if not leader:
            return future.result()
        try:
            result = calculate()
            with self._facet_lock:
                self._facet_cache[key] = (time.monotonic(), result)
                self._facet_cache.move_to_end(key)
                while len(self._facet_cache) > _FACET_CACHE_SIZE:
                    self._facet_cache.popitem(last=False)
            future.set_result(result)
            return result
        except BaseException as exc:
            future.set_exception(exc)
            raise
        finally:
            with self._facet_lock:
                self._facet_inflight.pop(key, None)

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

    def next_schedule(
        self,
        dag: object,
        paused: bool,
        *,
        last_automated: datetime | None | object = _NOT_LOADED,
    ) -> dict:
        schedule = dag.schedule
        if schedule is None:
            return {"status": "manual", "at": None}
        if paused:
            return {"status": "paused", "at": None}
        now = datetime.now(timezone.utc)
        last = (
            self.store.latest_automated_logical_date(dag.dag_id)
            if last_automated is _NOT_LOADED
            else last_automated
        )
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

    def catalog(self) -> dict:
        discovered = self.discover()
        metadata = [workflow_metadata(source) for source in discovered.sources]
        projects = {item.project for item in metadata if item.project is not None}
        tags = {tag for item in metadata for tag in item.tags}
        scheduler = self.scheduler_status()
        return {
            "projects": sorted(projects, key=lambda value: (value.casefold(), value)),
            "tags": sorted(tags, key=lambda value: (value.casefold(), value)),
            "issues": [
                f"{item.file_path.name}: {item.message}" for item in discovered.issues
            ],
            "loading": discovered.pending,
            "scheduler": scheduler,
            "scheduler_running": scheduler["running"],
        }

    def workflow_states(self, dag_ids: list[str]) -> dict[str, dict]:
        if not dag_ids:
            return {}
        with sqlite3.connect(self.store.path) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute(
                "CREATE TEMP TABLE workflow_ids (dag_id TEXT PRIMARY KEY)"
            )
            connection.executemany(
                "INSERT INTO workflow_ids VALUES (?)", ((dag_id,) for dag_id in dag_ids)
            )
            rows = connection.execute(
                "SELECT i.dag_id, COALESCE(c.is_paused, 0) AS paused, "
                "(SELECT state FROM dag_run r WHERE r.dag_id = i.dag_id "
                "ORDER BY queued_at DESC, rowid DESC LIMIT 1) AS latest_state, "
                "(SELECT COUNT(*) FROM dag_run r WHERE r.dag_id = i.dag_id "
                "AND r.state = 'running') AS running_count "
                "FROM workflow_ids i LEFT JOIN dag_control c ON c.dag_id = i.dag_id"
            ).fetchall()
        return {row["dag_id"]: dict(row) for row in rows}

    def workflow_page_details(self, dag_ids: list[str]) -> dict[str, dict]:
        """只读取当前页的运行和调度信息，不为每一行重复初始化状态库。"""
        if not dag_ids:
            return {}
        with sqlite3.connect(self.store.path) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("CREATE TEMP TABLE page_ids (dag_id TEXT PRIMARY KEY)")
            connection.executemany(
                "INSERT INTO page_ids VALUES (?)", ((dag_id,) for dag_id in dag_ids)
            )
            rows = connection.execute(
                "SELECT i.dag_id, r.run_id, r.run_type, r.logical_date, r.state, "
                "r.queued_at, r.start_date, r.end_date, "
                "(SELECT COUNT(*) FROM dag_run q WHERE q.dag_id = i.dag_id "
                "AND q.state = 'queued') AS queued_count, "
                "(SELECT MAX(a.logical_date) FROM dag_run a WHERE a.dag_id = i.dag_id "
                "AND a.run_type IN ('scheduled', 'backfill')) AS last_automated "
                "FROM page_ids i LEFT JOIN dag_run r ON r.rowid = "
                "(SELECT latest.rowid FROM dag_run latest WHERE latest.dag_id = i.dag_id "
                "ORDER BY latest.queued_at DESC, latest.rowid DESC LIMIT 1)"
            ).fetchall()
        result = {}
        for row in rows:
            latest = (
                {
                    key: row[key]
                    for key in (
                        "run_id",
                        "run_type",
                        "logical_date",
                        "state",
                        "queued_at",
                        "start_date",
                        "end_date",
                    )
                }
                if row["run_id"] is not None
                else None
            )
            result[row["dag_id"]] = {
                "latest_run": latest,
                "queued_count": row["queued_count"],
                "last_automated": (
                    datetime.fromisoformat(row["last_automated"])
                    if row["last_automated"] is not None
                    else None
                ),
            }
        return result

    def workflow_facets(
        self, *, query: str | None, state: str, tags: list[str]
    ) -> dict:
        discovered = self.discover()
        metadata = [workflow_metadata(source) for source in discovered.sources]
        pending = discovered.pending
        complete = not discovered.pending and not discovered.issues
        terms = search_terms(query)
        selected_tags = set(tags)
        key = (
            "workflows",
            _facet_metadata_key(metadata, complete),
            pending,
            tuple(terms),
            state,
            tuple(sorted(selected_tags)),
        )

        def calculate() -> dict:
            groups = _empty_facet_groups(metadata)
            calculated_at = datetime.now(timezone.utc).isoformat()
            states = self.workflow_states([item.dag_id for item in metadata])
            for item in metadata:
                group = groups[(item.project_kind, item.project)]
                group["total"] += 1
                status = states[item.dag_id]
                if selected_tags and not selected_tags.intersection(item.tags):
                    continue
                search_text = item.search_text
                if not all(term in search_text for term in terms):
                    continue
                if state == "running" and status["running_count"] <= 0:
                    continue
                if state == "failed" and status["latest_state"] != "failed":
                    continue
                if state == "paused" and not status["paused"]:
                    continue
                group["matched"] += 1
            return {
                "groups": list(groups.values()),
                "complete": complete,
                "pending": pending,
                "calculated_at": calculated_at,
            }

        return self._cached_facets(key, calculate)

    def history_facets(
        self,
        *,
        dag: str | None,
        dag_id: str | None,
        state: str | None,
        query: str | None,
        tags: list[str],
    ) -> dict:
        try:
            discovered = self.discover()
            metadata = [workflow_metadata(source) for source in discovered.sources]
            pending = discovered.pending
            complete = not discovered.pending and not discovered.issues
        except ValueError:
            metadata = []
            pending = False
            complete = False
        terms = search_terms(query)
        selected_tags = sorted(set(tags))
        key = (
            "history",
            _facet_metadata_key(metadata, complete),
            pending,
            dag,
            dag_id,
            state,
            tuple(terms),
            tuple(selected_tags),
        )

        def calculate() -> dict:
            groups = _empty_facet_groups(metadata)
            conditions, params = _history_conditions(
                dag=dag, dag_id=dag_id, state=state, query=query, tags=selected_tags
            )
            predicate = " AND ".join(conditions) if conditions else "1"
            calculated_at = datetime.now(timezone.utc).isoformat()
            with sqlite3.connect(self.store.path) as connection:
                connection.row_factory = sqlite3.Row
                _prepare_history_metadata(connection, metadata, selected_tags)
                rows = connection.execute(
                    "SELECT COALESCE(m.project_kind, 'unknown') AS project_kind, "
                    "m.project, COUNT(*) AS total, "
                    f"SUM(CASE WHEN {predicate} THEN 1 ELSE 0 END) AS matched "
                    "FROM dag_run r LEFT JOIN workflow_metadata m ON m.dag_id = r.dag_id "
                    "GROUP BY COALESCE(m.project_kind, 'unknown'), m.project",
                    tuple(params),
                ).fetchall()
            for row in rows:
                group_key = (row["project_kind"], row["project"])
                groups[group_key] = {
                    "project_kind": row["project_kind"],
                    "project": row["project"],
                    "total": row["total"],
                    "matched": row["matched"],
                }
            return {
                "groups": list(groups.values()),
                "complete": complete,
                "pending": pending,
                "calculated_at": calculated_at,
            }

        return self._cached_facets(key, calculate)

    def workflows(
        self,
        *,
        limit: int,
        cursor: str | None,
        query: str | None,
        project_kind: str,
        project: str | None,
        tags: list[str],
        state: str,
    ) -> dict:
        validate_project_filter(project_kind, project)
        discovered = self.discover()
        terms = search_terms(query)
        selected_tags = set(tags)
        candidates = []
        for source in discovered.sources:
            metadata = workflow_metadata(source)
            if not matches_project(metadata, project_kind, project):
                continue
            if selected_tags and not selected_tags.intersection(metadata.tags):
                continue
            search_text = metadata.search_text
            if not all(term in search_text for term in terms):
                continue
            candidates.append((source, metadata))
        states = self.workflow_states([source.dag_id for source, _ in candidates])
        summary = {
            "total": len(candidates),
            "running": sum(
                states[source.dag_id]["running_count"] > 0 for source, _ in candidates
            ),
            "failed": sum(
                states[source.dag_id]["latest_state"] == "failed"
                for source, _ in candidates
            ),
        }

        def matches_state(source: DagSource) -> bool:
            status = states[source.dag_id]
            if state == "running":
                return status["running_count"] > 0
            if state == "failed":
                return status["latest_state"] == "failed"
            if state == "paused":
                return bool(status["paused"])
            return True

        eligible = [
            (source, metadata)
            for source, metadata in candidates
            if matches_state(source) and (cursor is None or source.dag_id > cursor)
        ]
        eligible.sort(key=lambda item: item[0].dag_id)
        page = eligible[:limit]
        details = self.workflow_page_details([source.dag_id for source, _ in page])
        return {
            "dags": [
                {
                    **self.dag_info(
                        source,
                        snapshot={**states[source.dag_id], **details[source.dag_id]},
                    ),
                    **metadata.as_dict(),
                }
                for source, metadata in page
            ],
            "summary": summary,
            "next_cursor": page[-1][0].dag_id if len(eligible) > limit else None,
            "loading": discovered.pending,
        }

    def history(
        self,
        *,
        limit: int,
        cursor: str | None,
        dag: str | None,
        dag_id: str | None = None,
        state: str | None,
        query: str | None = None,
        project_kind: str = "all",
        project: str | None = None,
        tags: list[str] | None = None,
    ) -> dict:
        validate_project_filter(project_kind, project)
        selected_tags = sorted(set(tags or []))
        conditions, params = _history_conditions(
            dag=dag, dag_id=dag_id, state=state, query=query, tags=selected_tags
        )
        if cursor:
            conditions.append("(r.queued_at, r.dag_id, r.run_id) < (?, ?, ?)")
            params.extend(_parse_history_cursor(cursor))
        if project_kind == "unknown":
            conditions.append("m.dag_id IS NULL")
        elif project_kind == "project":
            conditions.append("m.project_kind = 'project' AND m.project = ?")
            params.append(project)
        elif project_kind != "all":
            conditions.append("m.project_kind = ?")
            params.append(project_kind)
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(limit + 1)
        try:
            discovered = self.discover()
            metadata = [workflow_metadata(source) for source in discovered.sources]
            loading = discovered.pending
            metadata_error = None
        except ValueError as exc:
            metadata = []
            loading = False
            metadata_error = str(exc)
        with sqlite3.connect(self.store.path) as connection:
            connection.row_factory = sqlite3.Row
            _prepare_history_metadata(connection, metadata, selected_tags)
            rows = [
                dict(row)
                for row in connection.execute(
                    "SELECT r.dag_id, r.run_id, r.run_type, r.state, r.logical_date, "
                    "r.queued_at, r.start_date, r.end_date, "
                    "COALESCE(m.project_kind, 'unknown') AS project_kind, "
                    "m.project, m.tags, m.issue AS metadata_issue FROM dag_run r "
                    "LEFT JOIN workflow_metadata m ON m.dag_id = r.dag_id"
                    f"{where} ORDER BY r.queued_at DESC, r.dag_id DESC, r.run_id DESC LIMIT ?",
                    tuple(params),
                ).fetchall()
            ]
        for row in rows:
            row["tags"] = json.loads(row["tags"]) if row["tags"] else []
        return {
            "runs": rows[:limit],
            "next_cursor": _history_cursor(rows[limit - 1])
            if len(rows) > limit
            else None,
            "metadata_loading": loading,
            "metadata_error": metadata_error,
        }

    def dag_info(self, source: DagSource, *, snapshot: dict | None = None) -> dict:
        dag = source.dag
        if snapshot is None:
            states = self.workflow_states([source.dag_id])[source.dag_id]
            details = self.workflow_page_details([source.dag_id])[source.dag_id]
            snapshot = {**states, **details}
        paused = bool(snapshot["paused"])
        return {
            **workflow_metadata(source).as_dict(),
            "dag_id": source.dag_id,
            "schedule": str(dag.schedule) if dag.schedule is not None else None,
            "paused": paused,
            "next_schedule": self.next_schedule(
                dag, paused, last_automated=snapshot["last_automated"]
            ),
            "running_count": snapshot["running_count"],
            "queued_count": snapshot["queued_count"],
            "task_count": len(dag.tasks),
            "latest_run": snapshot["latest_run"],
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
