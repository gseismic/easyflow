from __future__ import annotations

import json
import os
import sqlite3
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from easyflow.sdk.exceptions import (
    StateConflictError,
    XComSerializationError,
    XComValueTooLargeError,
)
from easyflow.sdk.states import DagRunState, TaskInstanceState

MAX_XCOM_BYTES = 1_048_576


class DagRunCapacityError(StateConflictError):
    pass


class ScheduledRunExists(StateConflictError):
    pass


class SchedulerLeaseLost(RuntimeError):
    pass


class BackfillRunNotEligible(StateConflictError):
    pass


@dataclass(frozen=True)
class SchedulerFence:
    owner: str
    started_at: datetime
    started_monotonic: float


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS dag_definition (
    dag_id TEXT PRIMARY KEY,
    definition_hash TEXT NOT NULL,
    file_path TEXT,
    parsed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dag_run (
    dag_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    run_type TEXT NOT NULL,
    logical_date TEXT NOT NULL,
    data_interval_start TEXT NOT NULL,
    data_interval_end TEXT NOT NULL,
    state TEXT NOT NULL,
    definition_hash TEXT,
    backfill_id TEXT,
    queued_at TEXT NOT NULL,
    start_date TEXT,
    end_date TEXT,
    conf_json TEXT,
    PRIMARY KEY (dag_id, run_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_scheduled_dag_run
ON dag_run(dag_id, logical_date)
WHERE run_type = 'scheduled';

CREATE UNIQUE INDEX IF NOT EXISTS uq_backfill_dag_run
ON dag_run(dag_id, backfill_id, logical_date)
WHERE run_type = 'backfill';

CREATE TABLE IF NOT EXISTS task_instance (
    dag_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    map_index INTEGER NOT NULL DEFAULT -1,
    state TEXT,
    try_number INTEGER NOT NULL DEFAULT 0,
    queued_at TEXT,
    started_at TEXT,
    ended_at TEXT,
    next_retry_at TEXT,
    executor_id TEXT,
    worker_pid INTEGER,
    lease_expires_at TEXT,
    last_heartbeat_at TEXT,
    PRIMARY KEY (dag_id, run_id, task_id, map_index),
    FOREIGN KEY (dag_id, run_id) REFERENCES dag_run(dag_id, run_id)
);

CREATE TABLE IF NOT EXISTS task_try (
    dag_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    map_index INTEGER NOT NULL DEFAULT -1,
    try_number INTEGER NOT NULL,
    state TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    log_path TEXT,
    error_code TEXT,
    error_message TEXT,
    traceback TEXT,
    executor_info_json TEXT,
    PRIMARY KEY (dag_id, run_id, task_id, map_index, try_number),
    FOREIGN KEY (dag_id, run_id, task_id, map_index)
        REFERENCES task_instance(dag_id, run_id, task_id, map_index)
);

CREATE TABLE IF NOT EXISTS xcom (
    dag_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    map_index INTEGER NOT NULL DEFAULT -1,
    key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (dag_id, run_id, task_id, map_index, key),
    FOREIGN KEY (dag_id, run_id, task_id, map_index)
        REFERENCES task_instance(dag_id, run_id, task_id, map_index)
);

CREATE TABLE IF NOT EXISTS scheduler_lease (
    lease_id TEXT PRIMARY KEY CHECK (lease_id = 'singleton'),
    owner TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_heartbeat (
    dag_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    map_index INTEGER NOT NULL DEFAULT -1,
    try_number INTEGER NOT NULL,
    heartbeat_at TEXT NOT NULL,
    worker_pid INTEGER,
    PRIMARY KEY (dag_id, run_id, task_id, map_index, try_number)
);

CREATE TABLE IF NOT EXISTS backfill (
    backfill_id TEXT PRIMARY KEY,
    dag_id TEXT NOT NULL,
    start_logical_date TEXT NOT NULL,
    end_logical_date TEXT NOT NULL,
    reprocessing_behavior TEXT NOT NULL,
    max_active_runs INTEGER,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    dag_id TEXT,
    run_id TEXT,
    task_id TEXT,
    payload_json TEXT,
    created_at TEXT NOT NULL
);
"""


def path_from_database_url(database_url: str) -> Path:
    if database_url.startswith("sqlite:///"):
        return Path(database_url.removeprefix("sqlite:///"))
    if database_url.startswith("sqlite://"):
        raise ValueError("sqlite URL 必须指向本地文件。")
    if "://" in database_url:
        raise ValueError("v0.1 只支持 sqlite 数据库 URL。")
    return Path(database_url)


class SQLiteStateStore:
    def __init__(self, database_url: str = "sqlite:///./.easyflow/easyflow.db") -> None:
        self.database_url = database_url
        self.path = path_from_database_url(database_url)

    def init_db(self) -> Path:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as connection:
            connection.executescript(SCHEMA)
            connection.execute("BEGIN IMMEDIATE")
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(dag_run)")
            }
            if "logical_timezone" not in columns:
                connection.execute(
                    "ALTER TABLE dag_run ADD COLUMN logical_timezone TEXT"
                )
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(dag_definition)")
            }
            if "max_active_runs" not in columns:
                connection.execute(
                    "ALTER TABLE dag_definition ADD COLUMN max_active_runs INTEGER NOT NULL DEFAULT 1"
                )
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(task_instance)")
            }
            if "retry_base" not in columns:
                connection.execute(
                    "ALTER TABLE task_instance ADD COLUMN retry_base INTEGER NOT NULL DEFAULT 0"
                )
                _restore_retry_baselines(connection)
        return self.path

    def list_tables(self) -> list[str]:
        with sqlite3.connect(self.path) as connection:
            rows = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        return [row[0] for row in rows]

    def create_dag_run(
        self,
        *,
        dag: Any,
        run_id: str,
        run_type: str,
        logical_date: datetime,
        data_interval_start: datetime,
        data_interval_end: datetime,
        definition_hash: str,
        file_path: str | None,
        conf: dict[str, Any] | None = None,
        backfill_id: str | None = None,
        max_active_runs: int | None = None,
        only_once: bool = False,
        scheduler_fence: SchedulerFence | None = None,
        reprocessing_behavior: str | None = None,
    ) -> None:
        now = _utc_now()
        self.init_db()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            _assert_scheduler_lease(connection, scheduler_fence)
            if run_type == "scheduled":
                query = (
                    "SELECT 1 FROM dag_run WHERE dag_id = ? AND run_type = 'scheduled'"
                )
                params: list[Any] = [dag.dag_id]
                if not only_once:
                    query += " AND logical_date = ?"
                    params.append(_iso(logical_date))
                if connection.execute(query + " LIMIT 1", params).fetchone():
                    raise ScheduledRunExists(
                        f"scheduled DagRun 已存在: {dag.dag_id}/{logical_date}"
                    )
            if run_type == "backfill" and reprocessing_behavior is not None:
                latest = connection.execute(
                    "SELECT state FROM dag_run WHERE dag_id = ? AND logical_date = ? ORDER BY queued_at DESC, rowid DESC LIMIT 1",
                    (dag.dag_id, _iso(logical_date)),
                ).fetchone()
                eligible = (
                    latest is None
                    or (reprocessing_behavior == "failed" and latest[0] == "failed")
                    or (
                        reprocessing_behavior == "completed"
                        and latest[0] in ("success", "failed")
                    )
                )
                if not eligible:
                    raise BackfillRunNotEligible(
                        f"backfill 区间已有运行: {dag.dag_id}/{logical_date}"
                    )
            limit = dag.max_active_runs or 1
            active = connection.execute(
                "SELECT COUNT(*) FROM dag_run WHERE dag_id = ? AND state IN ('queued', 'running')",
                (dag.dag_id,),
            ).fetchone()[0]
            if active >= limit:
                raise DagRunCapacityError(
                    f"DAG {dag.dag_id!r} 已达到 max_active_runs={limit}。"
                )
            if run_type == "backfill" and max_active_runs is not None:
                backfill_active = connection.execute(
                    "SELECT COUNT(*) FROM dag_run WHERE dag_id = ? AND backfill_id = ? "
                    "AND state IN ('queued', 'running')",
                    (dag.dag_id, backfill_id),
                ).fetchone()[0]
                if backfill_active >= max_active_runs:
                    raise DagRunCapacityError(
                        f"backfill {backfill_id!r} 已达到 max_active_runs={max_active_runs}。"
                    )
            connection.execute(
                """
                INSERT INTO dag_definition(dag_id, definition_hash, file_path, parsed_at, max_active_runs)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(dag_id) DO UPDATE SET
                    definition_hash = excluded.definition_hash,
                    file_path = excluded.file_path,
                    parsed_at = excluded.parsed_at,
                    max_active_runs = excluded.max_active_runs
                """,
                (dag.dag_id, definition_hash, file_path, now, dag.max_active_runs or 1),
            )
            connection.execute(
                """
                INSERT INTO dag_run(
                    dag_id, run_id, run_type, logical_date, data_interval_start,
                    data_interval_end, state, definition_hash, backfill_id, queued_at,
                    conf_json, logical_timezone
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    dag.dag_id,
                    run_id,
                    run_type,
                    _iso(logical_date),
                    _iso(data_interval_start),
                    _iso(data_interval_end),
                    DagRunState.QUEUED.value,
                    definition_hash,
                    backfill_id,
                    now,
                    json.dumps(conf or {}, ensure_ascii=False),
                    _timezone_id(logical_date),
                ),
            )
            for task in dag.tasks:
                connection.execute(
                    """
                    INSERT INTO task_instance(dag_id, run_id, task_id, map_index, state)
                    VALUES (?, ?, ?, -1, NULL)
                    """,
                    (dag.dag_id, run_id, task.task_id),
                )

    def create_backfill(
        self,
        *,
        backfill_id: str,
        dag_id: str,
        start_logical_date: datetime,
        end_logical_date: datetime,
        reprocessing_behavior: str,
        max_active_runs: int | None,
    ) -> None:
        now = _utc_now()
        self.init_db()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO backfill(
                    backfill_id, dag_id, start_logical_date, end_logical_date,
                    reprocessing_behavior, max_active_runs, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    backfill_id,
                    dag_id,
                    _iso(start_logical_date),
                    _iso(end_logical_date),
                    reprocessing_behavior,
                    max_active_runs,
                    now,
                ),
            )

    def dag_run_exists_for_logical_date(
        self,
        *,
        dag_id: str,
        logical_date: datetime,
        run_type: str | None = None,
    ) -> bool:
        self.init_db()
        query = "SELECT 1 FROM dag_run WHERE dag_id = ? AND logical_date = ?"
        params: list[Any] = [dag_id, _iso(logical_date)]
        if run_type is not None:
            query += " AND run_type = ?"
            params.append(run_type)
        query += " LIMIT 1"
        with self._connect() as connection:
            row = connection.execute(query, params).fetchone()
        return row is not None

    def latest_dag_run_state_for_logical_date(
        self,
        *,
        dag_id: str,
        logical_date: datetime,
    ) -> DagRunState | None:
        self.init_db()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT state
                FROM dag_run
                WHERE dag_id = ? AND logical_date = ?
                ORDER BY queued_at DESC, rowid DESC
                LIMIT 1
                """,
                (dag_id, _iso(logical_date)),
            ).fetchone()
        return None if row is None else DagRunState(row[0])

    def previous_scheduled_or_backfill_task_state(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        logical_date: datetime,
    ) -> "PreviousTaskRecord":
        self.init_db()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT dr.run_id, ti.state
                FROM dag_run dr
                JOIN task_instance ti
                  ON ti.dag_id = dr.dag_id AND ti.run_id = dr.run_id
                WHERE dr.dag_id = ?
                  AND dr.run_id != ?
                  AND dr.run_type IN ('scheduled', 'backfill')
                  AND dr.logical_date < ?
                  AND ti.task_id = ?
                  AND ti.map_index = -1
                ORDER BY dr.logical_date DESC, dr.queued_at DESC
                LIMIT 1
                """,
                (dag_id, run_id, _iso(logical_date), task_id),
            ).fetchone()
        if row is None:
            return PreviousTaskRecord(exists=False, run_id=None, state=None)
        return PreviousTaskRecord(
            exists=True,
            run_id=row[0],
            state=TaskInstanceState(row[1]) if row[1] is not None else None,
        )

    def count_dag_runs(self, *, dag_id: str, run_type: str | None = None) -> int:
        self.init_db()
        query = "SELECT COUNT(*) FROM dag_run WHERE dag_id = ?"
        params: list[Any] = [dag_id]
        if run_type is not None:
            query += " AND run_type = ?"
            params.append(run_type)
        with self._connect() as connection:
            row = connection.execute(query, params).fetchone()
        return int(row[0])

    def count_active_dag_runs(self, *, dag_id: str) -> int:
        self.init_db()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*)
                FROM dag_run
                WHERE dag_id = ? AND state IN (?, ?)
                """,
                (dag_id, DagRunState.QUEUED.value, DagRunState.RUNNING.value),
            ).fetchone()
        return int(row[0])

    def list_active_dag_runs(self, dag_id: str) -> list[str]:
        self.init_db()
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT run_id FROM dag_run WHERE dag_id = ? AND state IN ('queued', 'running') "
                "ORDER BY logical_date, queued_at, run_id",
                (dag_id,),
            ).fetchall()
        return [row[0] for row in rows]

    def get_dag_run(self, dag_id: str, run_id: str) -> "DagRunRecord | None":
        self.init_db()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT dag_id, run_id, run_type, logical_date, data_interval_start,
                       data_interval_end, state, definition_hash, conf_json, logical_timezone
                FROM dag_run
                WHERE dag_id = ? AND run_id = ?
                """,
                (dag_id, run_id),
            ).fetchone()
        if row is None:
            return None
        return DagRunRecord(*row)

    def set_dag_run_state(self, dag_id: str, run_id: str, state: DagRunState) -> None:
        self.init_db()
        now = _utc_now()
        start_date = now if state == DagRunState.RUNNING else None
        end_date = now if state in {DagRunState.SUCCESS, DagRunState.FAILED} else None
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE dag_run
                SET state = ?,
                    start_date = COALESCE(start_date, ?),
                    end_date = COALESCE(?, end_date)
                WHERE dag_id = ? AND run_id = ?
                """,
                (state.value, start_date, end_date, dag_id, run_id),
            )

    def finish_dag_run(self, dag: Any, run_id: str) -> DagRunState:
        self.init_db()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT state FROM dag_run WHERE dag_id = ? AND run_id = ?",
                (dag.dag_id, run_id),
            ).fetchone()
            if row is None:
                raise StateConflictError(f"DagRun 不存在: {dag.dag_id}/{run_id}")
            if row[0] in ("success", "failed"):
                return DagRunState(row[0])
            states = dict(
                connection.execute(
                    "SELECT task_id, state FROM task_instance WHERE dag_id = ? AND run_id = ? AND map_index = -1",
                    (dag.dag_id, run_id),
                ).fetchall()
            )
            if any(
                state not in ("success", "failed", "upstream_failed", "skipped")
                for state in states.values()
            ):
                return DagRunState(row[0])
            failed = any(
                states.get(task.task_id) in ("failed", "upstream_failed")
                for task in dag.tasks
                if not task.downstream_task_ids
            )
            result = DagRunState.FAILED if failed else DagRunState.SUCCESS
            connection.execute(
                "UPDATE dag_run SET state = ?, end_date = ? WHERE dag_id = ? AND run_id = ?",
                (result.value, _utc_now(), dag.dag_id, run_id),
            )
            return result

    def list_task_states(
        self, dag_id: str, run_id: str
    ) -> dict[str, TaskInstanceState | None]:
        self.init_db()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT task_id, state
                FROM task_instance
                WHERE dag_id = ? AND run_id = ?
                ORDER BY task_id
                """,
                (dag_id, run_id),
            ).fetchall()
        return {
            task_id: (TaskInstanceState(state) if state is not None else None)
            for task_id, state in rows
        }

    def get_task_state(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
    ) -> TaskInstanceState | None:
        self.init_db()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT state FROM task_instance
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1
                """,
                (dag_id, run_id, task_id),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return TaskInstanceState(row[0])

    def start_task_try(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        *,
        owner: str | None = None,
        lease_seconds: int = 30,
        now: datetime | None = None,
        scheduler_fence: SchedulerFence | None = None,
    ) -> int | None:
        self.init_db()
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            raise ValueError("now 必须带时区。")
        owner = owner or f"runner-{os.getpid()}"
        now_iso = _iso(current)
        lease_until = _iso(current + timedelta(seconds=lease_seconds))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if now is None:
                current = datetime.now(timezone.utc)
                now_iso = _iso(current)
                lease_until = _iso(current + timedelta(seconds=lease_seconds))
            _assert_scheduler_lease(connection, scheduler_fence)
            dag_run = connection.execute(
                "SELECT state FROM dag_run WHERE dag_id = ? AND run_id = ?",
                (dag_id, run_id),
            ).fetchone()
            if dag_run is None or dag_run[0] not in ("queued", "running"):
                return None
            row = connection.execute(
                """
                SELECT state, try_number, next_retry_at
                FROM task_instance
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1
                """,
                (dag_id, run_id, task_id),
            ).fetchone()
            if row is None:
                raise StateConflictError(
                    f"TaskInstance 不存在: {dag_id}/{run_id}/{task_id}"
                )
            state, try_number, next_retry_at = row
            if (
                state == TaskInstanceState.UP_FOR_RETRY.value
                and next_retry_at
                and next_retry_at > now_iso
            ):
                return None
            if state == TaskInstanceState.SUCCESS.value:
                return None
            if state in {
                TaskInstanceState.FAILED.value,
                TaskInstanceState.UPSTREAM_FAILED.value,
                TaskInstanceState.SKIPPED.value,
                TaskInstanceState.RUNNING.value,
            }:
                raise StateConflictError(
                    f"TaskInstance {task_id} 当前状态 {state!r} 不能开始新 try。"
                )
            next_try = int(try_number) + 1
            updated = connection.execute(
                """
                UPDATE task_instance
                SET state = ?,
                    try_number = ?,
                    started_at = ?,
                    next_retry_at = NULL,
                    executor_id = ?,
                    worker_pid = ?,
                    lease_expires_at = ?,
                    last_heartbeat_at = ?
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1
                  AND (state IS NULL OR state IN (?, ?, ?))
                """,
                (
                    TaskInstanceState.RUNNING.value,
                    next_try,
                    now_iso,
                    owner,
                    os.getpid(),
                    lease_until,
                    now_iso,
                    dag_id,
                    run_id,
                    task_id,
                    TaskInstanceState.SCHEDULED.value,
                    TaskInstanceState.QUEUED.value,
                    TaskInstanceState.UP_FOR_RETRY.value,
                ),
            ).rowcount
            if updated != 1:
                raise StateConflictError(f"TaskInstance {task_id} 状态发生并发变化。")
            connection.execute(
                """
                INSERT INTO task_try(
                    dag_id, run_id, task_id, map_index, try_number, state, started_at
                )
                VALUES (?, ?, ?, -1, ?, ?, ?)
                """,
                (
                    dag_id,
                    run_id,
                    task_id,
                    next_try,
                    TaskInstanceState.RUNNING.value,
                    now_iso,
                ),
            )
            connection.execute(
                """
                INSERT INTO task_heartbeat(
                    dag_id, run_id, task_id, map_index, try_number, heartbeat_at, worker_pid
                )
                VALUES (?, ?, ?, -1, ?, ?, ?)
                ON CONFLICT(dag_id, run_id, task_id, map_index, try_number)
                DO UPDATE SET heartbeat_at = excluded.heartbeat_at,
                              worker_pid = excluded.worker_pid
                """,
                (dag_id, run_id, task_id, next_try, now_iso, os.getpid()),
            )
        return next_try

    def task_retry_at(self, dag_id: str, run_id: str, task_id: str) -> datetime | None:
        self.init_db()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT next_retry_at FROM task_instance WHERE dag_id = ? AND run_id = ? AND task_id = ?",
                (dag_id, run_id, task_id),
            ).fetchone()
        return datetime.fromisoformat(row[0]) if row and row[0] else None

    def renew_task_lease(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int,
        owner: str | None = None,
        lease_seconds: int = 30,
        now: datetime | None = None,
    ) -> bool:
        self.init_db()
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            raise ValueError("now 必须带时区。")
        owner = owner or f"runner-{os.getpid()}"
        now_iso = _iso(current)
        lease_until = _iso(current + timedelta(seconds=lease_seconds))
        with self._connect() as connection:
            connection.execute("BEGIN")
            updated = connection.execute(
                """
                UPDATE task_instance
                SET lease_expires_at = ?, last_heartbeat_at = ?, executor_id = ?, worker_pid = ?
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1
                  AND state = ? AND try_number = ?
                """,
                (
                    lease_until,
                    now_iso,
                    owner,
                    os.getpid(),
                    dag_id,
                    run_id,
                    task_id,
                    TaskInstanceState.RUNNING.value,
                    try_number,
                ),
            ).rowcount
            if updated != 1:
                return False
            connection.execute(
                """
                INSERT INTO task_heartbeat(
                    dag_id, run_id, task_id, map_index, try_number, heartbeat_at, worker_pid
                )
                VALUES (?, ?, ?, -1, ?, ?, ?)
                ON CONFLICT(dag_id, run_id, task_id, map_index, try_number)
                DO UPDATE SET heartbeat_at = excluded.heartbeat_at,
                              worker_pid = excluded.worker_pid
                """,
                (dag_id, run_id, task_id, try_number, now_iso, os.getpid()),
            )
        return True

    def set_task_try_log_path(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int,
        log_path: str,
    ) -> None:
        self.init_db()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE task_try
                SET log_path = ?
                WHERE dag_id = ? AND run_id = ? AND task_id = ?
                  AND map_index = -1 AND try_number = ?
                """,
                (log_path, dag_id, run_id, task_id, try_number),
            )

    def commit_success(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int,
        xcom_values: dict[str, Any],
    ) -> None:
        self.init_db()
        now = _utc_now()
        encoded_xcom = {
            key: _json_dumps_xcom(value) for key, value in xcom_values.items()
        }
        with self._connect() as connection:
            connection.execute("BEGIN")
            updated = connection.execute(
                """
                UPDATE task_instance
                SET state = ?,
                    ended_at = ?,
                    lease_expires_at = NULL,
                    last_heartbeat_at = NULL,
                    executor_id = NULL,
                    worker_pid = NULL
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1
                  AND state = ? AND try_number = ?
                """,
                (
                    TaskInstanceState.SUCCESS.value,
                    now,
                    dag_id,
                    run_id,
                    task_id,
                    TaskInstanceState.RUNNING.value,
                    try_number,
                ),
            ).rowcount
            if updated != 1:
                raise StateConflictError(f"TaskInstance {task_id} success 提交冲突。")
            connection.execute(
                """
                UPDATE task_try
                SET state = ?, ended_at = ?
                WHERE dag_id = ? AND run_id = ? AND task_id = ?
                  AND map_index = -1 AND try_number = ?
                """,
                (
                    TaskInstanceState.SUCCESS.value,
                    now,
                    dag_id,
                    run_id,
                    task_id,
                    try_number,
                ),
            )
            for key, value_json in encoded_xcom.items():
                connection.execute(
                    """
                    INSERT INTO xcom(dag_id, run_id, task_id, map_index, key, value_json, created_at)
                    VALUES (?, ?, ?, -1, ?, ?, ?)
                    ON CONFLICT(dag_id, run_id, task_id, map_index, key)
                    DO UPDATE SET value_json = excluded.value_json, created_at = excluded.created_at
                    """,
                    (dag_id, run_id, task_id, key, value_json, now),
                )

    def commit_failure(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int,
        error: BaseException,
        retries: int,
        retry_delay: timedelta = timedelta(0),
        retry_factor: float = 1.0,
        max_retry_delay: timedelta | None = None,
    ) -> TaskInstanceState:
        self.init_db()
        current = datetime.now(timezone.utc)
        now = _iso(current)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT retry_base FROM task_instance WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1",
                (dag_id, run_id, task_id),
            ).fetchone()
            if row is None:
                raise StateConflictError(
                    f"TaskInstance 不存在: {dag_id}/{run_id}/{task_id}"
                )
            attempt = try_number - row[0]
            next_state = (
                TaskInstanceState.UP_FOR_RETRY
                if attempt <= retries
                else TaskInstanceState.FAILED
            )
            next_retry = (
                _retry_at(current, attempt, retry_delay, retry_factor, max_retry_delay)
                if next_state == TaskInstanceState.UP_FOR_RETRY
                else None
            )
            updated = connection.execute(
                """
                UPDATE task_instance
                SET state = ?,
                    ended_at = ?,
                    next_retry_at = ?,
                    lease_expires_at = NULL,
                    last_heartbeat_at = NULL,
                    executor_id = NULL,
                    worker_pid = NULL
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1
                  AND state = ? AND try_number = ?
                """,
                (
                    next_state.value,
                    now,
                    next_retry,
                    dag_id,
                    run_id,
                    task_id,
                    TaskInstanceState.RUNNING.value,
                    try_number,
                ),
            ).rowcount
            if updated != 1:
                raise StateConflictError(f"TaskInstance {task_id} failure 提交冲突。")
            connection.execute(
                """
                UPDATE task_try
                SET state = ?, ended_at = ?, error_code = ?, error_message = ?, traceback = ?
                WHERE dag_id = ? AND run_id = ? AND task_id = ?
                  AND map_index = -1 AND try_number = ?
                """,
                (
                    TaskInstanceState.FAILED.value,
                    now,
                    getattr(error, "error_code", type(error).__name__),
                    str(error),
                    "".join(traceback.format_exception(error)),
                    dag_id,
                    run_id,
                    task_id,
                    try_number,
                ),
            )
        return next_state

    def recover_orphaned_task(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        retries: int,
        now: datetime | None = None,
        retry_delay: timedelta = timedelta(0),
        retry_factor: float = 1.0,
        max_retry_delay: timedelta | None = None,
    ) -> TaskInstanceState | None:
        self.init_db()
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            raise ValueError("now 必须带时区。")
        now_iso = _iso(current)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT state, try_number, lease_expires_at, retry_base
                FROM task_instance
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1
                """,
                (dag_id, run_id, task_id),
            ).fetchone()
            if row is None:
                raise StateConflictError(
                    f"TaskInstance 不存在: {dag_id}/{run_id}/{task_id}"
                )
            state, try_number, lease_expires_at, retry_base = row
            if state != TaskInstanceState.RUNNING.value:
                return None
            if lease_expires_at is None:
                return None
            if datetime.fromisoformat(lease_expires_at) > current:
                return None
            next_state = (
                TaskInstanceState.UP_FOR_RETRY
                if int(try_number) - retry_base <= retries
                else TaskInstanceState.FAILED
            )
            next_retry = (
                _retry_at(
                    current,
                    int(try_number) - retry_base,
                    retry_delay,
                    retry_factor,
                    max_retry_delay,
                )
                if next_state == TaskInstanceState.UP_FOR_RETRY
                else None
            )
            updated = connection.execute(
                """
                UPDATE task_instance
                SET state = ?,
                    ended_at = ?,
                    next_retry_at = ?,
                    lease_expires_at = NULL,
                    last_heartbeat_at = NULL,
                    executor_id = NULL,
                    worker_pid = NULL
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1
                  AND state = ? AND try_number = ? AND lease_expires_at = ?
                """,
                (
                    next_state.value,
                    now_iso,
                    next_retry,
                    dag_id,
                    run_id,
                    task_id,
                    TaskInstanceState.RUNNING.value,
                    try_number,
                    lease_expires_at,
                ),
            ).rowcount
            if updated != 1:
                raise StateConflictError(f"TaskInstance {task_id} orphan 恢复冲突。")
            connection.execute(
                """
                UPDATE task_try
                SET state = ?, ended_at = ?, error_code = ?, error_message = ?
                WHERE dag_id = ? AND run_id = ? AND task_id = ?
                  AND map_index = -1 AND try_number = ?
                """,
                (
                    TaskInstanceState.FAILED.value,
                    now_iso,
                    "TASK_PROCESS_LOST",
                    "Task lease expired before success/failure commit.",
                    dag_id,
                    run_id,
                    task_id,
                    try_number,
                ),
            )
            connection.execute(
                """
                INSERT INTO audit_log(action, dag_id, run_id, task_id, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "task_process_lost",
                    dag_id,
                    run_id,
                    task_id,
                    json.dumps(
                        {
                            "try_number": try_number,
                            "lease_expires_at": lease_expires_at,
                            "next_state": next_state.value,
                        },
                        ensure_ascii=False,
                    ),
                    now_iso,
                ),
            )
        return next_state

    def recover_orphaned_tasks_for_run(
        self,
        *,
        dag: Any,
        run_id: str,
        now: datetime | None = None,
    ) -> dict[str, TaskInstanceState]:
        recovered: dict[str, TaskInstanceState] = {}
        for task in dag.tasks:
            delay, factor, maximum = task_retry_settings(dag, task)
            state = self.recover_orphaned_task(
                dag_id=dag.dag_id,
                run_id=run_id,
                task_id=task.task_id,
                retries=_task_retries(dag, task),
                now=now,
                retry_delay=delay,
                retry_factor=factor,
                max_retry_delay=maximum,
            )
            if state is not None:
                recovered[task.task_id] = state
        return recovered

    def mark_task_terminal(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        state: TaskInstanceState,
    ) -> None:
        self.init_db()
        if state not in {
            TaskInstanceState.UPSTREAM_FAILED,
            TaskInstanceState.SKIPPED,
        }:
            raise ValueError("mark_task_terminal 只用于 upstream_failed/skipped。")
        now = _utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE task_instance
                SET state = ?, ended_at = ?, next_retry_at = NULL
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1
                  AND (state IS NULL OR state IN ('scheduled', 'queued', 'up_for_retry'))
                """,
                (state.value, now, dag_id, run_id, task_id),
            )

    def fail_worker_startup(
        self, dag_id: str, run_id: str, task_id: str, *, message: str, log_path: str
    ) -> None:
        self.init_db()
        now = _utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT state, try_number FROM task_instance WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1",
                (dag_id, run_id, task_id),
            ).fetchone()
            if row is None or row[0] not in (
                None,
                "queued",
                "scheduled",
                "up_for_retry",
            ):
                return
            attempt = row[1] + 1
            connection.execute(
                "UPDATE task_instance SET state = 'failed', try_number = ?, ended_at = ?, next_retry_at = NULL "
                "WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1",
                (attempt, now, dag_id, run_id, task_id),
            )
            connection.execute(
                "INSERT INTO task_try(dag_id, run_id, task_id, map_index, try_number, state, started_at, ended_at, log_path, error_code, error_message) "
                "VALUES (?, ?, ?, -1, ?, 'failed', ?, ?, ?, 'WORKER_STARTUP_FAILED', ?)",
                (dag_id, run_id, task_id, attempt, now, now, log_path, message),
            )

    def pull_xcom(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        key: str,
    ) -> Any:
        self.init_db()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT value_json FROM xcom
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1 AND key = ?
                """,
                (dag_id, run_id, task_id, key),
            ).fetchone()
        if row is None:
            raise KeyError(f"XCom 不存在: {dag_id}/{run_id}/{task_id}/{key}")
        return json.loads(row[0])

    def latest_task_log_path(
        self, dag_id: str, run_id: str, task_id: str
    ) -> str | None:
        self.init_db()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT log_path FROM task_try
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1
                ORDER BY try_number DESC
                LIMIT 1
                """,
                (dag_id, run_id, task_id),
            ).fetchone()
        return None if row is None else row[0]

    def acquire_scheduler_lease(
        self,
        *,
        owner: str,
        lease_seconds: int = 30,
        now: datetime | None = None,
    ) -> "SchedulerLeaseResult":
        self.init_db()
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            raise ValueError("now 必须带时区。")
        expires_at = current + timedelta(seconds=lease_seconds)
        current_iso = _iso(current)
        expires_iso = _iso(expires_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if now is None:
                current = datetime.now(timezone.utc)
                current_iso = _iso(current)
                expires_iso = _iso(current + timedelta(seconds=lease_seconds))
            row = connection.execute(
                """
                SELECT owner, lease_expires_at
                FROM scheduler_lease
                WHERE lease_id = 'singleton'
                """
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO scheduler_lease(lease_id, owner, lease_expires_at, updated_at)
                    VALUES ('singleton', ?, ?, ?)
                    """,
                    (owner, expires_iso, current_iso),
                )
                return SchedulerLeaseResult(True, owner, expires_iso)
            existing_owner, existing_expires = row
            if (
                existing_owner != owner
                and datetime.fromisoformat(existing_expires) > current
            ):
                return SchedulerLeaseResult(False, existing_owner, existing_expires)
            connection.execute(
                """
                UPDATE scheduler_lease
                SET owner = ?, lease_expires_at = ?, updated_at = ?
                WHERE lease_id = 'singleton'
                """,
                (owner, expires_iso, current_iso),
            )
            return SchedulerLeaseResult(True, owner, expires_iso)

    def renew_scheduler_lease(
        self, *, owner: str, lease_seconds: int, now: datetime | None = None
    ) -> bool:
        self.init_db()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = now or datetime.now(timezone.utc)
            updated = connection.execute(
                "UPDATE scheduler_lease SET lease_expires_at = ?, updated_at = ? "
                "WHERE lease_id = 'singleton' AND owner = ? AND lease_expires_at > ?",
                (
                    _iso(current + timedelta(seconds=lease_seconds)),
                    _iso(current),
                    owner,
                    _iso(current),
                ),
            ).rowcount
        return updated == 1

    def release_scheduler_lease(self, *, owner: str) -> None:
        self.init_db()
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM scheduler_lease WHERE lease_id = 'singleton' AND owner = ?",
                (owner,),
            )

    def fail_dag_run_definition_changed(
        self,
        *,
        dag_id: str,
        run_id: str,
        expected_hash: str | None,
        actual_hash: str,
    ) -> None:
        now = _utc_now()
        payload = {
            "expected_hash": expected_hash,
            "actual_hash": actual_hash,
            "error_code": "DAG_DEFINITION_CHANGED",
        }
        with self._connect() as connection:
            connection.execute("BEGIN")
            connection.execute(
                """
                UPDATE dag_run
                SET state = ?, end_date = ?
                WHERE dag_id = ? AND run_id = ? AND state NOT IN (?, ?)
                """,
                (
                    DagRunState.FAILED.value,
                    now,
                    dag_id,
                    run_id,
                    DagRunState.SUCCESS.value,
                    DagRunState.FAILED.value,
                ),
            )
            connection.execute(
                """
                INSERT INTO audit_log(action, dag_id, run_id, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "dag_definition_changed",
                    dag_id,
                    run_id,
                    json.dumps(payload, ensure_ascii=False),
                    now,
                ),
            )

    def preview_clear_task_instances(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_ids: list[str] | None = None,
    ) -> "ClearTaskResult":
        self.init_db()
        rows = self._select_clear_candidates(dag_id, run_id, task_ids)
        allowed = {
            TaskInstanceState.SUCCESS.value,
            TaskInstanceState.FAILED.value,
            TaskInstanceState.UPSTREAM_FAILED.value,
            TaskInstanceState.SKIPPED.value,
            TaskInstanceState.UP_FOR_RETRY.value,
        }
        to_clear = [task_id for task_id, state in rows if state in allowed]
        blocked = [
            ClearBlockedTask(task_id, state, "只允许 clear 终态或 up_for_retry 任务。")
            for task_id, state in rows
            if state not in allowed
        ]
        return ClearTaskResult(to_clear=to_clear, blocked=blocked, dry_run=True)

    def clear_task_instances(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_ids: list[str] | None = None,
        actor: str = "cli",
        reason: str = "",
    ) -> "ClearTaskResult":
        self.init_db()
        now = _utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = self._select_clear_candidates(
                dag_id, run_id, task_ids, connection=connection
            )
            allowed = {
                "success",
                "failed",
                "upstream_failed",
                "skipped",
                "up_for_retry",
            }
            to_clear = [task_id for task_id, state in rows if state in allowed]
            blocked = [
                ClearBlockedTask(
                    task_id, state, "只允许 clear 终态或 up_for_retry 任务。"
                )
                for task_id, state in rows
                if state not in allowed
            ]
            if blocked or not to_clear:
                return ClearTaskResult(
                    to_clear=to_clear, blocked=blocked, dry_run=False
                )
            run = connection.execute(
                "SELECT state FROM dag_run WHERE dag_id = ? AND run_id = ?",
                (dag_id, run_id),
            ).fetchone()
            if run and run[0] in ("success", "failed"):
                limit_row = connection.execute(
                    "SELECT max_active_runs FROM dag_definition WHERE dag_id = ?",
                    (dag_id,),
                ).fetchone()
                limit = limit_row[0] if limit_row else 1
                active = connection.execute(
                    "SELECT COUNT(*) FROM dag_run WHERE dag_id = ? AND state IN ('queued', 'running')",
                    (dag_id,),
                ).fetchone()[0]
                if active >= limit:
                    blocked = [
                        ClearBlockedTask(
                            task_id, state, f"DAG 已达到 max_active_runs={limit}。"
                        )
                        for task_id, state in rows
                        if task_id in to_clear
                    ]
                    return ClearTaskResult(
                        to_clear=to_clear, blocked=blocked, dry_run=False
                    )
            payload = {"actor": actor, "reason": reason, "task_ids": to_clear}
            placeholders = ",".join("?" for _ in to_clear)
            params = [dag_id, run_id, *to_clear]
            connection.execute(
                f"""
                DELETE FROM xcom
                WHERE dag_id = ? AND run_id = ? AND task_id IN ({placeholders})
                """,
                params,
            )
            connection.execute(
                f"""
                UPDATE task_instance
                SET state = NULL,
                    retry_base = try_number,
                    queued_at = NULL,
                    started_at = NULL,
                    ended_at = NULL,
                    next_retry_at = NULL,
                    executor_id = NULL,
                    worker_pid = NULL,
                    lease_expires_at = NULL,
                    last_heartbeat_at = NULL
                WHERE dag_id = ? AND run_id = ? AND task_id IN ({placeholders})
                """,
                params,
            )
            connection.execute(
                """
                UPDATE dag_run
                SET state = ?, end_date = NULL
                WHERE dag_id = ? AND run_id = ? AND state IN (?, ?)
                """,
                (
                    DagRunState.QUEUED.value,
                    dag_id,
                    run_id,
                    DagRunState.SUCCESS.value,
                    DagRunState.FAILED.value,
                ),
            )
            connection.execute(
                """
                INSERT INTO audit_log(action, dag_id, run_id, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "tasks_clear",
                    dag_id,
                    run_id,
                    json.dumps(payload, ensure_ascii=False),
                    now,
                ),
            )
        return ClearTaskResult(to_clear=to_clear, blocked=[], dry_run=False)

    def list_audit_actions(self) -> list[tuple[str, str | None, str | None]]:
        self.init_db()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT action, dag_id, run_id
                FROM audit_log
                ORDER BY audit_id
                """
            ).fetchall()
        return [(row[0], row[1], row[2]) for row in rows]

    def _select_clear_candidates(
        self,
        dag_id: str,
        run_id: str,
        task_ids: list[str] | None,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> list[tuple[str, str | None]]:
        query = """
            SELECT task_id, state
            FROM task_instance
            WHERE dag_id = ? AND run_id = ?
        """
        params: list[Any] = [dag_id, run_id]
        if task_ids:
            placeholders = ",".join("?" for _ in task_ids)
            query += f" AND task_id IN ({placeholders})"
            params.extend(task_ids)
        query += " ORDER BY task_id"
        if connection is None:
            with self._connect() as connection:
                rows = connection.execute(query, params).fetchall()
        else:
            rows = connection.execute(query, params).fetchall()
        return [(row[0], row[1]) for row in rows]

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.execute("PRAGMA foreign_keys=ON")
        return connection


@dataclass(frozen=True)
class DagRunRecord:
    dag_id: str
    run_id: str
    run_type: str
    logical_date: str
    data_interval_start: str
    data_interval_end: str
    state: str
    definition_hash: str | None
    conf_json: str | None
    logical_timezone: str | None = None


@dataclass(frozen=True)
class PreviousTaskRecord:
    exists: bool
    run_id: str | None
    state: TaskInstanceState | None


@dataclass(frozen=True)
class SchedulerLeaseResult:
    acquired: bool
    owner: str
    lease_expires_at: str


@dataclass(frozen=True)
class ClearBlockedTask:
    task_id: str
    state: str | None
    reason: str


@dataclass(frozen=True)
class ClearTaskResult:
    to_clear: list[str]
    blocked: list[ClearBlockedTask]
    dry_run: bool


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("datetime 必须带时区。")
    return value.astimezone(timezone.utc).isoformat()


def _assert_scheduler_lease(
    connection: sqlite3.Connection, fence: SchedulerFence | None
) -> None:
    if fence is None:
        return
    current = max(
        datetime.now(timezone.utc),
        fence.started_at
        + timedelta(seconds=time.monotonic() - fence.started_monotonic),
    )
    row = connection.execute(
        "SELECT 1 FROM scheduler_lease WHERE lease_id = 'singleton' AND owner = ? AND lease_expires_at > ?",
        (fence.owner, _iso(current)),
    ).fetchone()
    if row is None:
        raise SchedulerLeaseLost("scheduler lease 已丢失，停止调度。")


def _restore_retry_baselines(connection: sqlite3.Connection) -> None:
    rows = connection.execute(
        "SELECT dag_id, run_id, payload_json, created_at FROM audit_log "
        "WHERE action = 'tasks_clear' ORDER BY audit_id"
    ).fetchall()
    for dag_id, run_id, payload_json, cleared_at in rows:
        try:
            payload = json.loads(payload_json)
        except (TypeError, ValueError):
            continue
        if not isinstance(payload, dict) or not isinstance(
            payload.get("task_ids"), list
        ):
            continue
        for task_id in payload["task_ids"]:
            if not isinstance(task_id, str):
                continue
            row = connection.execute(
                "SELECT COALESCE(MAX(try_number), 0) FROM task_try "
                "WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1 "
                "AND COALESCE(started_at, ended_at) <= ?",
                (dag_id, run_id, task_id, cleared_at),
            ).fetchone()
            connection.execute(
                "UPDATE task_instance SET retry_base = ? "
                "WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1",
                (row[0], dag_id, run_id, task_id),
            )
    connection.execute(
        "UPDATE task_instance SET retry_base = try_number "
        "WHERE state IS NULL AND try_number > 0 AND retry_base = 0"
    )


def _timezone_id(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("datetime 必须带时区。")
    key = getattr(value.tzinfo, "key", None) or getattr(value.tzinfo, "zone", None)
    if key:
        return f"zone:{key}"
    return f"offset:{int(value.utcoffset().total_seconds())}"


def run_timezone(value: str | None) -> tzinfo:
    if value and value.startswith("zone:"):
        try:
            return ZoneInfo(value[5:])
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"运行时区不可用: {value[5:]}") from exc
    if value and value.startswith("offset:"):
        return timezone(timedelta(seconds=int(value[7:])))
    return timezone.utc


def task_retry_settings(
    dag: Any, task: Any
) -> tuple[timedelta, float, timedelta | None]:
    delay = (
        task.retry_delay
        if task.retry_delay is not None
        else dag.default_args.get("retry_delay", timedelta(0))
    )
    maximum = (
        task.max_retry_delay
        if task.max_retry_delay is not None
        else dag.default_args.get("max_retry_delay")
    )
    backoff = task.retry_exponential_backoff
    factor = 2.0 if backoff is True else max(1.0, float(backoff or 1))
    return delay or timedelta(0), factor, maximum


def _retry_at(
    current: datetime,
    attempt: int,
    delay: timedelta,
    factor: float,
    maximum: timedelta | None,
) -> str:
    try:
        seconds = delay.total_seconds() * factor ** max(0, attempt - 1)
    except OverflowError:
        seconds = float("inf")
    if maximum is not None:
        seconds = min(seconds, maximum.total_seconds())
    seconds = max(
        0,
        min(
            seconds,
            (datetime.max.replace(tzinfo=timezone.utc) - current).total_seconds() - 1,
        ),
    )
    return _iso(current + timedelta(seconds=seconds))


def _json_dumps_xcom(value: Any) -> str:
    try:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise XComSerializationError(
            f"XCom value is not JSON serializable: {exc}"
        ) from exc
    if len(encoded.encode("utf-8")) > MAX_XCOM_BYTES:
        raise XComValueTooLargeError(f"XCom value exceeds {MAX_XCOM_BYTES} bytes.")
    return encoded


def _task_retries(dag: Any, task: Any) -> int:
    value = task.retries
    if value is None:
        value = dag.default_args.get("retries", 0)
    return int(value or 0)
