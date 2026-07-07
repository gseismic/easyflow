from __future__ import annotations

import json
import os
import sqlite3
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from easyflow.sdk.exceptions import StateConflictError, XComSerializationError, XComValueTooLargeError
from easyflow.sdk.states import DagRunState, TaskInstanceState

MAX_XCOM_BYTES = 1_048_576


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
    ) -> None:
        now = _utc_now()
        self.init_db()
        with self._connect() as connection:
            connection.execute("BEGIN")
            connection.execute(
                """
                INSERT INTO dag_definition(dag_id, definition_hash, file_path, parsed_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(dag_id) DO UPDATE SET
                    definition_hash = excluded.definition_hash,
                    file_path = excluded.file_path,
                    parsed_at = excluded.parsed_at
                """,
                (dag.dag_id, definition_hash, file_path, now),
            )
            connection.execute(
                """
                INSERT INTO dag_run(
                    dag_id, run_id, run_type, logical_date, data_interval_start,
                    data_interval_end, state, definition_hash, backfill_id, queued_at,
                    conf_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ORDER BY queued_at DESC
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

    def get_dag_run(self, dag_id: str, run_id: str) -> "DagRunRecord | None":
        self.init_db()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT dag_id, run_id, run_type, logical_date, data_interval_start,
                       data_interval_end, state, definition_hash, conf_json
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

    def list_task_states(self, dag_id: str, run_id: str) -> dict[str, TaskInstanceState | None]:
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
    ) -> int | None:
        self.init_db()
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            raise ValueError("now 必须带时区。")
        owner = owner or f"runner-{os.getpid()}"
        now_iso = _iso(current)
        lease_until = _iso(current + timedelta(seconds=lease_seconds))
        with self._connect() as connection:
            connection.execute("BEGIN")
            row = connection.execute(
                """
                SELECT state, try_number
                FROM task_instance
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1
                """,
                (dag_id, run_id, task_id),
            ).fetchone()
            if row is None:
                raise StateConflictError(f"TaskInstance 不存在: {dag_id}/{run_id}/{task_id}")
            state, try_number = row
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
                (dag_id, run_id, task_id, next_try, TaskInstanceState.RUNNING.value, now_iso),
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
    ) -> TaskInstanceState:
        self.init_db()
        now = _utc_now()
        next_state = (
            TaskInstanceState.UP_FOR_RETRY
            if try_number <= retries
            else TaskInstanceState.FAILED
        )
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
                    next_state.value,
                    now,
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
    ) -> TaskInstanceState | None:
        self.init_db()
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            raise ValueError("now 必须带时区。")
        now_iso = _iso(current)
        with self._connect() as connection:
            connection.execute("BEGIN")
            row = connection.execute(
                """
                SELECT state, try_number, lease_expires_at
                FROM task_instance
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1
                """,
                (dag_id, run_id, task_id),
            ).fetchone()
            if row is None:
                raise StateConflictError(f"TaskInstance 不存在: {dag_id}/{run_id}/{task_id}")
            state, try_number, lease_expires_at = row
            if state != TaskInstanceState.RUNNING.value:
                return None
            if lease_expires_at is None:
                return None
            if datetime.fromisoformat(lease_expires_at) > current:
                return None
            next_state = (
                TaskInstanceState.UP_FOR_RETRY
                if int(try_number) <= retries
                else TaskInstanceState.FAILED
            )
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
                  AND state = ? AND try_number = ? AND lease_expires_at = ?
                """,
                (
                    next_state.value,
                    now_iso,
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
            state = self.recover_orphaned_task(
                dag_id=dag.dag_id,
                run_id=run_id,
                task_id=task.task_id,
                retries=_task_retries(dag, task),
                now=now,
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
                SET state = ?, ended_at = ?
                WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = -1
                  AND state IS NULL
                """,
                (state.value, now, dag_id, run_id, task_id),
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

    def latest_task_log_path(self, dag_id: str, run_id: str, task_id: str) -> str | None:
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
            connection.execute("BEGIN")
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
            if existing_owner != owner and datetime.fromisoformat(existing_expires) > current:
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
        preview = self.preview_clear_task_instances(
            dag_id=dag_id,
            run_id=run_id,
            task_ids=task_ids,
        )
        if preview.blocked:
            return ClearTaskResult(
                to_clear=preview.to_clear,
                blocked=preview.blocked,
                dry_run=False,
            )
        if not preview.to_clear:
            return ClearTaskResult(to_clear=[], blocked=[], dry_run=False)
        now = _utc_now()
        payload = {
            "actor": actor,
            "reason": reason,
            "task_ids": preview.to_clear,
        }
        with self._connect() as connection:
            connection.execute("BEGIN")
            placeholders = ",".join("?" for _ in preview.to_clear)
            params = [dag_id, run_id, *preview.to_clear]
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
        return ClearTaskResult(to_clear=preview.to_clear, blocked=[], dry_run=False)

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
        with self._connect() as connection:
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


def _json_dumps_xcom(value: Any) -> str:
    try:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise XComSerializationError(f"XCom value is not JSON serializable: {exc}") from exc
    if len(encoded.encode("utf-8")) > MAX_XCOM_BYTES:
        raise XComValueTooLargeError(f"XCom value exceeds {MAX_XCOM_BYTES} bytes.")
    return encoded


def _task_retries(dag: Any, task: Any) -> int:
    value = task.retries
    if value is None:
        value = dag.default_args.get("retries", 0)
    return int(value or 0)
