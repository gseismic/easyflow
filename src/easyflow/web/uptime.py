from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

SLOT_SECONDS = 30 * 60


def _timestamp(value: str | None) -> float | None:
    if value is None:
        return None
    return datetime.fromisoformat(value).timestamp()


def workflow_uptime(
    database: Path,
    *,
    dag_ids: list[str],
    start_at: datetime,
    end_at: datetime,
    now: datetime | None = None,
) -> dict:
    """按实际执行区间和失败事件聚合；返回规模仅与工作流数和格子数有关。"""
    if not dag_ids or len(dag_ids) > 100 or any(not item for item in dag_ids):
        raise ValueError("请选择 1 到 100 个工作流。")
    if start_at.tzinfo is None or end_at.tzinfo is None:
        raise ValueError("uptime 起止时间必须带时区。")
    start = start_at.astimezone(UTC)
    end = end_at.astimezone(UTC)
    seconds = (end - start).total_seconds()
    if seconds <= 0 or seconds > 25 * 3600 or seconds % SLOT_SECONDS:
        raise ValueError("uptime 范围必须是正的半小时倍数，且不超过 25 小时。")
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        raise ValueError("now 必须带时区。")
    as_of = current.timestamp()
    ids = list(dict.fromkeys(dag_ids))
    boundaries = [
        (start + timedelta(seconds=index * SLOT_SECONDS)).timestamp()
        for index in range(int(seconds // SLOT_SECONDS) + 1)
    ]
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        # 使用 Python 时间转换保留微秒精度，避免 SQLite 时间函数在整点附近舍入。
        connection.create_function("uptime_time", 1, _timestamp, deterministic=True)
        connection.execute("CREATE TEMP TABLE uptime_ids (dag_id TEXT PRIMARY KEY)")
        connection.executemany(
            "INSERT INTO uptime_ids VALUES (?)", ((item,) for item in ids)
        )
        connection.execute(
            "CREATE TEMP TABLE uptime_slots (position INTEGER, start REAL, end REAL)"
        )
        connection.executemany(
            "INSERT INTO uptime_slots VALUES (?, ?, ?)",
            ((i, boundaries[i], boundaries[i + 1]) for i in range(len(boundaries) - 1)),
        )
        # 两次聚合共享读取快照；不把任务数量和运行数量混为一谈。
        activity = connection.execute(
            """
            WITH intervals AS (
                SELECT t.dag_id, t.run_id, uptime_time(t.started_at) AS started,
                    MIN(COALESCE(uptime_time(t.ended_at), uptime_time(r.end_date),
                        CASE WHEN t.state = 'running' AND r.state = 'running'
                             THEN :now ELSE uptime_time(t.started_at) END), :now) AS ended,
                    (t.state = 'running' AND t.ended_at IS NULL AND r.state = 'running') AS running
                FROM uptime_ids i JOIN task_try t ON t.dag_id = i.dag_id
                JOIN dag_run r ON r.dag_id = t.dag_id AND r.run_id = t.run_id
                WHERE t.started_at IS NOT NULL AND uptime_time(t.started_at) < :end
                UNION ALL
                SELECT r.dag_id, r.run_id, uptime_time(r.start_date),
                    MIN(COALESCE(uptime_time(r.end_date), :now), :now),
                    (r.state = 'running' AND r.end_date IS NULL)
                FROM uptime_ids i JOIN dag_run r ON r.dag_id = i.dag_id
                WHERE r.start_date IS NOT NULL AND r.state != 'queued'
                    AND uptime_time(r.start_date) < :end
                    AND NOT EXISTS (
                        SELECT 1 FROM task_try t
                        WHERE t.dag_id = r.dag_id AND t.run_id = r.run_id
                    )
            )
            SELECT s.position, a.dag_id, COUNT(DISTINCT a.run_id) AS run_count,
                COUNT(DISTINCT CASE WHEN a.running AND s.start <= :now AND :now < s.end
                    THEN a.run_id END) AS running_count,
                MIN(a.run_id) AS sample_run_id
            FROM intervals a JOIN uptime_slots s
                ON a.started < s.end AND a.started <= :now
                AND (a.ended > s.start OR (a.ended = a.started AND a.started >= s.start))
            GROUP BY s.position, a.dag_id
            """,
            {"end": end.timestamp(), "now": as_of},
        ).fetchall()
        failures = connection.execute(
            """
            WITH errors AS (
                SELECT t.dag_id, t.run_id, uptime_time(t.ended_at) AS failed_at,
                    t.task_id, t.try_number
                FROM uptime_ids i JOIN task_try t ON t.dag_id = i.dag_id
                WHERE t.state = 'failed' AND t.ended_at IS NOT NULL
                UNION ALL
                SELECT r.dag_id, r.run_id, uptime_time(r.end_date), NULL, NULL
                FROM uptime_ids i JOIN dag_run r ON r.dag_id = i.dag_id
                WHERE r.state = 'failed' AND r.end_date IS NOT NULL
            ), matched AS (
                SELECT s.position, e.*, ROW_NUMBER() OVER (
                    PARTITION BY s.position, e.dag_id
                    ORDER BY e.run_id, e.task_id IS NULL, e.failed_at, e.task_id, e.try_number
                ) AS priority
            FROM errors e JOIN uptime_slots s ON e.failed_at >= s.start AND e.failed_at < s.end
                AND e.failed_at <= :now
            )
            SELECT position, dag_id, COUNT(DISTINCT run_id) AS failed_run_count,
                MAX(CASE WHEN priority = 1 THEN run_id END) AS sample_run_id,
                MAX(CASE WHEN priority = 1 THEN task_id END) AS sample_task_id,
                MAX(CASE WHEN priority = 1 THEN try_number END) AS sample_try_number
            FROM matched GROUP BY position, dag_id
            """,
            {"now": as_of},
        ).fetchall()
        waiting = connection.execute(
            "SELECT r.dag_id, COUNT(DISTINCT r.run_id) AS waiting_run_count, "
            "MIN(r.run_id) AS sample_run_id FROM uptime_ids i "
            "JOIN dag_run r ON r.dag_id = i.dag_id "
            "WHERE r.state IN ('queued', 'running') AND uptime_time(r.queued_at) <= ? "
            "AND (r.state = 'queued' OR EXISTS (SELECT 1 FROM task_instance t "
            "WHERE t.dag_id = r.dag_id AND t.run_id = r.run_id "
            "AND (t.state IS NULL OR t.state IN ('queued', 'scheduled', 'up_for_retry')))) "
            "GROUP BY r.dag_id",
            (as_of,),
        ).fetchall()

    def iso(timestamp: float) -> str:
        return datetime.fromtimestamp(timestamp, UTC).isoformat()

    workflows = {
        dag_id: [
            {
                "start_at": iso(boundaries[index]),
                "end_at": iso(boundaries[index + 1]),
                "state": "future" if boundaries[index] > as_of else "idle",
                "run_count": 0,
                "failed_run_count": 0,
                "waiting_run_count": 0,
                "sample_run_id": None,
                "sample_task_id": None,
                "sample_try_number": None,
            }
            for index in range(len(boundaries) - 1)
        ]
        for dag_id in ids
    }
    for row in activity:
        slot = workflows[row["dag_id"]][row["position"]]
        slot.update(
            state="running" if row["running_count"] else "active",
            run_count=row["run_count"],
            sample_run_id=row["sample_run_id"],
        )
    if start.timestamp() <= as_of < end.timestamp():
        position = int((as_of - start.timestamp()) // SLOT_SECONDS)
        for row in waiting:
            slot = workflows[row["dag_id"]][position]
            slot["waiting_run_count"] = row["waiting_run_count"]
            if slot["state"] != "running":
                slot.update(state="waiting", sample_run_id=row["sample_run_id"])
    for row in failures:
        slot = workflows[row["dag_id"]][row["position"]]
        slot.update(
            state="failed",
            failed_run_count=row["failed_run_count"],
            sample_run_id=row["sample_run_id"],
            sample_task_id=row["sample_task_id"],
            sample_try_number=row["sample_try_number"],
        )
    return {
        "start_at": start.isoformat(),
        "end_at": end.isoformat(),
        "as_of": current.astimezone(UTC).isoformat(),
        "slot_minutes": 30,
        "workflows": workflows,
    }
