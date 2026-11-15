from __future__ import annotations

import sqlite3
from bisect import bisect_right
from datetime import UTC, datetime, time, timedelta
from itertools import pairwise
from math import ceil, floor
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

SLOT_SECONDS = 30 * 60


def _timestamp(value: str | None) -> float | None:
    if value is None:
        return None
    return datetime.fromisoformat(value).timestamp()


def _range_params(start: datetime, end: datetime) -> dict[str, float]:
    # SQLite 时间只用于可索引的粗筛；预留一秒以避免微秒舍入误删边界记录。
    # 无法由 SQLite 解析的 Python ISO 格式在查询中保留，仍按 _timestamp 精确判断。
    return {
        "start_jd": start.timestamp() / 86400 + 2440587.5 - 1 / 86400,
        "end_jd": end.timestamp() / 86400 + 2440587.5 + 1 / 86400,
    }


def workflow_uptime(
    database: Path,
    *,
    dag_ids: list[str],
    start_at: datetime | None = None,
    end_at: datetime | None = None,
    lookback_hours: int | None = None,
    granularity: str = "half_hour",
    timezone_name: str = "UTC",
    now: datetime | None = None,
) -> dict:
    """按实际执行区间和失败事件聚合；返回规模仅与工作流数和格子数有关。"""
    if not dag_ids or len(dag_ids) > 100 or any(not item for item in dag_ids):
        raise ValueError("请选择 1 到 100 个工作流。")
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        raise ValueError("now 必须带时区。")
    if lookback_hours is not None:
        if type(lookback_hours) is not int or not 1 <= lookback_hours <= 168:
            raise ValueError("最近小时数必须为 1 到 168 的整数。")
        if start_at is not None or end_at is not None or granularity != "half_hour":
            raise ValueError("最近小时不能与固定起止时间或按天汇总混用。")
        end_at = current
        start_at = current.astimezone(UTC) - timedelta(hours=lookback_hours)
    if start_at is None or end_at is None:
        raise ValueError("请选择最近小时数，或同时提供起止时间。")
    if start_at.tzinfo is None or end_at.tzinfo is None:
        raise ValueError("uptime 起止时间必须带时区。")
    start = start_at.astimezone(UTC)
    end = end_at.astimezone(UTC)
    seconds = (end - start).total_seconds()
    if lookback_hours is not None:
        # 中间桶固定在 UTC 半小时；裁剪首尾，避免轮询改变所有格子的标识。
        first = (floor(start.timestamp() / SLOT_SECONDS) + 1) * SLOT_SECONDS
        boundaries = [start.timestamp()]
        boundaries.extend(range(first, ceil(end.timestamp()), SLOT_SECONDS))
        boundaries.append(end.timestamp())
    elif granularity == "half_hour":
        if seconds <= 0 or seconds > 25 * 3600 or seconds % SLOT_SECONDS:
            raise ValueError("uptime 范围必须是正的半小时倍数，且不超过 25 小时。")
        boundaries = [
            (start + timedelta(seconds=index * SLOT_SECONDS)).timestamp()
            for index in range(int(seconds // SLOT_SECONDS) + 1)
        ]
    elif granularity == "day":
        try:
            zone = ZoneInfo(timezone_name)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError("uptime 时区无效，请使用有效的 IANA 时区。") from exc
        local_start, local_end = start.astimezone(zone), end.astimezone(zone)
        days = (local_end.date() - local_start.date()).days
        def midnight(day):
            return datetime.combine(day, time.min, zone).astimezone(UTC)

        if start != midnight(local_start.date()) or end != midnight(local_end.date()):
            raise ValueError("按天汇总的起止时间必须是指定时区的午夜。")
        if not 1 <= days <= 31:
            raise ValueError("按天汇总的范围必须为 1 到 31 个本地日。")
        boundaries = [
            midnight(local_start.date() + timedelta(days=index)).timestamp()
            for index in range(days + 1)
        ]
        if any(left >= right for left, right in pairwise(boundaries)):
            raise ValueError("所选范围包含不可表示的本地日期，请调整日期。")
    else:
        raise ValueError("uptime 粒度必须为 half_hour 或 day。")
    as_of = current.timestamp()
    ids = list(dict.fromkeys(dag_ids))
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
        # 各项聚合共享读取快照；不把任务数量和运行数量混为一谈。
        # CROSS JOIN 固定由少量工作流 ID 驱动范围索引，避免优化器先扫描全部历史。
        activity = connection.execute(
            """
            WITH intervals AS (
                SELECT t.dag_id, t.run_id, uptime_time(t.started_at) AS started,
                    MIN(COALESCE(uptime_time(t.ended_at), uptime_time(r.end_date),
                        CASE WHEN t.state = 'running' AND r.state = 'running'
                             THEN :now ELSE uptime_time(t.started_at) END), :now) AS ended,
                    (t.state = 'running' AND t.ended_at IS NULL AND r.state = 'running') AS running
                FROM uptime_ids i CROSS JOIN task_try t ON t.dag_id = i.dag_id
                CROSS JOIN dag_run r ON r.dag_id = t.dag_id AND r.run_id = t.run_id
                WHERE COALESCE(julianday(t.ended_at), 1e100) >= :start_jd
                    AND COALESCE(julianday(t.started_at), 0) < :end_jd
                    AND t.started_at IS NOT NULL AND uptime_time(t.started_at) < :end
                UNION ALL
                SELECT r.dag_id, r.run_id, uptime_time(r.start_date),
                    MIN(COALESCE(uptime_time(r.end_date), :now), :now),
                    (r.state = 'running' AND r.end_date IS NULL)
                FROM uptime_ids i CROSS JOIN dag_run r ON r.dag_id = i.dag_id
                WHERE r.start_date IS NOT NULL AND r.state != 'queued'
                    AND COALESCE(julianday(r.end_date), 1e100) >= :start_jd
                    AND COALESCE(julianday(r.start_date), 0) < :end_jd
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
            {**_range_params(start, end), "end": end.timestamp(), "now": as_of},
        ).fetchall()
        failures = connection.execute(
            """
            WITH errors AS (
                SELECT t.dag_id, t.run_id, uptime_time(t.ended_at) AS failed_at,
                    t.task_id, t.try_number
                FROM uptime_ids i CROSS JOIN task_try t ON t.dag_id = i.dag_id
                WHERE t.state = 'failed' AND t.ended_at IS NOT NULL
                    AND COALESCE(julianday(t.ended_at), 1e100) >= :start_jd
                    AND COALESCE(julianday(t.ended_at), 0) < :end_jd
                UNION ALL
                SELECT r.dag_id, r.run_id, uptime_time(r.end_date), NULL, NULL
                FROM uptime_ids i CROSS JOIN dag_run r ON r.dag_id = i.dag_id
                WHERE r.state = 'failed' AND r.end_date IS NOT NULL
                    AND COALESCE(julianday(r.end_date), 1e100) >= :start_jd
                    AND COALESCE(julianday(r.end_date), 0) < :end_jd
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
            {**_range_params(start, end), "now": as_of},
        ).fetchall()
        waiting = connection.execute(
            "SELECT r.dag_id, COUNT(DISTINCT r.run_id) AS waiting_run_count, "
            "MIN(r.run_id) AS sample_run_id FROM uptime_ids i "
            "CROSS JOIN dag_run r ON r.dag_id = i.dag_id "
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
    if start.timestamp() <= as_of < end.timestamp() or lookback_hours is not None:
        # 最近窗口止于 now；等待是该时刻的快照，仍应显示在最后一格。
        position = min(len(boundaries) - 2, bisect_right(boundaries, as_of) - 1)
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
        **({"lookback_hours": lookback_hours} if lookback_hours is not None else {}),
        "as_of": current.astimezone(UTC).isoformat(),
        "granularity": granularity,
        "timezone": timezone_name if granularity == "day" else "UTC",
        "slot_minutes": 30 if granularity == "half_hour" else None,
        "workflows": workflows,
    }


def workflow_failures(
    database: Path,
    *,
    dag_id: str,
    start_at: datetime,
    end_at: datetime,
    limit: int = 25,
    offset: int = 0,
    now: datetime | None = None,
) -> dict:
    """按失败发生时间分页读取历史尝试；同格没有任务失败时补充运行级失败。"""
    if not dag_id:
        raise ValueError("请选择一个工作流。")
    if start_at.tzinfo is None or end_at.tzinfo is None:
        raise ValueError("失败记录的起止时间必须带时区。")
    start, end = start_at.astimezone(UTC), end_at.astimezone(UTC)
    if not 0 < (end - start).total_seconds() <= 25 * 3600:
        raise ValueError("失败记录的范围必须大于零且不超过 25 小时。")
    if not 1 <= limit <= 100 or not 0 <= offset <= 2**63 - 1:
        raise ValueError("失败记录每页为 1 到 100 条，偏移量必须为非负的 64 位整数。")
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        raise ValueError("now 必须带时区。")
    params = {
        **_range_params(start, end),
        "dag_id": dag_id,
        "start": start.timestamp(),
        "end": end.timestamp(),
        "now": current.timestamp(),
        "limit": limit,
        "offset": offset,
    }
    failures = """
        WITH failed_tasks AS (
            SELECT t.run_id, t.task_id, t.try_number, t.started_at, t.ended_at,
                uptime_time(t.ended_at) AS failed_at,
                SUBSTR(t.error_code, 1, 200) AS error_code,
                SUBSTR(t.error_message, 1, 2000) AS error_message,
                LENGTH(COALESCE(t.error_message, '')) > 2000 AS error_message_truncated,
                r.state AS run_state
            FROM task_try t JOIN dag_run r ON r.dag_id = t.dag_id AND r.run_id = t.run_id
            WHERE t.dag_id = :dag_id AND t.state = 'failed'
                AND COALESCE(julianday(t.ended_at), 1e100) >= :start_jd
                AND COALESCE(julianday(t.ended_at), 0) < :end_jd
                AND uptime_time(t.ended_at) >= :start AND uptime_time(t.ended_at) < :end
                AND uptime_time(t.ended_at) <= :now
        ), failures AS (
            SELECT * FROM failed_tasks
            UNION ALL
            SELECT r.run_id, NULL, NULL, r.start_date, r.end_date,
                uptime_time(r.end_date), NULL, NULL, 0, r.state
            FROM dag_run r
            WHERE r.dag_id = :dag_id AND r.state = 'failed'
                AND COALESCE(julianday(r.end_date), 1e100) >= :start_jd
                AND COALESCE(julianday(r.end_date), 0) < :end_jd
                AND uptime_time(r.end_date) >= :start AND uptime_time(r.end_date) < :end
                AND uptime_time(r.end_date) <= :now
                AND NOT EXISTS (SELECT 1 FROM failed_tasks t WHERE t.run_id = r.run_id)
        )
    """
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        connection.create_function("uptime_time", 1, _timestamp, deterministic=True)
        # 计数与当前页共享快照，分页之间允许读取最新状态。
        connection.execute("BEGIN")
        counts = connection.execute(
            failures
            + "SELECT COUNT(*) AS total, COUNT(DISTINCT run_id) AS runs FROM failures",
            params,
        ).fetchone()
        rows = connection.execute(
            failures
            + "SELECT run_id, task_id, try_number, started_at, ended_at, error_code, "
            "error_message, error_message_truncated, run_state FROM failures "
            "ORDER BY failed_at DESC, run_id, task_id, try_number DESC LIMIT :limit OFFSET :offset",
            params,
        ).fetchall()
    items = [dict(row) for row in rows]
    for item in items:
        item["error_message_truncated"] = bool(item["error_message_truncated"])
    return {
        "start_at": start.isoformat(),
        "end_at": end.isoformat(),
        "as_of": current.astimezone(UTC).isoformat(),
        "items": items,
        "total": counts["total"],
        "failed_run_count": counts["runs"],
        "next_offset": offset + limit if offset + limit < counts["total"] else None,
    }
