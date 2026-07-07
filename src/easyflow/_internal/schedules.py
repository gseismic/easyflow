from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from easyflow._internal.cron import (
    cron_next_after,
    cron_next_at_or_after,
    cron_previous_at_or_before,
    cron_previous_before,
    is_cron_schedule,
    validate_cron,
)


@dataclass(frozen=True)
class DataInterval:
    logical_date: datetime
    data_interval_start: datetime
    data_interval_end: datetime


def validate_schedule(schedule: str | timedelta | None) -> None:
    if schedule is None:
        return
    if isinstance(schedule, timedelta):
        if schedule <= timedelta(0):
            raise ValueError("timedelta schedule 必须大于 0。")
        return
    if schedule in {"@once", "@hourly", "@daily", "@weekly", "@monthly", "@yearly"}:
        return
    if is_cron_schedule(schedule):
        validate_cron(schedule)
        return
    raise ValueError(f"暂不支持 schedule {schedule!r}。")


def scheduler_intervals(dag: Any, now: datetime) -> list[DataInterval]:
    if now.tzinfo is None:
        raise ValueError("now 必须带时区。")
    schedule = dag.schedule
    if schedule is None:
        return []
    if schedule == "@once":
        logical_date = dag.start_date or now
        return [DataInterval(logical_date, logical_date, logical_date)] if logical_date <= now else []
    latest = latest_ended_interval(schedule, now)
    if latest is None:
        return []
    start_date = dag.start_date
    if not dag.catchup or start_date is None:
        return [latest]
    intervals = []
    current = align_logical_date(schedule, start_date)
    while True:
        interval = interval_for_logical_date(schedule, current)
        if interval.data_interval_end > now or interval.logical_date > latest.logical_date:
            break
        intervals.append(interval)
        current = next_logical_date(schedule, current)
    return intervals


def backfill_intervals(
    schedule: str | timedelta | None,
    start_date: date,
    end_date: date,
    tz: timezone = timezone.utc,
) -> list[DataInterval]:
    if schedule is None:
        raise ValueError("无 schedule 的 DAG 不支持 backfill。")
    if end_date < start_date:
        raise ValueError("end-date 必须大于等于 start-date。")
    start = datetime.combine(start_date, time.min, tzinfo=tz)
    end = datetime.combine(end_date, time.min, tzinfo=tz)
    intervals = []
    current = align_logical_date(schedule, start)
    while current.date() <= end.date():
        intervals.append(interval_for_logical_date(schedule, current))
        current = next_logical_date(schedule, current)
    return intervals


def latest_ended_interval(schedule: str | timedelta, now: datetime) -> DataInterval | None:
    if isinstance(schedule, timedelta):
        logical_date = now - schedule
        return DataInterval(logical_date, logical_date, logical_date + schedule)
    if schedule == "@hourly":
        floored = now.replace(minute=0, second=0, microsecond=0)
        logical_date = floored - timedelta(hours=1)
        return interval_for_logical_date(schedule, logical_date)
    if schedule == "@daily":
        floored = now.replace(hour=0, minute=0, second=0, microsecond=0)
        logical_date = floored - timedelta(days=1)
        return interval_for_logical_date(schedule, logical_date)
    if schedule == "@weekly":
        floored = datetime.combine(now.date(), time.min, tzinfo=now.tzinfo)
        floored = floored - timedelta(days=floored.weekday())
        return interval_for_logical_date(schedule, floored - timedelta(days=7))
    if schedule == "@monthly":
        first = datetime(now.year, now.month, 1, tzinfo=now.tzinfo)
        return interval_for_logical_date(schedule, add_months(first, -1))
    if schedule == "@yearly":
        first = datetime(now.year, 1, 1, tzinfo=now.tzinfo)
        return interval_for_logical_date(schedule, datetime(now.year - 1, 1, 1, tzinfo=now.tzinfo))
    if is_cron_schedule(schedule):
        end = cron_previous_at_or_before(schedule, now)
        start = cron_previous_before(schedule, end)
        return DataInterval(start, start, end)
    if isinstance(schedule, str) and schedule.startswith("@"):
        raise ValueError(f"暂不支持 schedule {schedule!r}。")
    raise ValueError(f"暂不支持 schedule {schedule!r}。")


def interval_for_logical_date(schedule: str | timedelta, logical_date: datetime) -> DataInterval:
    if isinstance(schedule, timedelta):
        return DataInterval(logical_date, logical_date, logical_date + schedule)
    if schedule == "@hourly":
        return DataInterval(logical_date, logical_date, logical_date + timedelta(hours=1))
    if schedule == "@daily":
        return DataInterval(logical_date, logical_date, logical_date + timedelta(days=1))
    if schedule == "@weekly":
        return DataInterval(logical_date, logical_date, logical_date + timedelta(days=7))
    if schedule == "@monthly":
        return DataInterval(logical_date, logical_date, add_months(logical_date, 1))
    if schedule == "@yearly":
        return DataInterval(logical_date, logical_date, add_months(logical_date, 12))
    if schedule == "@once":
        return DataInterval(logical_date, logical_date, logical_date)
    if is_cron_schedule(schedule):
        validate_cron(schedule)
        return DataInterval(logical_date, logical_date, cron_next_after(schedule, logical_date))
    raise ValueError(f"暂不支持 schedule {schedule!r}。")


def next_logical_date(schedule: str | timedelta, logical_date: datetime) -> datetime:
    if isinstance(schedule, timedelta):
        return logical_date + schedule
    if schedule == "@hourly":
        return logical_date + timedelta(hours=1)
    if schedule == "@daily":
        return logical_date + timedelta(days=1)
    if schedule == "@weekly":
        return logical_date + timedelta(days=7)
    if schedule == "@monthly":
        return add_months(logical_date, 1)
    if schedule == "@yearly":
        return add_months(logical_date, 12)
    if schedule == "@once":
        return logical_date
    if is_cron_schedule(schedule):
        validate_cron(schedule)
        return cron_next_after(schedule, logical_date)
    raise ValueError(f"暂不支持 schedule {schedule!r}。")


def align_logical_date(schedule: str | timedelta, value: datetime) -> datetime:
    if isinstance(schedule, timedelta):
        return value
    if schedule == "@hourly":
        return value.replace(minute=0, second=0, microsecond=0)
    if schedule == "@daily":
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    if schedule == "@weekly":
        base = value.replace(hour=0, minute=0, second=0, microsecond=0)
        return base - timedelta(days=base.weekday())
    if schedule == "@monthly":
        return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if schedule == "@yearly":
        return value.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    if schedule == "@once":
        return value
    if is_cron_schedule(schedule):
        validate_cron(schedule)
        return cron_next_at_or_after(schedule, value)
    raise ValueError(f"暂不支持 schedule {schedule!r}。")


def add_months(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    return value.replace(year=year, month=month, day=1)
