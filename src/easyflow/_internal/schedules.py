from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from typing import Any

from easyflow._internal.cron import (
    cron_next_after,
    cron_next_at_or_after,
    cron_previous_at_or_before,
    is_cron_schedule,
    validate_cron,
)

_PRESETS = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@weekly": "0 0 * * 1",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
}
MAX_CATCHUP_PER_SCAN = 1000


@dataclass(frozen=True)
class DataInterval:
    logical_date: datetime
    data_interval_start: datetime
    data_interval_end: datetime


def _utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc)


def _expression(schedule: str) -> str:
    return _PRESETS.get(schedule, schedule)


def dag_timezone(dag: Any) -> tzinfo:
    return dag.start_date.tzinfo if dag.start_date is not None else timezone.utc


def validate_schedule(schedule: str | timedelta | None) -> None:
    if schedule is None:
        return
    if isinstance(schedule, timedelta):
        if schedule <= timedelta(0):
            raise ValueError("timedelta schedule 必须大于 0。")
        return
    if schedule == "@once" or schedule in _PRESETS:
        return
    if is_cron_schedule(schedule):
        validate_cron(schedule)
        return
    raise ValueError(f"暂不支持 schedule {schedule!r}。")


def scheduler_intervals(
    dag: Any,
    now: datetime,
    *,
    last_automated_date: datetime | None = None,
    max_intervals: int = MAX_CATCHUP_PER_SCAN,
) -> list[DataInterval]:
    if now.tzinfo is None:
        raise ValueError("now 必须带时区。")
    if max_intervals < 1:
        raise ValueError("max_intervals 必须大于 0。")
    schedule = dag.schedule
    if schedule is None:
        return []
    now = now.astimezone(dag_timezone(dag))
    last = (
        last_automated_date.astimezone(now.tzinfo)
        if last_automated_date is not None
        else None
    )
    if schedule == "@once":
        if last is not None:
            return []
        logical_date = dag.start_date or now
        if _utc(logical_date) > _utc(now) or (
            dag.end_date and _utc(logical_date) > _utc(dag.end_date)
        ):
            return []
        return [DataInterval(logical_date, logical_date, logical_date)]

    if last is not None:
        first = next_logical_date(schedule, last)
        if dag.start_date is not None:
            start = dag.start_date.astimezone(now.tzinfo)
            if _utc(first) < _utc(start):
                first = (
                    start
                    if isinstance(schedule, timedelta)
                    else cron_next_at_or_after(_expression(schedule), start)
                )
    elif dag.start_date is not None:
        first = (
            dag.start_date.astimezone(now.tzinfo)
            if isinstance(schedule, timedelta)
            else cron_next_at_or_after(
                _expression(schedule), dag.start_date.astimezone(now.tzinfo)
            )
        )
    elif isinstance(schedule, timedelta):
        first = now
    else:
        previous = cron_previous_at_or_before(_expression(schedule), now)
        following = cron_next_after(_expression(schedule), previous)
        tolerance = max((_utc(following) - _utc(previous)) / 10, timedelta(minutes=5))
        first = previous if _utc(now) - _utc(previous) <= tolerance else following

    if not dag.catchup:
        latest = (
            now
            if isinstance(schedule, timedelta)
            else cron_previous_at_or_before(_expression(schedule), now)
        )
        if _utc(first) > _utc(latest):
            return []
        first = latest

    intervals: list[DataInterval] = []
    while _utc(first) <= _utc(now) and len(intervals) < max_intervals:
        if dag.end_date is not None and _utc(first) > _utc(dag.end_date):
            break
        intervals.append(interval_for_logical_date(schedule, first))
        if not dag.catchup:
            break
        first = next_logical_date(schedule, first)
    return intervals


def backfill_intervals(
    schedule: str | timedelta | None,
    start_date: date,
    end_date: date,
    tz: tzinfo = timezone.utc,
    *,
    anchor: datetime | None = None,
) -> list[DataInterval]:
    if schedule is None:
        raise ValueError("无 schedule 的 DAG 不支持 backfill。")
    if end_date < start_date:
        raise ValueError("end-date 必须大于等于 start-date。")
    start = datetime.combine(start_date, time.min, tzinfo=tz)
    if schedule == "@once":
        logical = anchor.astimezone(tz) if anchor is not None else start
        return (
            [interval_for_logical_date(schedule, logical)]
            if start_date <= logical.date() <= end_date
            else []
        )
    current = _ceil_logical_date(schedule, start, anchor=anchor or start)
    intervals = []
    while current.date() <= end_date:
        intervals.append(interval_for_logical_date(schedule, current))
        next_value = next_logical_date(schedule, current)
        if _utc(next_value) <= _utc(current):
            raise ValueError("schedule 产生了非递增区间。")
        current = next_value
    return intervals


def interval_for_logical_date(
    schedule: str | timedelta, logical_date: datetime
) -> DataInterval:
    return DataInterval(logical_date, logical_date, logical_date)


def next_logical_date(schedule: str | timedelta, logical_date: datetime) -> datetime:
    if isinstance(schedule, timedelta):
        return (_utc(logical_date) + schedule).astimezone(logical_date.tzinfo)
    if schedule == "@once":
        return logical_date
    return cron_next_after(_expression(schedule), logical_date)


def _ceil_logical_date(
    schedule: str | timedelta, value: datetime, *, anchor: datetime
) -> datetime:
    if isinstance(schedule, timedelta):
        delta = _utc(value) - _utc(anchor)
        count = -(-delta // schedule)
        return (_utc(anchor) + count * schedule).astimezone(value.tzinfo)
    if schedule == "@once":
        return value
    return cron_next_at_or_after(_expression(schedule), value)


def align_logical_date(schedule: str | timedelta, value: datetime) -> datetime:
    if isinstance(schedule, timedelta) or schedule == "@once":
        return value
    if is_cron_schedule(schedule):
        return cron_next_at_or_after(schedule, value)
    return cron_previous_at_or_before(_expression(schedule), value)


def add_months(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 + months
    return value.replace(
        year=value.year + month_index // 12, month=month_index % 12 + 1, day=1
    )
