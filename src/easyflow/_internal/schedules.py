from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from typing import Any

from easyflow._internal.cron import (
    cron_next_after,
    cron_next_at_or_after,
    cron_previous_at_or_before,
    cron_previous_before,
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
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


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


def scheduler_intervals(dag: Any, now: datetime) -> list[DataInterval]:
    if now.tzinfo is None:
        raise ValueError("now 必须带时区。")
    schedule = dag.schedule
    if schedule is None:
        return []
    now = now.astimezone(dag_timezone(dag))
    if schedule == "@once":
        logical_date = dag.start_date or now
        if _utc(logical_date) > _utc(now) or (
            dag.end_date and _utc(logical_date) > _utc(dag.end_date)
        ):
            return []
        return [DataInterval(logical_date, logical_date, logical_date)]
    anchor = dag.start_date or _EPOCH
    latest = latest_ended_interval(schedule, now, anchor=anchor)
    if latest is None:
        return []
    if dag.end_date is not None and _utc(latest.logical_date) > _utc(dag.end_date):
        end = dag.end_date.astimezone(now.tzinfo)
        if isinstance(schedule, timedelta):
            count = (_utc(end) - _utc(anchor)) // schedule
            last_start = (_utc(anchor) + count * schedule).astimezone(now.tzinfo)
        else:
            last_start = cron_previous_at_or_before(_expression(schedule), end)
        latest = interval_for_logical_date(schedule, last_start)
    if latest is None or (
        dag.start_date is not None and _utc(latest.logical_date) < _utc(dag.start_date)
    ):
        return []
    if not dag.catchup or dag.start_date is None:
        return [latest]
    intervals: list[DataInterval] = []
    current = _ceil_logical_date(schedule, dag.start_date, anchor=anchor)
    while _utc(current) <= _utc(latest.logical_date):
        interval = interval_for_logical_date(schedule, current)
        if _utc(interval.data_interval_end) > _utc(now):
            break
        if dag.end_date is not None and _utc(current) > _utc(dag.end_date):
            break
        intervals.append(interval)
        current = next_logical_date(schedule, current)
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
    current = _ceil_logical_date(schedule, start, anchor=anchor or _EPOCH)
    intervals = []
    while current.date() <= end_date:
        intervals.append(interval_for_logical_date(schedule, current))
        next_value = next_logical_date(schedule, current)
        if _utc(next_value) <= _utc(current):
            raise ValueError("schedule 产生了非递增区间。")
        current = next_value
    return intervals


def latest_ended_interval(
    schedule: str | timedelta, now: datetime, *, anchor: datetime | None = None
) -> DataInterval | None:
    if isinstance(schedule, timedelta):
        anchor_utc = _utc(anchor or _EPOCH)
        count = (_utc(now) - anchor_utc) // schedule
        start = (anchor_utc + (count - 1) * schedule).astimezone(now.tzinfo)
        return interval_for_logical_date(schedule, start)
    if schedule == "@once":
        raise ValueError("@once 没有周期性区间。")
    expression = _expression(schedule)
    end = cron_previous_at_or_before(expression, now)
    start = cron_previous_before(expression, end)
    return DataInterval(start, start, end)


def interval_for_logical_date(
    schedule: str | timedelta, logical_date: datetime
) -> DataInterval:
    if schedule == "@once":
        return DataInterval(logical_date, logical_date, logical_date)
    end = next_logical_date(schedule, logical_date)
    return DataInterval(logical_date, logical_date, end)


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
