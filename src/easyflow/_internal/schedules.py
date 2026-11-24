from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, timezone, tzinfo
from itertools import islice
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
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
}
MAX_CATCHUP_PER_SCAN = 1000
_MICROSECOND = timedelta(microseconds=1)


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
    start_date: date | datetime,
    end_date: date | datetime,
    tz: tzinfo = timezone.utc,
    *,
    anchor: datetime | None = None,
    max_intervals: int | None = None,
    start_bound: datetime | None = None,
    end_bound: datetime | None = None,
) -> list[DataInterval]:
    if max_intervals is not None and max_intervals < 1:
        raise ValueError("max_intervals 必须大于 0。")
    ranges = backfill_logical_date_ranges(
        schedule,
        start_date,
        end_date,
        tz,
        anchor=anchor,
        start_bound=start_bound,
        end_bound=end_bound,
    )
    intervals = (
        interval
        for first, last in ranges
        for interval in iter_backfill_intervals(schedule, first, last)
    )
    if max_intervals is None:
        return list(intervals)
    preview = list(islice(intervals, max_intervals + 1))
    if len(preview) > max_intervals:
        raise ValueError(
            f"回补预览最多支持 {max_intervals} 个区间；请缩小范围或使用带时区的精确时间分段预览。"
        )
    return preview


def backfill_logical_date_ranges(
    schedule: str | timedelta | None,
    start_date: date | datetime,
    end_date: date | datetime,
    tz: tzinfo = UTC,
    *,
    anchor: datetime | None = None,
    start_bound: datetime | None = None,
    end_bound: datetime | None = None,
) -> list[tuple[datetime, datetime]]:
    """计算裁剪后的逻辑日期范围，保留原调度锚点及跨日回拨的间隔。"""
    if schedule is None:
        raise ValueError("无 schedule 的 DAG 不支持 backfill。")
    start, end, gaps = _backfill_window(start_date, end_date, tz)
    if end < start:
        if type(start_date) is date and type(end_date) is date:
            # 有些时区调整会跳过整天。
            return []
        raise ValueError("end-date 必须大于等于 start-date。")
    anchor = anchor or start
    if start_bound is not None:
        start = max(start, _utc(start_bound))
    if end_bound is not None:
        end = min(end, _utc(end_bound))
    spans = []
    for gap_start, gap_end in sorted(gaps):
        spans.append((start, min(end, gap_start - _MICROSECOND)))
        start = max(start, gap_end)
    spans.append((start, end))
    ranges = []
    for left, right in spans:
        if left > right:
            continue
        bounds = _logical_date_bounds(schedule, left.astimezone(tz), right.astimezone(tz), anchor)
        if bounds is not None:
            ranges.append(bounds)
    return ranges


def _logical_date_bounds(
    schedule: str | timedelta, start: datetime, end: datetime, anchor: datetime
) -> tuple[datetime, datetime] | None:
    start_utc, end_utc = _utc(start), _utc(end)
    if schedule == "@once":
        logical = anchor.astimezone(start.tzinfo)
        return (logical, logical) if start_utc <= _utc(anchor) <= end_utc else None
    first = _ceil_logical_date(schedule, start, anchor=anchor)
    if _utc(first) > end_utc:
        return None
    if isinstance(schedule, timedelta):
        steps = (end_utc - _utc(first)) // schedule
        last = (_utc(first) + steps * schedule).astimezone(start.tzinfo)
    else:
        last = cron_previous_at_or_before(_expression(schedule), end)
    return first, last


def iter_backfill_intervals(
    schedule: str | timedelta, first: datetime, last: datetime
) -> Iterator[DataInterval]:
    current = first
    end = _utc(last)
    while _utc(current) <= end:
        yield interval_for_logical_date(schedule, current)
        if _utc(current) == end:
            break
        next_value = next_logical_date(schedule, current)
        if _utc(next_value) <= _utc(current):
            raise ValueError("schedule 产生了非递增区间。")
        current = next_value


def parse_backfill_boundary(value: str) -> date | datetime:
    try:
        return date.fromisoformat(value)
    except ValueError:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("回补精确时间必须带时区。")
        return parsed


def _backfill_window(
    start_date: date | datetime, end_date: date | datetime, tz: tzinfo
) -> tuple[datetime, datetime, list[tuple[datetime, datetime]]]:
    """返回 UTC 包含边界和其中需要排除的左闭右开区间。"""
    if type(start_date) is date and type(end_date) is date and end_date < start_date:
        raise ValueError("end-date 必须大于等于 start-date。")
    for boundary in (start_date, end_date):
        if isinstance(boundary, datetime) and boundary.utcoffset() is None:
            raise ValueError("回补精确时间必须带时区。")
    gaps = []
    if isinstance(start_date, datetime):
        start = _utc(start_date)
    else:
        start, repeated = _local_date_start(start_date, tz)
        if repeated is not None:
            gaps.append(repeated)
    if isinstance(end_date, datetime):
        end = _utc(end_date)
    elif end_date == date.max:
        end = _utc(datetime.combine(end_date, time.max, tzinfo=tz).replace(fold=1))
    else:
        end, repeated = _local_date_start(end_date + timedelta(days=1), tz)
        if repeated is not None:
            rollback, next_midnight = repeated
            gaps.append((end, rollback))
            end = next_midnight
        end -= _MICROSECOND
    return start, end, gaps


def _local_date_start(
    day: date, tz: tzinfo
) -> tuple[datetime, tuple[datetime, datetime] | None]:
    """返回日期的首次边界，以及回拨至前一日到再次跨日的区间。"""
    midnight = datetime.combine(day, time.min, tzinfo=tz)
    first, second = _utc(midnight), _utc(midnight.replace(fold=1))
    if second < first:
        # 午夜不存在时，定位前拨发生的实际时刻。
        return _date_transition(second, first, day, tz, forward=True), None
    if first < second and (second - _MICROSECOND).astimezone(tz).date() < day:
        rollback = _date_transition(first, second - _MICROSECOND, day, tz, forward=False)
        return first, (rollback, second)
    return first, None


def _date_transition(
    before: datetime, after: datetime, day: date, tz: tzinfo, *, forward: bool
) -> datetime:
    while after - before > _MICROSECOND:
        middle = before + (after - before) // 2
        if (middle.astimezone(tz).date() >= day) == forward:
            after = middle
        else:
            before = middle
    return after


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
