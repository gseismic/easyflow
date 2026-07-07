from __future__ import annotations

from datetime import datetime, timedelta

from croniter import CroniterError, croniter


def validate_cron(expression: str) -> None:
    if len(expression.split()) != 5:
        raise ValueError(
            f"cron schedule {expression!r} 必须是 5 字段表达式：minute hour day month weekday。"
        )
    if not croniter.is_valid(expression):
        raise ValueError(f"cron schedule {expression!r} 不是有效 cron 表达式。")


def cron_next_after(expression: str, value: datetime) -> datetime:
    validate_cron(expression)
    candidate = _floor_minute(value) + timedelta(minutes=1)
    return _next_at_or_after_validated(expression, candidate)


def cron_next_at_or_after(expression: str, value: datetime) -> datetime:
    validate_cron(expression)
    candidate = _floor_minute(value)
    if candidate < value:
        candidate += timedelta(minutes=1)
    return _next_at_or_after_validated(expression, candidate)


def cron_previous_before(expression: str, value: datetime) -> datetime:
    validate_cron(expression)
    candidate = _floor_minute(value)
    if candidate >= value:
        candidate -= timedelta(minutes=1)
    return _previous_at_or_before_validated(expression, candidate)


def cron_previous_at_or_before(expression: str, value: datetime) -> datetime:
    validate_cron(expression)
    return _previous_at_or_before_validated(expression, _floor_minute(value))


def is_cron_schedule(schedule: object) -> bool:
    return isinstance(schedule, str) and not schedule.startswith("@")


def _next_at_or_after_validated(expression: str, candidate: datetime) -> datetime:
    if _matches(expression, candidate):
        return candidate
    try:
        return croniter(expression, candidate, day_or=True).get_next(datetime)
    except CroniterError as exc:
        raise ValueError(f"cron schedule {expression!r} 找不到下一次运行时间。") from exc


def _previous_at_or_before_validated(expression: str, candidate: datetime) -> datetime:
    if _matches(expression, candidate):
        return candidate
    try:
        return croniter(expression, candidate, day_or=True).get_prev(datetime)
    except CroniterError as exc:
        raise ValueError(f"cron schedule {expression!r} 找不到上一次运行时间。") from exc


def _matches(expression: str, value: datetime) -> bool:
    return bool(croniter.match(expression, _floor_minute(value), day_or=True))


def _floor_minute(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)
