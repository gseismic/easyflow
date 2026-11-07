from __future__ import annotations

from datetime import datetime, timedelta, timezone

from croniter import CroniterError, croniter


def validate_cron(expression: str) -> None:
    if len(expression.split()) != 5:
        raise ValueError(
            f"cron schedule {expression!r} 必须是 5 字段表达式：minute hour day month weekday。"
        )
    if not croniter.is_valid(expression):
        raise ValueError(f"cron schedule {expression!r} 不是有效 cron 表达式。")


def cron_next_after(expression: str, value: datetime) -> datetime:
    return _search(expression, value, forward=True, inclusive=False)


def cron_next_at_or_after(expression: str, value: datetime) -> datetime:
    return _search(expression, value, forward=True, inclusive=True)


def cron_previous_before(expression: str, value: datetime) -> datetime:
    return _search(expression, value, forward=False, inclusive=False)


def cron_previous_at_or_before(expression: str, value: datetime) -> datetime:
    return _search(expression, value, forward=False, inclusive=True)


def is_cron_schedule(schedule: object) -> bool:
    return isinstance(schedule, str) and not schedule.startswith("@")


def _search(
    expression: str, value: datetime, *, forward: bool, inclusive: bool
) -> datetime:
    validate_cron(expression)
    if value.tzinfo is None:
        raise ValueError("cron 时间必须带时区。")
    zone = value.tzinfo
    instant = value.astimezone(timezone.utc)
    wall = value.replace(tzinfo=None, second=0, microsecond=0)
    # 回拨附近同一个墙上时间有两个真实时刻，需搜索相邻的墙上时间。
    nearby_transition = (instant - timedelta(days=1)).astimezone(zone).utcoffset() != (
        instant + timedelta(days=1)
    ).astimezone(zone).utcoffset()
    direction = 1 if forward else -1
    cursor = wall - timedelta(hours=4 * direction) if nearby_transition else wall
    if croniter.match(expression, cursor, day_or=True):
        candidate_wall = cursor
    else:
        try:
            iterator = croniter(expression, cursor, day_or=True)
            candidate_wall = (
                iterator.get_next(datetime) if forward else iterator.get_prev(datetime)
            )
        except CroniterError as exc:
            raise ValueError(f"cron schedule {expression!r} 找不到运行时间。") from exc
    best: datetime | None = None
    while True:
        for fold in (0, 1):
            candidate = candidate_wall.replace(tzinfo=zone, fold=fold)
            actual = candidate.astimezone(timezone.utc)
            # 往返转换可以排除春季跳时产生的不存在的本地时间。
            if actual.astimezone(zone).replace(tzinfo=None) != candidate_wall:
                continue
            acceptable = (
                (actual >= instant if inclusive else actual > instant)
                if forward
                else (actual <= instant if inclusive else actual < instant)
            )
            if acceptable and (
                best is None
                or (
                    actual < best.astimezone(timezone.utc)
                    if forward
                    else actual > best.astimezone(timezone.utc)
                )
            ):
                best = candidate
        if best is not None:
            if (
                not nearby_transition
                or direction
                * (candidate_wall - best.replace(tzinfo=None)).total_seconds()
                >= 4 * 3600
            ):
                return best
        try:
            iterator = croniter(expression, candidate_wall, day_or=True)
            candidate_wall = (
                iterator.get_next(datetime) if forward else iterator.get_prev(datetime)
            )
        except CroniterError as exc:
            raise ValueError(f"cron schedule {expression!r} 找不到运行时间。") from exc
