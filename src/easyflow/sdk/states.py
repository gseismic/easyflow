from enum import StrEnum


class DagRunState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class TaskInstanceState(StrEnum):
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RUNNING = "running"
    UP_FOR_RETRY = "up_for_retry"
    SUCCESS = "success"
    FAILED = "failed"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"


class TriggerRule(StrEnum):
    ALL_SUCCESS = "all_success"
    ALL_DONE = "all_done"
    ONE_SUCCESS = "one_success"
    ONE_FAILED = "one_failed"
    NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"
    ALWAYS = "always"
