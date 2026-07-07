from __future__ import annotations

from easyflow.sdk.exceptions import UnsupportedFeatureError

DAG_UNSUPPORTED_KWARGS = {
    "access_control",
    "auto_register",
    "concurrency",
    "dag_display_name",
    "dagrun_timeout",
    "default_view",
    "doc_md",
    "fail_fast",
    "is_paused_upon_creation",
    "jinja_environment_kwargs",
    "max_consecutive_failed_dag_runs",
    "max_active_tasks",
    "on_failure_callback",
    "on_success_callback",
    "orientation",
    "render_template_as_native_obj",
    "schedule_interval",
    "sla_miss_callback",
    "template_searchpath",
    "timetable",
    "user_defined_filters",
    "user_defined_macros",
}

OPERATOR_UNSUPPORTED_KWARGS = {
    "executor",
    "executor_config",
    "inlets",
    "lineage_data",
    "on_execute_callback",
    "on_failure_callback",
    "on_retry_callback",
    "on_skipped_callback",
    "on_success_callback",
    "outlets",
    "pool",
    "pool_slots",
    "priority_weight",
    "queue",
    "resources",
    "run_as_user",
    "sla",
    "task_display_name",
    "task_group",
    "wait_for_downstream",
    "weight_rule",
}


def reject_kwargs(owner: str, kwargs: dict[str, object], unsupported: set[str]) -> None:
    if not kwargs:
        return
    key = next(iter(kwargs))
    if key in unsupported:
        raise UnsupportedFeatureError(
            f'参数 "{key}" 属于 Airflow 公共接口，但 easyflow v0.1 尚不支持。'
        )
    raise TypeError(f"{owner} got an unexpected keyword argument '{key}'")
