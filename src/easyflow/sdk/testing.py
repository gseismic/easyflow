from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from jinja2 import Environment, StrictUndefined, TemplateError

from easyflow.sdk.context import DagRunContext, TaskInstanceContext, set_current_context
from easyflow.sdk.dag import DAG, DagTestResult
from easyflow.sdk.exceptions import (
    AirflowSkipException,
    TemplateRenderError,
    XComSerializationError,
    XComValueTooLargeError,
)
from easyflow.sdk.states import TaskInstanceState, TriggerRule
from easyflow.sdk.xcom import RETURN_VALUE_KEY, XComArg

TERMINAL_STATES = {
    TaskInstanceState.SUCCESS,
    TaskInstanceState.FAILED,
    TaskInstanceState.UPSTREAM_FAILED,
    TaskInstanceState.SKIPPED,
}
MAX_XCOM_BYTES = 1_048_576


@dataclass
class InMemoryXComStore:
    values: dict[tuple[str, str], Any]

    def push(self, task_id: str, key: str, value: Any) -> None:
        encoded = _json_validate(value)
        if len(encoded) > MAX_XCOM_BYTES:
            raise XComValueTooLargeError(
                f"XCom value for task {task_id!r} key {key!r} exceeds {MAX_XCOM_BYTES} bytes."
            )
        self.values[(task_id, key)] = encoded

    def pull(self, task_id: str, key: str = RETURN_VALUE_KEY) -> Any:
        return json.loads(self.values[(task_id, key)])

    def clear_task(self, task_id: str) -> None:
        for stored_key in [key for key in self.values if key[0] == task_id]:
            del self.values[stored_key]


def _json_validate(value: Any) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
    except (TypeError, ValueError) as exc:
        raise XComSerializationError(
            f"XCom value is not JSON serializable: {exc}"
        ) from exc


def resolve_xcom_args(value: Any, xcom_store: InMemoryXComStore) -> Any:
    if isinstance(value, XComArg):
        return xcom_store.pull(value.operator.task_id, value.key)
    if isinstance(value, Mapping):
        return {key: resolve_xcom_args(item, xcom_store) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_xcom_args(item, xcom_store) for item in value]
    if isinstance(value, tuple):
        return tuple(resolve_xcom_args(item, xcom_store) for item in value)
    if isinstance(value, set):
        return {resolve_xcom_args(item, xcom_store) for item in value}
    if isinstance(value, frozenset):
        return frozenset(resolve_xcom_args(item, xcom_store) for item in value)
    return value


def run_dag_in_process(
    dag: DAG,
    *,
    logical_date: datetime,
    run_conf: dict[str, Any],
) -> DagTestResult:
    run_id = f"test__{logical_date.astimezone(timezone.utc).isoformat()}"
    xcom_store = InMemoryXComStore({})
    task_states: dict[str, TaskInstanceState | None] = {
        task.task_id: None for task in dag.tasks
    }
    task_results: dict[str, Any] = {}
    try_numbers: dict[str, int] = {task.task_id: 0 for task in dag.tasks}
    ordered_tasks = dag.topological_sort()

    while not all(state in TERMINAL_STATES for state in task_states.values()):
        progressed = False
        for task in ordered_tasks:
            if task_states[task.task_id] in TERMINAL_STATES:
                continue
            decision = _dependency_decision(task, task_states)
            if decision == "wait":
                continue
            if decision in {"upstream_failed", "skipped"}:
                task_states[task.task_id] = (
                    TaskInstanceState.UPSTREAM_FAILED
                    if decision == "upstream_failed"
                    else TaskInstanceState.SKIPPED
                )
                progressed = True
                continue

            task_states[task.task_id] = _run_task_with_retries(
                dag=dag,
                task=task,
                logical_date=logical_date,
                run_id=run_id,
                run_conf=run_conf,
                xcom_store=xcom_store,
                task_results=task_results,
                try_numbers=try_numbers,
            )
            progressed = True

        if not progressed:
            waiting = [
                task_id
                for task_id, state in task_states.items()
                if state not in TERMINAL_STATES
            ]
            raise RuntimeError(
                f"DAG execution made no progress. Waiting tasks: {waiting}"
            )

    return DagTestResult(
        dag_id=dag.dag_id,
        run_id=run_id,
        logical_date=logical_date,
        task_states={
            task_id: state
            for task_id, state in task_states.items()
            if state is not None
        },
        task_results=task_results,
        try_numbers=try_numbers,
        leaf_task_ids=frozenset(
            task.task_id for task in dag.tasks if not task.downstream_task_ids
        ),
    )


def _dependency_decision(task: Any, states: dict[str, TaskInstanceState | None]) -> str:
    upstream_states = [states[task_id] for task_id in task.upstream_task_ids]
    if not upstream_states:
        return "run"
    terminal = [state for state in upstream_states if state in TERMINAL_STATES]
    has_failed = any(
        state in {TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED}
        for state in upstream_states
    )
    has_success = any(state == TaskInstanceState.SUCCESS for state in upstream_states)

    rule = task.trigger_rule
    if rule == TriggerRule.ALWAYS:
        return "run"
    if rule == TriggerRule.ONE_SUCCESS and has_success:
        return "run"
    if rule == TriggerRule.ONE_FAILED and has_failed:
        return "run"
    if len(terminal) != len(upstream_states):
        return "wait"
    if rule == TriggerRule.ALL_SUCCESS:
        return (
            "run"
            if all(state == TaskInstanceState.SUCCESS for state in upstream_states)
            else _blocked_decision(has_failed)
        )
    if rule == TriggerRule.ALL_DONE:
        return "run"
    if rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS:
        return (
            "run" if not has_failed and has_success else _blocked_decision(has_failed)
        )
    if rule == TriggerRule.ONE_SUCCESS:
        return _blocked_decision(has_failed)
    if rule == TriggerRule.ONE_FAILED:
        return "skipped"
    return "wait"


def _blocked_decision(has_failed: bool) -> str:
    return "upstream_failed" if has_failed else "skipped"


def _run_task_with_retries(
    *,
    dag: DAG,
    task: Any,
    logical_date: datetime,
    run_id: str,
    run_conf: dict[str, Any],
    xcom_store: InMemoryXComStore,
    task_results: dict[str, Any],
    try_numbers: dict[str, int],
) -> TaskInstanceState:
    retries = _resolve_retries(dag, task)
    last_error: BaseException | None = None
    for _ in range(retries + 1):
        try_numbers[task.task_id] += 1
        try:
            result = _run_single_attempt(
                dag=dag,
                task=task,
                logical_date=logical_date,
                run_id=run_id,
                run_conf=run_conf,
                xcom_store=xcom_store,
            )
            task_results[task.task_id] = result
            if task.do_xcom_push:
                _push_result(task, result, xcom_store)
            return TaskInstanceState.SUCCESS
        except AirflowSkipException:
            xcom_store.clear_task(task.task_id)
            return TaskInstanceState.SKIPPED
        except Exception as exc:
            xcom_store.clear_task(task.task_id)
            last_error = exc
            if try_numbers[task.task_id] <= retries:
                continue
            task_results[task.task_id] = last_error
            return TaskInstanceState.FAILED
    return TaskInstanceState.FAILED


def _resolve_retries(dag: DAG, task: Any) -> int:
    value = task.retries
    if value is None:
        value = dag.default_args.get("retries", 0)
    return int(value or 0)


def _run_single_attempt(
    *,
    dag: DAG,
    task: Any,
    logical_date: datetime,
    run_id: str,
    run_conf: dict[str, Any],
    xcom_store: InMemoryXComStore,
) -> Any:
    execution_task = copy.copy(task)
    data_interval_start, data_interval_end = _data_interval(dag, logical_date)
    context = _build_context(
        dag=dag,
        task=execution_task,
        logical_date=logical_date,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
        run_id=run_id,
        run_conf=run_conf,
        xcom_store=xcom_store,
    )
    _render_template_fields(execution_task, context)
    with set_current_context(context):
        return execution_task.execute(context)


def _data_interval(dag: DAG, logical_date: datetime) -> tuple[datetime, datetime]:
    if dag.schedule is not None:
        from easyflow._internal.schedules import interval_for_logical_date

        interval = interval_for_logical_date(dag.schedule, logical_date)
        return interval.data_interval_start, interval.data_interval_end
    return logical_date, logical_date


def _build_context(
    *,
    dag: DAG,
    task: Any,
    logical_date: datetime,
    data_interval_start: datetime,
    data_interval_end: datetime,
    run_id: str,
    run_conf: dict[str, Any],
    xcom_store: InMemoryXComStore,
) -> dict[str, Any]:
    params = dict(dag.params)
    params.update(task.params)
    params.update(run_conf)
    task_instance = TaskInstanceContext(dag.dag_id, task.task_id, run_id, xcom_store)
    return {
        "dag": dag,
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "dag_run": DagRunContext(dag.dag_id, run_id, run_conf, logical_date),
        "run_id": run_id,
        "logical_date": logical_date,
        "data_interval_start": data_interval_start,
        "data_interval_end": data_interval_end,
        "ds": logical_date.date().isoformat(),
        "ts": logical_date.isoformat(),
        "params": params,
        "task_instance_key_str": f"{dag.dag_id}__{task.task_id}__{run_id}",
        "_xcom_store": xcom_store,
    }


def _render_template_fields(task: Any, context: dict[str, Any]) -> None:
    env = Environment(undefined=StrictUndefined, autoescape=False)
    for field_name in getattr(task, "template_fields", ()):
        if not hasattr(task, field_name):
            continue
        try:
            rendered = _render_value(getattr(task, field_name), env, context)
        except TemplateError as exc:
            raise TemplateRenderError(
                f"模板字段 {task.task_id}.{field_name} 渲染失败: {exc}"
            ) from exc
        setattr(task, field_name, rendered)


def _render_value(value: Any, env: Environment, context: dict[str, Any]) -> Any:
    if isinstance(value, XComArg):
        return resolve_xcom_args(value, context["_xcom_store"])
    if isinstance(value, str):
        return env.from_string(value).render(context)
    if isinstance(value, list):
        return [_render_value(item, env, context) for item in value]
    if isinstance(value, tuple):
        return tuple(_render_value(item, env, context) for item in value)
    if isinstance(value, Mapping):
        return {key: _render_value(item, env, context) for key, item in value.items()}
    if isinstance(value, set):
        return {_render_value(item, env, context) for item in value}
    if isinstance(value, frozenset):
        return frozenset(_render_value(item, env, context) for item in value)
    return value


def _push_result(task: Any, result: Any, xcom_store: InMemoryXComStore) -> None:
    xcom_store.push(task.task_id, RETURN_VALUE_KEY, result)
    if task.multiple_outputs:
        if not isinstance(result, Mapping):
            raise XComSerializationError(
                "multiple_outputs=True requires the task return value to be a mapping."
            )
        for key, value in result.items():
            if not isinstance(key, str):
                raise XComSerializationError("multiple_outputs keys must be strings.")
            xcom_store.push(task.task_id, key, value)
