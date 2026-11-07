from __future__ import annotations

import inspect
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any, Callable

from easyflow.providers.standard.operators.python import PythonOperator
from easyflow.sdk.dag import current_dag
from easyflow.sdk.xcom import XComArg


def _iter_xcom_args(value: Any) -> list[XComArg]:
    if isinstance(value, XComArg):
        return [value]
    if isinstance(value, Mapping):
        found: list[XComArg] = []
        for item in value.values():
            found.extend(_iter_xcom_args(item))
        return found
    if isinstance(value, (list, tuple, set, frozenset)):
        found = []
        for item in value:
            found.extend(_iter_xcom_args(item))
        return found
    return []


class TaskFlowPythonOperator(PythonOperator):
    def execute(self, context: dict[str, Any]) -> Any:
        from easyflow.sdk.testing import resolve_xcom_args

        xcom_store = context["_xcom_store"]
        args = resolve_xcom_args(self.op_args, xcom_store)
        kwargs = resolve_xcom_args(self.op_kwargs, xcom_store)
        return self.python_callable(*args, **kwargs)


@dataclass(frozen=True)
class TaskDecorator:
    python_callable: Callable[..., Any]
    operator_kwargs: dict[str, Any]

    def __call__(self, *args: Any, **kwargs: Any) -> XComArg:
        dag = self.operator_kwargs.get("dag") or current_dag()
        base_task_id = (
            self.operator_kwargs.get("task_id") or self.python_callable.__name__
        )
        task_id = dag.next_task_id(base_task_id) if dag is not None else base_task_id
        op_kwargs = dict(self.operator_kwargs)
        op_kwargs["task_id"] = task_id
        op_kwargs.pop("dag", None)
        operator = TaskFlowPythonOperator(
            python_callable=self.python_callable,
            op_args=list(args),
            op_kwargs=dict(kwargs),
            dag=dag,
            **op_kwargs,
        )
        for xcom_arg in _iter_xcom_args(args) + _iter_xcom_args(kwargs):
            xcom_arg.operator.set_downstream(operator)
        return XComArg(operator)

    def override(self, **kwargs: Any) -> "TaskDecorator":
        merged = dict(self.operator_kwargs)
        merged.update(kwargs)
        return replace(self, operator_kwargs=merged)


def task(
    _func: Callable[..., Any] | None = None,
    **operator_kwargs: Any,
) -> TaskDecorator | Callable[[Callable[..., Any]], TaskDecorator]:
    def decorate(func: Callable[..., Any]) -> TaskDecorator:
        if not callable(func):
            raise TypeError("@task 只能装饰可调用对象。")
        inspect.signature(func)
        return TaskDecorator(func, dict(operator_kwargs))

    if _func is not None:
        return decorate(_func)
    return decorate
