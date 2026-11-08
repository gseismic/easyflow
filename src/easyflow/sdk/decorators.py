from __future__ import annotations

import inspect
from dataclasses import dataclass, replace
from typing import Any, Callable

from easyflow.providers.standard.operators.python import PythonOperator
from easyflow.sdk.dag import current_dag
from easyflow.sdk.xcom import XComArg


class TaskFlowPythonOperator(PythonOperator):
    pass


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
