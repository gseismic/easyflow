from __future__ import annotations

from collections.abc import Iterable
from datetime import timedelta
from typing import Any, ClassVar

from easyflow.sdk._compat import OPERATOR_UNSUPPORTED_KWARGS, reject_kwargs
from easyflow.sdk.states import TriggerRule


def _as_operator(value: Any) -> "BaseOperator":
    from easyflow.sdk.xcom import XComArg

    if isinstance(value, XComArg):
        return value.operator
    if isinstance(value, BaseOperator):
        return value
    raise TypeError(f"Expected BaseOperator or XComArg, got {type(value).__name__}")


def _is_many(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(value, (str, bytes, dict))


class BaseOperator:
    template_fields: ClassVar[tuple[str, ...]] = ()

    def __init__(
        self,
        *,
        task_id: str,
        dag: Any | None = None,
        owner: str = "airflow",
        retries: int | None = None,
        retry_delay: timedelta | None = None,
        retry_exponential_backoff: bool | float = False,
        max_retry_delay: timedelta | None = None,
        trigger_rule: TriggerRule | str = TriggerRule.ALL_SUCCESS,
        depends_on_past: bool = False,
        params: dict[str, Any] | None = None,
        do_xcom_push: bool = True,
        multiple_outputs: bool = False,
        **kwargs: Any,
    ) -> None:
        reject_kwargs(type(self).__name__, kwargs, OPERATOR_UNSUPPORTED_KWARGS)
        self.task_id = task_id
        self.owner = owner
        self.retries = retries
        self.retry_delay = retry_delay
        self.retry_exponential_backoff = retry_exponential_backoff
        self.max_retry_delay = max_retry_delay
        self.trigger_rule = TriggerRule(trigger_rule)
        self.depends_on_past = depends_on_past
        self.params = dict(params or {})
        self.do_xcom_push = do_xcom_push
        self.multiple_outputs = multiple_outputs
        self._dag = None
        self.upstream_task_ids: set[str] = set()
        self.downstream_task_ids: set[str] = set()

        from easyflow.sdk.dag import current_dag

        context_dag = current_dag()
        if dag is not None and context_dag is not None and dag is not context_dag:
            raise ValueError("显式 DAG 与当前 DAG context 不一致。")
        if dag is None:
            dag = context_dag
        if dag is not None:
            dag.add_task(self)

    @property
    def dag(self) -> Any | None:
        return self._dag

    @property
    def dag_id(self) -> str | None:
        return None if self._dag is None else self._dag.dag_id

    def execute(self, context: dict[str, Any]) -> Any:
        raise NotImplementedError

    def bind_to_dag(self, dag: Any) -> None:
        if self._dag is not None and self._dag is not dag:
            raise ValueError(
                f"Task {self.task_id!r} 已绑定到 DAG {self._dag.dag_id!r}，不能再绑定到 {dag.dag_id!r}。"
            )
        self._dag = dag

    def set_downstream(self, other: Any) -> None:
        if _is_many(other):
            for item in other:
                self.set_downstream(item)
            return
        other_operator = _as_operator(other)
        self._ensure_same_dag_or_bind(other_operator)
        self.downstream_task_ids.add(other_operator.task_id)
        other_operator.upstream_task_ids.add(self.task_id)

    def set_upstream(self, other: Any) -> None:
        if _is_many(other):
            for item in other:
                self.set_upstream(item)
            return
        other_operator = _as_operator(other)
        other_operator.set_downstream(self)

    def _ensure_same_dag_or_bind(self, other: "BaseOperator") -> None:
        if self.dag is not None and other.dag is not None and self.dag is not other.dag:
            raise ValueError(
                f"不能在不同 DAG 的任务之间建立依赖：{self.dag_id} 与 {other.dag_id}。"
            )
        if self.dag is not None and other.dag is None:
            self.dag.add_task(other)
        elif self.dag is None and other.dag is not None:
            other.dag.add_task(self)

    def __rshift__(self, other: Any) -> Any:
        self.set_downstream(other)
        return other

    def __lshift__(self, other: Any) -> Any:
        self.set_upstream(other)
        return other

    def __rrshift__(self, other: Any) -> "BaseOperator":
        self.set_upstream(other)
        return self

    def __rlshift__(self, other: Any) -> "BaseOperator":
        self.set_downstream(other)
        return self

    def __repr__(self) -> str:
        return f"{type(self).__name__}(task_id={self.task_id!r})"


def chain(*tasks: Any) -> None:
    for left, right in zip(tasks, tasks[1:]):
        if _is_many(left):
            for item in left:
                _as_operator(item).set_downstream(right)
        else:
            _as_operator(left).set_downstream(right)
