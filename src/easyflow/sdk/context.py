from __future__ import annotations

import inspect
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterator

from easyflow.sdk.exceptions import ContextNotAvailableError

Context = dict[str, Any]


@dataclass(frozen=True)
class DagRunContext:
    dag_id: str
    run_id: str
    conf: dict[str, Any]
    logical_date: datetime

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


@dataclass(frozen=True)
class TaskInstanceContext:
    dag_id: str
    task_id: str
    run_id: str
    _xcom_store: Any

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def xcom_push(self, key: str, value: Any) -> None:
        self._xcom_store.push(self.task_id, key, value)

    def xcom_pull(
        self,
        task_ids: str | list[str] | tuple[str, ...],
        key: str = "return_value",
        default: Any = None,
    ) -> Any:
        def pull(task_id: str) -> Any:
            try:
                return self._xcom_store.pull(task_id, key)
            except KeyError:
                return default

        if isinstance(task_ids, str):
            return pull(task_ids)
        return [pull(task_id) for task_id in task_ids]


def call_with_context(
    func: Callable[..., Any],
    args: list[Any],
    kwargs: dict[str, Any],
    context: Context,
) -> Any:
    signature = inspect.signature(func)
    bound = signature.bind_partial(*args, **kwargs)
    supplied = dict(kwargs)
    accepts_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    for name, value in context.items():
        if name.startswith("_") or name in bound.arguments or name in supplied:
            continue
        parameter = signature.parameters.get(name)
        if (
            parameter is not None
            and parameter.kind == inspect.Parameter.POSITIONAL_ONLY
        ):
            continue
        if accepts_kwargs or (
            parameter is not None
            and parameter.kind
            in {inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY}
        ):
            supplied[name] = value
    return func(*args, **supplied)


_CURRENT_CONTEXT: ContextVar[Context | None] = ContextVar(
    "easyflow_current_context",
    default=None,
)


def get_current_context() -> Context:
    context = _CURRENT_CONTEXT.get()
    if context is None:
        raise ContextNotAvailableError(
            "get_current_context() 只能在 Task 执行期间调用。"
        )
    return context


@contextmanager
def set_current_context(context: Context) -> Iterator[None]:
    token = _CURRENT_CONTEXT.set(context)
    try:
        yield
    finally:
        _CURRENT_CONTEXT.reset(token)
