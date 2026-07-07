from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

from easyflow.sdk.exceptions import ContextNotAvailableError

Context = dict[str, Any]

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
