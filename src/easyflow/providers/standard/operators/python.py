from __future__ import annotations

from typing import Any, Callable, Sequence

from easyflow.sdk.operators.base import BaseOperator


class PythonOperator(BaseOperator):
    template_fields = ("op_args", "op_kwargs")

    def __init__(
        self,
        *,
        python_callable: Callable[..., Any],
        op_args: Sequence[Any] | None = None,
        op_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.op_args = list(op_args or [])
        self.op_kwargs = dict(op_kwargs or {})
        self.set_xcom_dependencies()

    def execute(self, context: dict[str, Any]) -> Any:
        from easyflow.sdk.context import call_with_context

        return call_with_context(
            self.python_callable, self.op_args, self.op_kwargs, context
        )
