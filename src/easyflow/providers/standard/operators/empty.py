from __future__ import annotations

from typing import Any

from easyflow.sdk.operators.base import BaseOperator


class EmptyOperator(BaseOperator):
    def execute(self, context: dict[str, Any]) -> None:
        return None
