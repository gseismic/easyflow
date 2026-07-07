from __future__ import annotations

from dataclasses import dataclass
from typing import Any


RETURN_VALUE_KEY = "return_value"


@dataclass(frozen=True)
class XComArg:
    operator: Any
    key: str = RETURN_VALUE_KEY

    def __getitem__(self, key: str) -> "XComArg":
        if not isinstance(key, str):
            raise TypeError("XComArg key must be a string")
        return XComArg(self.operator, key)

    def set_upstream(self, other: Any) -> None:
        self.operator.set_upstream(other)

    def set_downstream(self, other: Any) -> None:
        self.operator.set_downstream(other)

    def __rshift__(self, other: Any) -> Any:
        self.operator.__rshift__(other)
        return other

    def __lshift__(self, other: Any) -> Any:
        self.operator.__lshift__(other)
        return other

    def __rrshift__(self, other: Any) -> "XComArg":
        self.operator.__rrshift__(other)
        return self

    def __rlshift__(self, other: Any) -> "XComArg":
        self.operator.__rlshift__(other)
        return self
