from __future__ import annotations

import copy
import re
from collections import deque
from collections.abc import Collection
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator

from easyflow.sdk._compat import (
    DAG_UNSUPPORTED_KWARGS,
    OPERATOR_DEFAULT_ARG_KEYS,
    OPERATOR_UNSUPPORTED_KWARGS,
    reject_kwargs,
)
from easyflow.sdk.exceptions import (
    DagCycleError,
    DagDefinitionError,
    DuplicateTaskIdError,
    TaskBindingError,
)
from easyflow.sdk.states import TaskInstanceState

_DAG_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_DAG_STACK: list[DAG] = []
_REGISTERED_DAGS: ContextVar[list[DAG] | None] = ContextVar(
    "easyflow_registered_dags", default=None
)


def current_dag() -> "DAG | None":
    return _DAG_STACK[-1] if _DAG_STACK else None


@contextmanager
def collect_registered_dags() -> Iterator[list[DAG]]:
    dags: list[DAG] = []
    token = _REGISTERED_DAGS.set(dags)
    try:
        yield dags
    finally:
        _REGISTERED_DAGS.reset(token)


@dataclass(frozen=True)
class DagTestResult:
    dag_id: str
    run_id: str
    logical_date: datetime
    task_states: dict[str, TaskInstanceState]
    task_results: dict[str, Any]
    try_numbers: dict[str, int]
    leaf_task_ids: frozenset[str] | None = None

    @property
    def success(self) -> bool:
        leaves = (
            self.leaf_task_ids
            if self.leaf_task_ids is not None
            else self.task_states.keys()
        )
        return all(
            self.task_states[task_id]
            in {TaskInstanceState.SUCCESS, TaskInstanceState.SKIPPED}
            for task_id in leaves
        )


class DAG:
    def __init__(
        self,
        dag_id: str,
        *,
        description: str | None = None,
        schedule: str | timedelta | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        catchup: bool | None = None,
        default_args: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        tags: Collection[str] | None = None,
        max_active_tasks: int | None = None,
        max_active_runs: int | None = None,
        auto_register: bool = True,
        **kwargs: Any,
    ) -> None:
        reject_kwargs("DAG", kwargs, DAG_UNSUPPORTED_KWARGS)
        self.dag_id = dag_id
        self.description = description
        self.schedule = schedule
        self.start_date = start_date
        self.end_date = end_date
        self.catchup = catchup
        self.default_args = copy.deepcopy(default_args or {})
        reject_kwargs(
            "DAG.default_args",
            {
                key: value
                for key, value in self.default_args.items()
                if key not in OPERATOR_DEFAULT_ARG_KEYS
            },
            OPERATOR_UNSUPPORTED_KWARGS,
        )
        self.params = dict(params or {})
        self.tags = tuple(tags or ())
        self.max_active_tasks = max_active_tasks
        self.max_active_runs = max_active_runs
        self.auto_register = auto_register
        self.task_dict: dict[str, Any] = {}

        self._validate_init()

    @property
    def tasks(self) -> list[Any]:
        return list(self.task_dict.values())

    def _validate_init(self) -> None:
        if not _DAG_ID_RE.match(self.dag_id):
            raise DagDefinitionError(
                f"dag_id {self.dag_id!r} 非法，只允许字母、数字、点、下划线和连字符。"
            )
        for field_name, value in (
            ("start_date", self.start_date),
            ("end_date", self.end_date),
        ):
            if value is not None and value.tzinfo is None:
                raise DagDefinitionError(f"{field_name} 必须是带时区 datetime。")
        if self.start_date is not None and self.end_date is not None:
            if self.start_date.astimezone(timezone.utc) >= self.end_date.astimezone(
                timezone.utc
            ):
                raise DagDefinitionError("start_date 必须小于 end_date。")
        for name, value in (
            ("max_active_runs", self.max_active_runs),
            ("max_active_tasks", self.max_active_tasks),
        ):
            if value is not None and value < 1:
                raise DagDefinitionError(f"{name} 必须大于 0。")

    def __enter__(self) -> "DAG":
        _DAG_STACK.append(self)
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        popped = _DAG_STACK.pop()
        if popped is not self:
            raise RuntimeError("DAG context stack corrupted")
        registry = _REGISTERED_DAGS.get()
        if exc_type is None and self.auto_register and registry is not None:
            registry.append(self)

    def add_task(self, task: Any) -> None:
        if task.task_id in self.task_dict:
            if self.task_dict[task.task_id] is task:
                return
            raise DuplicateTaskIdError(
                f"DAG {self.dag_id!r} 中已存在 task_id {task.task_id!r}。"
            )
        if task.dag is not None and task.dag is not self:
            raise TaskBindingError(
                f"Task {task.task_id!r} 已绑定到 DAG {task.dag.dag_id!r}。"
            )
        task.bind_to_dag(self)
        self.task_dict[task.task_id] = task

    def get_task(self, task_id: str) -> Any:
        return self.task_dict[task_id]

    def validate(self) -> None:
        for task in self.tasks:
            task.set_xcom_dependencies()
            if task.dag is not self:
                raise TaskBindingError(
                    f"Task {task.task_id!r} 未绑定到 DAG {self.dag_id!r}。"
                )
            for upstream in task.upstream_task_ids:
                if upstream not in self.task_dict:
                    raise TaskBindingError(
                        f"Task {task.task_id!r} 引用了不存在的上游任务 {upstream!r}。"
                    )
        self.topological_sort()

    def topological_sort(self) -> list[Any]:
        indegree = {task_id: 0 for task_id in self.task_dict}
        downstreams = {task_id: set() for task_id in self.task_dict}
        for task in self.tasks:
            for upstream in task.upstream_task_ids:
                indegree[task.task_id] += 1
                downstreams[upstream].add(task.task_id)

        ready = deque(
            sorted(task_id for task_id, degree in indegree.items() if degree == 0)
        )
        ordered_ids: list[str] = []
        while ready:
            task_id = ready.popleft()
            ordered_ids.append(task_id)
            for downstream in sorted(downstreams[task_id]):
                indegree[downstream] -= 1
                if indegree[downstream] == 0:
                    ready.append(downstream)

        if len(ordered_ids) != len(self.task_dict):
            raise DagCycleError(f"DAG {self.dag_id!r} 存在环。")
        return [self.task_dict[task_id] for task_id in ordered_ids]

    def next_task_id(self, base_task_id: str) -> str:
        if base_task_id not in self.task_dict:
            return base_task_id
        index = 1
        while f"{base_task_id}__{index}" in self.task_dict:
            index += 1
        return f"{base_task_id}__{index}"

    def test(
        self,
        logical_date: datetime | None = None,
        run_conf: dict[str, Any] | None = None,
        use_executor: bool = False,
    ) -> DagTestResult:
        if use_executor:
            raise NotImplementedError(
                "dag.test(use_executor=True) 将在后续执行器里程碑实现。"
            )
        from easyflow.sdk.testing import run_dag_in_process

        logical_date = logical_date or datetime.now(timezone.utc)
        if logical_date.tzinfo is None:
            raise DagDefinitionError("logical_date 必须是带时区 datetime。")
        self.validate()
        return run_dag_in_process(
            self, logical_date=logical_date, run_conf=run_conf or {}
        )


def dag(
    _func: Callable[..., Any] | None = None,
    *,
    dag_id: str | None = None,
    **dag_kwargs: Any,
) -> Callable[..., DAG] | DAG:
    def decorate(func: Callable[..., Any]) -> Callable[..., DAG]:
        def factory(*args: Any, **kwargs: Any) -> DAG:
            built = DAG(dag_id or func.__name__, **dag_kwargs)
            with built:
                func(*args, **kwargs)
            return built

        factory.__name__ = func.__name__
        factory.__doc__ = func.__doc__
        return factory

    if _func is not None:
        return decorate(_func)
    return decorate
