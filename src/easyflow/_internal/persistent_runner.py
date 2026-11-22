from __future__ import annotations

import copy
import hashlib
import io
import json
import os
import time
import traceback
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from easyflow._internal.dag_parser import definition_hash_for_dag
from easyflow._internal.schedules import DataInterval
from easyflow._internal.state_store import (
    SchedulerFence,
    SQLiteStateStore,
    TaskCapacityFull,
    DagPaused,
    _json_dumps_xcom,
    run_timezone,
    task_retry_settings,
)
from easyflow._internal.task_heartbeat import TaskHeartbeat
from easyflow.sdk.context import set_current_context
from easyflow.sdk.dag import DAG
from easyflow.sdk.exceptions import AirflowSkipException
from easyflow.sdk.states import DagRunState, TaskInstanceState
from easyflow.sdk.testing import (
    TERMINAL_STATES,
    _build_context,
    _data_interval,
    _dependency_decision,
    _push_result,
    _render_template_fields,
    _resolve_retries,
)


@dataclass(frozen=True)
class PersistentRunResult:
    dag_id: str
    run_id: str
    state: DagRunState
    task_states: dict[str, TaskInstanceState | None]


class ExecutorName(str, Enum):
    SEQUENTIAL = "sequential"
    LOCAL = "local"


def validate_executor(executor: str) -> None:
    if executor not in {ExecutorName.SEQUENTIAL, ExecutorName.LOCAL}:
        raise ValueError(f"未知 executor: {executor}；可选值为 sequential、local。")


class PersistentXComStore:
    def __init__(self, store: SQLiteStateStore, dag_id: str, run_id: str) -> None:
        self.store = store
        self.dag_id = dag_id
        self.run_id = run_id
        self.pending: dict[tuple[str, str], str] = {}

    def push(self, task_id: str, key: str, value: Any) -> None:
        self.pending[(task_id, key)] = _json_dumps_xcom(value)

    def pull(self, task_id: str, key: str = "return_value") -> Any:
        if (task_id, key) in self.pending:
            return json.loads(self.pending[(task_id, key)])
        return self.store.pull_xcom(self.dag_id, self.run_id, task_id, key)

    def values_for_task(self, task_id: str) -> dict[str, Any]:
        return {
            key: json.loads(value)
            for (stored_task_id, key), value in self.pending.items()
            if stored_task_id == task_id
        }

    def clear_task(self, task_id: str) -> None:
        for key in [key for key in self.pending if key[0] == task_id]:
            self.pending.pop(key, None)


class PersistentDagRunner:
    def __init__(
        self,
        store: SQLiteStateStore,
        *,
        log_root: str | Path = "./.easyflow/logs",
        task_lease_seconds: int = 30,
        should_continue: Callable[[], bool] | None = None,
        scheduler_fence: SchedulerFence | None = None,
    ) -> None:
        self.store = store
        self.log_root = Path(log_root).resolve()
        self.task_lease_seconds = task_lease_seconds
        self.runner_id = f"runner-{os.getpid()}"
        self.should_continue = should_continue or (lambda: True)
        self.scheduler_fence = scheduler_fence

    def create_manual_run(
        self,
        dag: DAG,
        *,
        file_path: str | Path | None,
        logical_date: datetime,
        run_id: str | None = None,
        conf: dict[str, Any] | None = None,
    ) -> str:
        if logical_date.tzinfo is None:
            raise ValueError("logical_date 必须带时区。")
        run_id = (
            run_id or f"manual__{logical_date.astimezone(timezone.utc).isoformat()}"
        )
        interval_start, interval_end = _data_interval(dag, logical_date)
        return self.create_run(
            dag,
            file_path=file_path,
            run_id=run_id,
            run_type="manual",
            interval=DataInterval(logical_date, interval_start, interval_end),
            conf=conf or {},
        )

    def create_scheduled_run(
        self,
        dag: DAG,
        *,
        file_path: str | Path | None,
        interval: DataInterval,
    ) -> str:
        run_id = (
            f"scheduled__{interval.logical_date.astimezone(timezone.utc).isoformat()}"
        )
        return self.create_run(
            dag,
            file_path=file_path,
            run_id=run_id,
            run_type="scheduled",
            interval=interval,
            conf={},
        )

    def create_backfill_run(
        self,
        dag: DAG,
        *,
        file_path: str | Path | None,
        interval: DataInterval,
        backfill_id: str,
        max_active_runs: int | None = None,
        reprocessing_behavior: str | None = None,
    ) -> str:
        safe_logical_date = interval.logical_date.astimezone(timezone.utc).isoformat()
        run_id = f"backfill__{backfill_id}__{safe_logical_date}"
        return self.create_run(
            dag,
            file_path=file_path,
            run_id=run_id,
            run_type="backfill",
            interval=interval,
            conf={},
            backfill_id=backfill_id,
            max_active_runs=max_active_runs,
            reprocessing_behavior=reprocessing_behavior,
        )

    def create_run(
        self,
        dag: DAG,
        *,
        file_path: str | Path | None,
        run_id: str,
        run_type: str,
        interval: DataInterval,
        conf: dict[str, Any] | None = None,
        backfill_id: str | None = None,
        max_active_runs: int | None = None,
        reprocessing_behavior: str | None = None,
    ) -> str:
        loaded_source = getattr(dag, "_easyflow_source_sha256", None)
        if file_path is not None and loaded_source is not None:
            actual_source = hashlib.sha256(Path(file_path).read_bytes()).hexdigest()
            if actual_source != loaded_source:
                raise RuntimeError("创建 DagRun 前 DAG 源码已改变，请重新加载。")
        self.store.create_dag_run(
            dag=dag,
            run_id=run_id,
            run_type=run_type,
            logical_date=interval.logical_date,
            data_interval_start=interval.data_interval_start,
            data_interval_end=interval.data_interval_end,
            definition_hash=definition_hash_for_dag(
                dag, file_path, source_sha256=loaded_source
            ),
            file_path=str(file_path) if file_path is not None else None,
            conf=conf or {},
            backfill_id=backfill_id,
            max_active_runs=max_active_runs,
            only_once=run_type == "scheduled" and dag.schedule == "@once",
            scheduler_fence=self.scheduler_fence,
            reprocessing_behavior=reprocessing_behavior,
        )
        return run_id

    def run_existing(
        self,
        dag: DAG,
        *,
        run_id: str,
        file_path: str | Path | None = None,
        executor: str = "sequential",
        parallelism: int = 1,
        wait_for_retry: bool = True,
        wait_for_completion: bool = False,
    ) -> PersistentRunResult:
        """同步入口可等待同一次运行，扫描入口遇到锁争用时直接返回当前状态。"""
        validate_executor(executor)
        self.store.configure_parallelism(parallelism)
        while True:
            result = None
            with self.store.run_lock(dag.dag_id, run_id) as acquired:
                if acquired:
                    result = self._run_existing_locked(
                        dag,
                        run_id=run_id,
                        file_path=file_path,
                        executor=executor,
                        parallelism=parallelism,
                        wait_for_retry=wait_for_retry,
                    )
            if result is None:
                record = self.store.get_dag_run(dag.dag_id, run_id)
                if record is None:
                    raise ValueError(f"DagRun 不存在: {dag.dag_id}/{run_id}")
                state = DagRunState(record.state)
            else:
                state = result.state
            if (
                not wait_for_completion
                or state in {DagRunState.SUCCESS, DagRunState.FAILED}
                or not self.should_continue()
                or self.store.is_dag_paused(dag.dag_id)
            ):
                if result is not None:
                    return result
                return PersistentRunResult(
                    dag.dag_id,
                    run_id,
                    state,
                    self.store.list_task_states(dag.dag_id, run_id),
                )
            time.sleep(0.05)

    def _run_existing_locked(
        self,
        dag: DAG,
        *,
        run_id: str,
        file_path: str | Path | None = None,
        executor: str = "sequential",
        parallelism: int = 1,
        wait_for_retry: bool = True,
    ) -> PersistentRunResult:
        record = self.store.get_dag_run(dag.dag_id, run_id)
        if record is None:
            raise ValueError(f"DagRun 不存在: {dag.dag_id}/{run_id}")
        if record.state in {DagRunState.SUCCESS.value, DagRunState.FAILED.value}:
            return PersistentRunResult(
                dag_id=dag.dag_id,
                run_id=run_id,
                state=DagRunState(record.state),
                task_states=self.store.list_task_states(dag.dag_id, run_id),
            )
        if self.store.is_dag_paused(dag.dag_id):
            return PersistentRunResult(
                dag.dag_id,
                run_id,
                DagRunState(record.state),
                self.store.list_task_states(dag.dag_id, run_id),
            )
        if getattr(dag, "_easyflow_snapshot", False) and executor == "sequential":
            executor = "local"
            parallelism = 1
        if file_path is not None:
            current_hash = definition_hash_for_dag(dag, file_path)
            loaded_source = getattr(dag, "_easyflow_source_sha256", None)
            loaded_hash = definition_hash_for_dag(
                dag, file_path, source_sha256=loaded_source
            )
            if record.definition_hash is not None and (
                record.definition_hash != current_hash
                or record.definition_hash != loaded_hash
            ):
                failed = self.store.fail_dag_run_definition_changed(
                    dag_id=dag.dag_id,
                    run_id=run_id,
                    expected_hash=record.definition_hash,
                    actual_hash=(
                        current_hash
                        if current_hash != record.definition_hash
                        else loaded_hash
                    ),
                )
                return PersistentRunResult(
                    dag_id=dag.dag_id,
                    run_id=run_id,
                    state=DagRunState.FAILED if failed else DagRunState(record.state),
                    task_states=self.store.list_task_states(dag.dag_id, run_id),
                )
        zone = run_timezone(record.logical_timezone)
        logical_date = datetime.fromisoformat(record.logical_date).astimezone(zone)
        conf = {}
        if record.conf_json:
            conf = json.loads(record.conf_json)
        if executor == "local":
            if file_path is None:
                raise ValueError(
                    "LocalExecutor 需要 file_path 以便子进程重新加载 DAG。"
                )
            return self._run_existing_local(
                dag=dag,
                run_id=run_id,
                file_path=file_path,
                record=record,
                logical_date=logical_date,
                conf=conf,
                parallelism=parallelism,
                wait_for_retry=wait_for_retry,
            )
        self.store.recover_orphaned_tasks_for_run(dag=dag, run_id=run_id)
        self.store.set_dag_run_state(dag.dag_id, run_id, DagRunState.RUNNING)
        xcom_store = PersistentXComStore(self.store, dag.dag_id, run_id)
        ordered_tasks = dag.topological_sort()

        while True:
            task_states = self.store.list_task_states(dag.dag_id, run_id)
            if all(state in TERMINAL_STATES for state in task_states.values()):
                break
            if not self.should_continue():
                return PersistentRunResult(
                    dag.dag_id, run_id, DagRunState.RUNNING, task_states
                )
            progressed = False
            ready_tasks = []
            for task in ordered_tasks:
                if self.store.is_dag_paused(dag.dag_id):
                    break
                current_state = task_states[task.task_id]
                if current_state in TERMINAL_STATES:
                    continue
                if not self.should_continue():
                    break
                if current_state == TaskInstanceState.RUNNING:
                    continue
                past_decision = self._depends_on_past_decision(
                    dag_id=dag.dag_id,
                    run_id=run_id,
                    run_type=record.run_type,
                    task=task,
                    logical_date=logical_date,
                )
                if past_decision == "wait":
                    continue
                if past_decision == "upstream_failed":
                    self.store.mark_task_terminal(
                        dag.dag_id,
                        run_id,
                        task.task_id,
                        TaskInstanceState.UPSTREAM_FAILED,
                    )
                    progressed = True
                    continue
                decision = _dependency_decision(task, task_states)
                if decision == "wait":
                    continue
                if decision in {"upstream_failed", "skipped"}:
                    terminal_state = (
                        TaskInstanceState.UPSTREAM_FAILED
                        if decision == "upstream_failed"
                        else TaskInstanceState.SKIPPED
                    )
                    self.store.mark_task_terminal(
                        dag.dag_id,
                        run_id,
                        task.task_id,
                        terminal_state,
                    )
                    progressed = True
                    continue
                try:
                    self.store.queue_task(
                        dag.dag_id,
                        run_id,
                        task.task_id,
                        scheduler_fence=self.scheduler_fence,
                    )
                except DagPaused:
                    break
                ready_tasks.append(task)
            for task in ready_tasks:
                self._run_task_until_terminal(
                    dag=dag,
                    task=task,
                    run_id=run_id,
                    logical_date=logical_date,
                    conf=conf,
                    xcom_store=xcom_store,
                )
                progressed = True
            if not progressed:
                return PersistentRunResult(
                    dag.dag_id,
                    run_id,
                    DagRunState.RUNNING,
                    self.store.list_task_states(dag.dag_id, run_id),
                )

        final_states = self.store.list_task_states(dag.dag_id, run_id)
        final_state = self.store.finish_dag_run(dag, run_id)
        final_states = self.store.list_task_states(dag.dag_id, run_id)
        return PersistentRunResult(dag.dag_id, run_id, final_state, final_states)

    def _run_existing_local(
        self,
        *,
        dag: DAG,
        run_id: str,
        file_path: str | Path,
        record: Any,
        logical_date: datetime,
        conf: dict[str, Any],
        parallelism: int,
        wait_for_retry: bool,
    ) -> PersistentRunResult:
        from easyflow._internal.local_executor import LocalExecutor

        self.store.recover_orphaned_tasks_for_run(dag=dag, run_id=run_id)
        self.store.set_dag_run_state(dag.dag_id, run_id, DagRunState.RUNNING)
        max_active_tasks = dag.max_active_tasks or parallelism
        executor = LocalExecutor(
            store=self.store,
            dag=dag,
            run_id=run_id,
            file_path=file_path,
            log_root=self.log_root,
            task_lease_seconds=self.task_lease_seconds,
            parallelism=parallelism,
            max_active_tasks=max_active_tasks,
            depends_on_past_decision=self._depends_on_past_decision,
            run_type=record.run_type,
            logical_date=logical_date,
            conf=conf,
            should_continue=self.should_continue,
            scheduler_fence=self.scheduler_fence,
            wait_for_retry=wait_for_retry,
        )
        final_states = executor.run()
        record_now = self.store.get_dag_run(dag.dag_id, run_id)
        if record_now is not None and record_now.state == DagRunState.FAILED.value:
            return PersistentRunResult(
                dag.dag_id, run_id, DagRunState.FAILED, final_states
            )
        final_state = self.store.finish_dag_run(dag, run_id)
        final_states = self.store.list_task_states(dag.dag_id, run_id)
        return PersistentRunResult(dag.dag_id, run_id, final_state, final_states)

    def _depends_on_past_decision(
        self,
        *,
        dag_id: str,
        run_id: str,
        run_type: str,
        task: Any,
        logical_date: datetime,
    ) -> str:
        if not task.depends_on_past:
            return "ready"
        if run_type not in {"scheduled", "backfill"}:
            return "ready"
        previous = self.store.previous_scheduled_or_backfill_task_state(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task.task_id,
            logical_date=logical_date,
        )
        if not previous.exists:
            return "ready"
        if previous.state in {TaskInstanceState.SUCCESS, TaskInstanceState.SKIPPED}:
            return "ready"
        if previous.state in {
            TaskInstanceState.FAILED,
            TaskInstanceState.UPSTREAM_FAILED,
        }:
            return "upstream_failed"
        return "wait"

    def _run_task_until_terminal(
        self,
        *,
        dag: DAG,
        task: Any,
        run_id: str,
        logical_date: datetime,
        conf: dict[str, Any],
        xcom_store: PersistentXComStore,
        single_attempt: bool = False,
    ) -> None:
        retries = _resolve_retries(dag, task)
        delay, factor, maximum = task_retry_settings(dag, task)
        while self.should_continue():
            current_state = self.store.get_task_state(dag.dag_id, run_id, task.task_id)
            if current_state in TERMINAL_STATES:
                return
            retry_at = self.store.task_retry_at(dag.dag_id, run_id, task.task_id)
            if retry_at is not None:
                remaining = (retry_at - datetime.now(timezone.utc)).total_seconds()
                if remaining > 0:
                    if single_attempt:
                        return
                    time.sleep(min(remaining, 0.2))
                    continue
            try:
                if not self.store.queue_task(
                    dag.dag_id,
                    run_id,
                    task.task_id,
                    scheduler_fence=self.scheduler_fence,
                ):
                    return
                try_number = self.store.start_task_try(
                    dag.dag_id,
                    run_id,
                    task.task_id,
                    owner=self.runner_id,
                    lease_seconds=self.task_lease_seconds,
                    scheduler_fence=self.scheduler_fence,
                    max_active_tasks=dag.max_active_tasks,
                )
            except TaskCapacityFull:
                time.sleep(0.05)
                continue
            except DagPaused:
                if single_attempt:
                    raise
                return
            if try_number is None:
                return
            heartbeat = TaskHeartbeat(
                store=self.store,
                dag_id=dag.dag_id,
                run_id=run_id,
                task_id=task.task_id,
                try_number=try_number,
                owner=self.runner_id,
                lease_seconds=self.task_lease_seconds,
            )
            log_path = self._log_path(dag.dag_id, run_id, task.task_id, try_number)
            try:
                self.store.set_task_try_log_path(
                    dag.dag_id,
                    run_id,
                    task.task_id,
                    try_number,
                    str(log_path),
                )
                heartbeat.start()
                result = self._run_single_attempt(
                    dag=dag,
                    task=task,
                    run_id=run_id,
                    logical_date=logical_date,
                    conf=conf,
                    xcom_store=xcom_store,
                    log_path=log_path,
                )
                if task.do_xcom_push:
                    _push_result(task, result, xcom_store)
                self.store.commit_task_terminal(
                    dag_id=dag.dag_id,
                    run_id=run_id,
                    task_id=task.task_id,
                    try_number=try_number,
                    xcom_values=xcom_store.values_for_task(task.task_id),
                )
                xcom_store.clear_task(task.task_id)
                return
            except AirflowSkipException:
                xcom_store.clear_task(task.task_id)
                self.store.commit_task_terminal(
                    dag_id=dag.dag_id,
                    run_id=run_id,
                    task_id=task.task_id,
                    try_number=try_number,
                    xcom_values={},
                    state=TaskInstanceState.SKIPPED,
                )
                return
            except (Exception, SystemExit) as exc:
                try:
                    log_path.parent.mkdir(parents=True, exist_ok=True)
                    with log_path.open("a", encoding="utf-8") as log_file:
                        log_file.write("".join(traceback.format_exception(exc)))
                except OSError:
                    # 日志目录不可写时仍需结束 task_try，避免任务永久停在 running。
                    pass
                xcom_store.clear_task(task.task_id)
                next_state = self.store.commit_failure(
                    dag_id=dag.dag_id,
                    run_id=run_id,
                    task_id=task.task_id,
                    try_number=try_number,
                    error=exc,
                    retries=retries,
                    retry_delay=delay,
                    retry_factor=factor,
                    max_retry_delay=maximum,
                )
                if next_state == TaskInstanceState.FAILED or single_attempt:
                    return
            finally:
                heartbeat.stop()

    def _run_single_attempt(
        self,
        *,
        dag: DAG,
        task: Any,
        run_id: str,
        logical_date: datetime,
        conf: dict[str, Any],
        xcom_store: PersistentXComStore,
        log_path: Path,
    ) -> Any:
        execution_task = copy.copy(task)
        record = self.store.get_dag_run(dag.dag_id, run_id)
        if record is None:
            raise ValueError(f"DagRun 不存在: {dag.dag_id}/{run_id}")
        zone = run_timezone(record.logical_timezone)
        interval_start = datetime.fromisoformat(record.data_interval_start).astimezone(
            zone
        )
        interval_end = datetime.fromisoformat(record.data_interval_end).astimezone(zone)
        context = _build_context(
            dag=dag,
            task=execution_task,
            logical_date=logical_date,
            data_interval_start=interval_start,
            data_interval_end=interval_end,
            run_id=run_id,
            run_conf=conf,
            xcom_store=xcom_store,
        )
        _render_template_fields(execution_task, context)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as log_file:
            with redirect_stdout(_Tee(sys_stdout=True, file=log_file)):
                with redirect_stderr(_Tee(sys_stdout=False, file=log_file)):
                    with set_current_context(context):
                        return execution_task.execute(context)

    def _log_path(
        self, dag_id: str, run_id: str, task_id: str, try_number: int
    ) -> Path:
        return (
            self.log_root
            / _log_component("dag", dag_id)
            / _log_component("run", run_id)
            / _log_component("task", task_id)
            / "-1"
            / f"{try_number}.log"
        )


def _log_component(kind: str, value: str) -> str:
    raw = value.encode("utf-8")
    encoded = raw.hex()
    if len(encoded) <= 200:
        return f"{kind}_{encoded}"
    return f"{kind}-hash-{hashlib.sha256(raw).hexdigest()}"


class _Tee(io.TextIOBase):
    def __init__(self, *, sys_stdout: bool, file: Any) -> None:
        import sys

        self.stream = sys.stdout if sys_stdout else sys.stderr
        self.file = file

    def write(self, data: str) -> int:
        self.file.write(data)
        self.file.flush()
        return self.stream.write(data)

    def flush(self) -> None:
        self.file.flush()
        self.stream.flush()
