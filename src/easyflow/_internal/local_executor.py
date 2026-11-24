from __future__ import annotations

import multiprocessing
import os
import signal
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import IntEnum
from pathlib import Path
from typing import Any, Callable

from easyflow._internal.dag_parser import (
    PARSER_TIMEOUT_SECONDS,
    definition_hash_for_dag,
    open_dag_from_file,
)
from easyflow._internal.persistent_runner import (
    PersistentDagRunner,
    PersistentXComStore,
)
from easyflow._internal.state_store import (
    DagPaused,
    SchedulerFence,
    SchedulerLeaseLost,
    SQLiteStateStore,
    run_timezone,
    task_retry_settings,
)
from easyflow.sdk.states import TaskInstanceState
from easyflow.sdk.testing import TERMINAL_STATES, _dependency_decision, _resolve_retries


class _WorkerOutcome(IntEnum):
    LEASE_LOST = 1
    DAG_PAUSED = 2
    STARTUP_TIMEOUT = 3


class _WorkerReadiness(IntEnum):
    SESSION_CREATED = 1
    DEFINITION_LOADED = 2


@dataclass
class _Worker:
    process: multiprocessing.Process
    ready: Any
    outcome: Any
    startup_deadline: float
    previous_try_number: int


def _signal_worker_group(worker: _Worker, sig: int) -> bool:
    if os.name != "posix" or not worker.ready.value:
        return False
    try:
        os.killpg(worker.process.pid, sig)
    except ProcessLookupError:
        return False
    except PermissionError:
        # macOS 回收最后一个进程时可能返回 EPERM，仍存活的 worker 必须报告错误。
        if worker.process.is_alive():
            raise
        return False
    return True


def _stop_workers(workers: Iterable[_Worker], *, force: bool = False) -> None:
    workers = list(workers)
    if not force:
        for worker in workers:
            signaled = _signal_worker_group(worker, signal.SIGTERM)
            if not signaled and worker.process.is_alive():
                worker.process.terminate()
        grace_end = time.monotonic() + 0.2
        for worker in workers:
            worker.process.join(timeout=max(0, grace_end - time.monotonic()))
    ungrouped = []
    for worker in workers:
        if os.name == "posix" and _signal_worker_group(worker, signal.SIGKILL):
            continue
        ungrouped.append(worker)
        if worker.process.is_alive():
            worker.process.kill()
    kill_end = time.monotonic() + 1.0
    for worker in workers:
        worker.process.join(timeout=max(0, kill_end - time.monotonic()))
    if os.name == "posix":
        for worker in ungrouped:
            # 启动中的 worker 可能在收到直接终止信号前刚建立了会话。
            _signal_worker_group(worker, signal.SIGKILL)
    surviving = [worker.process.pid for worker in workers if worker.process.is_alive()]
    if surviving:
        raise RuntimeError(f"无法终止 worker，PID: {surviving}。")


class LocalExecutor:
    def __init__(
        self,
        *,
        store: SQLiteStateStore,
        dag: Any,
        run_id: str,
        file_path: str | Path,
        log_root: Path,
        task_lease_seconds: int,
        parallelism: int,
        max_active_tasks: int,
        depends_on_past_decision: Callable[..., str],
        run_type: str,
        logical_date: datetime,
        conf: dict[str, Any],
        should_continue: Callable[[], bool] | None = None,
        scheduler_fence: SchedulerFence | None = None,
        wait_for_retry: bool = True,
    ) -> None:
        self.store = store
        self.dag = dag
        self.run_id = run_id
        self.file_path = str(file_path)
        self.log_root = str(log_root)
        self.task_lease_seconds = task_lease_seconds
        self.parallelism = max(1, parallelism)
        self.max_active_tasks = max(1, max_active_tasks)
        self.depends_on_past_decision = depends_on_past_decision
        self.run_type = run_type
        self.logical_date = logical_date
        self.conf = conf
        self.should_continue = should_continue or (lambda: True)
        self.scheduler_fence = scheduler_fence
        self.wait_for_retry = wait_for_retry

    def run(self) -> dict[str, TaskInstanceState | None]:
        active: dict[str, _Worker] = {}
        try:
            return self._run(active)
        finally:
            _stop_workers(active.values())

    def _run(self, active: dict[str, _Worker]) -> dict[str, TaskInstanceState | None]:
        context = multiprocessing.get_context("spawn")
        ordered_tasks = self.dag.topological_sort()
        max_active = min(self.parallelism, self.max_active_tasks)

        while True:
            if not self.should_continue():
                return self.store.list_task_states(self.dag.dag_id, self.run_id)
            self._reap_workers(active)

            record = self.store.get_dag_run(self.dag.dag_id, self.run_id)
            if record is not None and record.state == "failed":
                return self.store.list_task_states(self.dag.dag_id, self.run_id)

            recovered = self.store.recover_orphaned_tasks_for_run(
                dag=self.dag, run_id=self.run_id
            )
            for task_id, attempt in recovered.items():
                worker = active.get(task_id)
                if (
                    worker is not None
                    and worker.previous_try_number + 1 == attempt.try_number
                    and worker.process.pid == attempt.worker_pid
                ):
                    _stop_workers([worker], force=True)
                    active.pop(task_id)
            task_states = self.store.list_task_states(self.dag.dag_id, self.run_id)
            if not active and all(
                state in TERMINAL_STATES for state in task_states.values()
            ):
                return task_states

            progressed = False
            waiting_retry = False
            if self.store.is_dag_paused(self.dag.dag_id):
                if active:
                    time.sleep(0.05)
                    continue
                return task_states
            existing_running = {
                task_id
                for task_id, state in task_states.items()
                if state == TaskInstanceState.RUNNING
            }
            ready_tasks = []
            for task in ordered_tasks:
                if task.task_id in active:
                    continue
                current_state = task_states[task.task_id]
                if current_state in TERMINAL_STATES:
                    continue
                if current_state == TaskInstanceState.RUNNING:
                    continue
                if current_state == TaskInstanceState.UP_FOR_RETRY:
                    retry_at = self.store.task_retry_at(
                        self.dag.dag_id, self.run_id, task.task_id
                    )
                    if retry_at is not None and retry_at > datetime.now(timezone.utc):
                        waiting_retry = True
                        continue
                past_decision = self.depends_on_past_decision(
                    dag_id=self.dag.dag_id,
                    run_id=self.run_id,
                    run_type=self.run_type,
                    task=task,
                    logical_date=self.logical_date,
                )
                if past_decision == "wait":
                    continue
                if past_decision == "upstream_failed":
                    self.store.mark_task_terminal(
                        self.dag.dag_id,
                        self.run_id,
                        task.task_id,
                        TaskInstanceState.UPSTREAM_FAILED,
                    )
                    progressed = True
                    continue
                decision = _dependency_decision(task, task_states)
                if decision == "wait":
                    continue
                if decision in {"upstream_failed", "skipped"}:
                    self.store.mark_task_terminal(
                        self.dag.dag_id,
                        self.run_id,
                        task.task_id,
                        TaskInstanceState.UPSTREAM_FAILED
                        if decision == "upstream_failed"
                        else TaskInstanceState.SKIPPED,
                    )
                    progressed = True
                    continue

                if current_state != TaskInstanceState.QUEUED:
                    try:
                        queued = self.store.queue_task(
                            self.dag.dag_id,
                            self.run_id,
                            task.task_id,
                            scheduler_fence=self.scheduler_fence,
                        )
                    except DagPaused:
                        break
                    if not queued:
                        continue
                ready_tasks.append(task)

            for task in ready_tasks:
                if len(set(active) | existing_running) >= max_active:
                    break
                # 启动监控读取单字节标志，避免等待被强制终止的子进程释放锁。
                ready = context.Value("b", 0, lock=False)
                # 仅在子进程退出后读取控制结果，任务退出码仍用于任务自身。
                outcome = context.Value("b", 0, lock=False)
                previous_try_number = self.store.task_try_number(
                    self.dag.dag_id, self.run_id, task.task_id
                )
                startup_deadline = time.monotonic() + PARSER_TIMEOUT_SECONDS
                process = context.Process(
                    target=run_local_task_worker,
                    args=(
                        self.store.database_url,
                        self.dag.dag_id,
                        self.file_path,
                        self.run_id,
                        task.task_id,
                        self.conf,
                        self.log_root,
                        self.task_lease_seconds,
                        self.scheduler_fence,
                        ready,
                        outcome,
                        startup_deadline,
                    ),
                )
                try:
                    process.start()
                except Exception as exc:
                    self._fail_startup(
                        task.task_id,
                        previous_try_number,
                        f"Worker 启动失败: {type(exc).__name__}: {exc}",
                    )
                    progressed = True
                    continue
                active[task.task_id] = _Worker(
                    process, ready, outcome, startup_deadline, previous_try_number
                )
                progressed = True

            if not progressed:
                if active:
                    time.sleep(0.05)
                    continue
                if waiting_retry and self.wait_for_retry:
                    time.sleep(0.05)
                    continue
                return task_states
            time.sleep(0.01)

    def _reap_workers(self, active: dict[str, _Worker]) -> None:
        for task_id, worker in list(active.items()):
            process = worker.process
            timed_out = False
            if process.is_alive():
                if (
                    time.monotonic() < worker.startup_deadline
                    or worker.ready.value == _WorkerReadiness.DEFINITION_LOADED
                ):
                    continue
                _stop_workers([worker], force=True)
                timed_out = True
            process.join(timeout=0)
            active.pop(task_id)
            if worker.outcome.value == _WorkerOutcome.LEASE_LOST:
                raise SchedulerLeaseLost("scheduler lease 已丢失，停止调度。")
            if worker.outcome.value == _WorkerOutcome.DAG_PAUSED:
                continue
            state = self.store.get_task_state(self.dag.dag_id, self.run_id, task_id)
            if not timed_out and (state == TaskInstanceState.RUNNING or process.exitcode != 0):
                # 异常退出的 worker 仍可能留下同组的子进程。
                _stop_workers([worker], force=True)
            if state == TaskInstanceState.RUNNING:
                task = self.dag.get_task(task_id)
                delay, factor, maximum = task_retry_settings(self.dag, task)
                self.store.recover_orphaned_task(
                    dag_id=self.dag.dag_id,
                    run_id=self.run_id,
                    task_id=task_id,
                    retries=_resolve_retries(self.dag, task),
                    retry_delay=delay,
                    retry_factor=factor,
                    max_retry_delay=maximum,
                    worker_exit=(process.pid, process.exitcode),
                    expected_try_number=worker.previous_try_number + 1,
                )
            elif state is None or state in {
                TaskInstanceState.QUEUED,
                TaskInstanceState.SCHEDULED,
                TaskInstanceState.UP_FOR_RETRY,
            }:
                if process.exitcode == 0 and state == TaskInstanceState.UP_FOR_RETRY:
                    continue
                message = (
                    "Worker 在领取任务前启动超时。"
                    if timed_out or worker.outcome.value == _WorkerOutcome.STARTUP_TIMEOUT
                    else f"Worker 在领取任务前退出，exitcode={process.exitcode}。"
                )
                self._fail_startup(task_id, worker.previous_try_number, message)

    def _fail_startup(self, task_id: str, previous_try_number: int, message: str) -> None:
        record = self.store.get_dag_run(self.dag.dag_id, self.run_id)
        if record is None or record.state not in {"queued", "running"}:
            return
        current_try_number = self.store.task_try_number(self.dag.dag_id, self.run_id, task_id)
        if current_try_number != previous_try_number:
            return
        runner = PersistentDagRunner(self.store, log_root=self.log_root)
        log_path = runner._log_path(self.dag.dag_id, self.run_id, task_id, 0)
        log_path = log_path.with_name(f"startup-{uuid.uuid4().hex}.log")
        stored_path = None
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text(message + "\n", encoding="utf-8")
            stored_path = str(log_path)
        except OSError:
            pass
        self.store.fail_worker_startup(
            self.dag.dag_id,
            self.run_id,
            task_id,
            message=message,
            log_path=stored_path,
            previous_try_number=previous_try_number,
        )


def run_local_task_worker(
    database_url: str,
    dag_id: str,
    file_path: str,
    run_id: str,
    task_id: str,
    conf: dict[str, Any],
    log_root: str,
    task_lease_seconds: int,
    scheduler_fence: SchedulerFence | None,
    ready: Any,
    outcome: Any,
    startup_deadline: float,
) -> None:
    if os.name == "posix":
        os.setsid()
        ready.value = _WorkerReadiness.SESSION_CREATED
    store = SQLiteStateStore(database_url)
    record = store.get_dag_run(dag_id, run_id)
    if record is None:
        raise ValueError(f"DagRun 不存在: {dag_id}/{run_id}")
    with open_dag_from_file(file_path, dag_id) as dag:
        actual_hash = definition_hash_for_dag(dag, file_path)
        loaded_hash = definition_hash_for_dag(
            dag, file_path, source_sha256=getattr(dag, "_easyflow_source_sha256", None)
        )
        ready.value = _WorkerReadiness.DEFINITION_LOADED
        if time.monotonic() >= startup_deadline:
            outcome.value = _WorkerOutcome.STARTUP_TIMEOUT
            return
        if record.definition_hash is not None and (
            actual_hash != record.definition_hash or loaded_hash != record.definition_hash
        ):
            store.fail_dag_run_definition_changed(
                dag_id=dag_id,
                run_id=run_id,
                expected_hash=record.definition_hash,
                actual_hash=(
                    actual_hash if actual_hash != record.definition_hash else loaded_hash
                ),
            )
            return
        task = dag.get_task(task_id)
        runner = PersistentDagRunner(
            store,
            log_root=log_root,
            task_lease_seconds=task_lease_seconds,
            scheduler_fence=scheduler_fence,
        )
        xcom_store = PersistentXComStore(store, dag_id, run_id)
        try:
            runner._run_task_until_terminal(
                dag=dag,
                task=task,
                run_id=run_id,
                logical_date=datetime.fromisoformat(record.logical_date).astimezone(
                    run_timezone(record.logical_timezone)
                ),
                conf=conf,
                xcom_store=xcom_store,
                single_attempt=True,
            )
        except SchedulerLeaseLost:
            outcome.value = _WorkerOutcome.LEASE_LOST
        except DagPaused:
            outcome.value = _WorkerOutcome.DAG_PAUSED
