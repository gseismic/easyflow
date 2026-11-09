from __future__ import annotations

import multiprocessing
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from easyflow._internal.dag_parser import (
    PARSER_TIMEOUT_SECONDS,
    definition_hash_for_dag,
    load_dag_from_file,
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
)
from easyflow.sdk.states import TaskInstanceState
from easyflow.sdk.testing import TERMINAL_STATES, _dependency_decision


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
        context = multiprocessing.get_context("spawn")
        active: dict[str, multiprocessing.Process] = {}
        ready_events: dict[str, Any] = {}
        started_at: dict[str, float] = {}
        ordered_tasks = self.dag.topological_sort()
        max_active = min(self.parallelism, self.max_active_tasks)

        while True:
            if not self.should_continue():
                for process in active.values():
                    process.terminate()
                    process.join()
                return self.store.list_task_states(self.dag.dag_id, self.run_id)
            for task_id, process in list(active.items()):
                if (
                    process.is_alive()
                    and not ready_events[task_id].is_set()
                    and time.monotonic() - started_at[task_id] > PARSER_TIMEOUT_SECONDS
                ):
                    process.terminate()
                    process.join()
                if not process.is_alive():
                    process.join()
                    active.pop(task_id)
                    ready_events.pop(task_id)
                    started_at.pop(task_id)
                    if process.exitcode == 75:
                        for other in active.values():
                            other.terminate()
                            other.join()
                        raise SchedulerLeaseLost("scheduler lease 已丢失，停止调度。")
                    if process.exitcode == 77:
                        continue
                    state = self.store.get_task_state(
                        self.dag.dag_id, self.run_id, task_id
                    )
                    if (
                        process.exitcode == 0
                        and state == TaskInstanceState.UP_FOR_RETRY
                    ):
                        continue
                    if state is None or state in {
                        TaskInstanceState.QUEUED,
                        TaskInstanceState.SCHEDULED,
                        TaskInstanceState.UP_FOR_RETRY,
                    }:
                        record = self.store.get_dag_run(self.dag.dag_id, self.run_id)
                        if record is not None and record.state == "failed":
                            continue
                        message = (
                            f"Worker 在领取任务前退出，exitcode={process.exitcode}。"
                        )
                        self._fail_startup(task_id, message)

            record = self.store.get_dag_run(self.dag.dag_id, self.run_id)
            if record is not None and record.state == "failed":
                for process in active.values():
                    process.terminate()
                    process.join()
                return self.store.list_task_states(self.dag.dag_id, self.run_id)

            self.store.recover_orphaned_tasks_for_run(dag=self.dag, run_id=self.run_id)
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
            for task in ordered_tasks:
                if len(active) >= max_active:
                    break
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

                ready = context.Event()
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
                    ),
                )
                try:
                    process.start()
                except Exception as exc:
                    self._fail_startup(
                        task.task_id, f"Worker 启动失败: {type(exc).__name__}: {exc}"
                    )
                    progressed = True
                    continue
                active[task.task_id] = process
                ready_events[task.task_id] = ready
                started_at[task.task_id] = time.monotonic()
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

    def _fail_startup(self, task_id: str, message: str) -> None:
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
    scheduler_fence: SchedulerFence | None = None,
    ready: Any | None = None,
) -> None:
    store = SQLiteStateStore(database_url)
    record = store.get_dag_run(dag_id, run_id)
    if record is None:
        raise ValueError(f"DagRun 不存在: {dag_id}/{run_id}")
    dag = load_dag_from_file(file_path, dag_id)
    actual_hash = definition_hash_for_dag(dag, file_path)
    loaded_hash = definition_hash_for_dag(
        dag, file_path, source_sha256=getattr(dag, "_easyflow_source_sha256", None)
    )
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
    if ready is not None:
        ready.set()
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
        raise SystemExit(75)
    except DagPaused:
        raise SystemExit(77)
