from __future__ import annotations

import multiprocessing
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from easyflow._internal.dag_parser import load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner, PersistentXComStore
from easyflow._internal.state_store import SQLiteStateStore
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

    def run(self) -> dict[str, TaskInstanceState | None]:
        context = multiprocessing.get_context("spawn")
        active: dict[str, multiprocessing.Process] = {}
        ordered_tasks = self.dag.topological_sort()
        max_active = min(self.parallelism, self.max_active_tasks)

        while True:
            for task_id, process in list(active.items()):
                if not process.is_alive():
                    process.join()
                    active.pop(task_id)

            self.store.recover_orphaned_tasks_for_run(dag=self.dag, run_id=self.run_id)
            task_states = self.store.list_task_states(self.dag.dag_id, self.run_id)
            if not active and all(state in TERMINAL_STATES for state in task_states.values()):
                return task_states

            progressed = False
            for task in ordered_tasks:
                if len(active) >= max_active:
                    break
                if task.task_id in active:
                    continue
                current_state = task_states[task.task_id]
                if current_state in TERMINAL_STATES:
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

                process = context.Process(
                    target=run_local_task_worker,
                    args=(
                        self.store.database_url,
                        self.dag.dag_id,
                        self.file_path,
                        self.run_id,
                        task.task_id,
                        self.logical_date.isoformat(),
                        self.conf,
                        self.log_root,
                        self.task_lease_seconds,
                    ),
                )
                process.start()
                active[task.task_id] = process
                progressed = True

            if not progressed:
                if active:
                    time.sleep(0.05)
                    continue
                waiting = [
                    task_id
                    for task_id, state in task_states.items()
                    if state not in TERMINAL_STATES
                ]
                raise RuntimeError(f"LocalExecutor 无法继续推进。等待任务: {waiting}")
            time.sleep(0.01)


def run_local_task_worker(
    database_url: str,
    dag_id: str,
    file_path: str,
    run_id: str,
    task_id: str,
    logical_date_iso: str,
    conf: dict[str, Any],
    log_root: str,
    task_lease_seconds: int,
) -> None:
    dag = load_dag_from_file(file_path, dag_id)
    task = dag.get_task(task_id)
    store = SQLiteStateStore(database_url)
    runner = PersistentDagRunner(
        store,
        log_root=log_root,
        task_lease_seconds=task_lease_seconds,
    )
    xcom_store = PersistentXComStore(store, dag_id, run_id)
    runner._run_task_until_terminal(
        dag=dag,
        task=task,
        run_id=run_id,
        logical_date=datetime.fromisoformat(logical_date_iso),
        conf=conf,
        xcom_store=xcom_store,
    )
