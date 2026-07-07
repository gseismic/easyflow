from __future__ import annotations

import copy
import io
import os
import threading
import time
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from easyflow._internal.dag_parser import definition_hash_for_dag
from easyflow._internal.schedules import DataInterval
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.sdk.context import set_current_context
from easyflow.sdk.dag import DAG
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


class PersistentXComStore:
    def __init__(self, store: SQLiteStateStore, dag_id: str, run_id: str) -> None:
        self.store = store
        self.dag_id = dag_id
        self.run_id = run_id
        self.pending: dict[tuple[str, str], Any] = {}

    def push(self, task_id: str, key: str, value: Any) -> None:
        self.pending[(task_id, key)] = value

    def pull(self, task_id: str, key: str = "return_value") -> Any:
        return self.store.pull_xcom(self.dag_id, self.run_id, task_id, key)

    def values_for_task(self, task_id: str) -> dict[str, Any]:
        return {
            key: value
            for (stored_task_id, key), value in self.pending.items()
            if stored_task_id == task_id
        }

    def clear_task(self, task_id: str) -> None:
        for key in [
            key for key in self.pending if key[0] == task_id
        ]:
            self.pending.pop(key, None)


class PersistentDagRunner:
    def __init__(
        self,
        store: SQLiteStateStore,
        *,
        log_root: str | Path = "./.easyflow/logs",
        task_lease_seconds: int = 30,
    ) -> None:
        self.store = store
        self.log_root = Path(log_root)
        self.task_lease_seconds = task_lease_seconds
        self.runner_id = f"runner-{os.getpid()}"

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
        run_id = run_id or f"manual__{logical_date.astimezone(timezone.utc).isoformat()}"
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
        run_id = f"scheduled__{interval.logical_date.astimezone(timezone.utc).isoformat()}"
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
    ) -> str:
        self.store.create_dag_run(
            dag=dag,
            run_id=run_id,
            run_type=run_type,
            logical_date=interval.logical_date,
            data_interval_start=interval.data_interval_start,
            data_interval_end=interval.data_interval_end,
            definition_hash=definition_hash_for_dag(dag, file_path),
            file_path=str(file_path) if file_path is not None else None,
            conf=conf or {},
            backfill_id=backfill_id,
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
        if file_path is not None:
            current_hash = definition_hash_for_dag(dag, file_path)
            if record.definition_hash is not None and record.definition_hash != current_hash:
                self.store.fail_dag_run_definition_changed(
                    dag_id=dag.dag_id,
                    run_id=run_id,
                    expected_hash=record.definition_hash,
                    actual_hash=current_hash,
                )
                return PersistentRunResult(
                    dag_id=dag.dag_id,
                    run_id=run_id,
                    state=DagRunState.FAILED,
                    task_states=self.store.list_task_states(dag.dag_id, run_id),
                )
        logical_date = datetime.fromisoformat(record.logical_date)
        conf = {}
        if record.conf_json:
            import json

            conf = json.loads(record.conf_json)
        if executor == "local":
            if file_path is None:
                raise ValueError("LocalExecutor 需要 file_path 以便子进程重新加载 DAG。")
            return self._run_existing_local(
                dag=dag,
                run_id=run_id,
                file_path=file_path,
                record=record,
                logical_date=logical_date,
                conf=conf,
                parallelism=parallelism,
            )
        if executor != "sequential":
            raise ValueError(f"未知 executor: {executor}")
        self.store.recover_orphaned_tasks_for_run(dag=dag, run_id=run_id)
        self.store.set_dag_run_state(dag.dag_id, run_id, DagRunState.RUNNING)
        xcom_store = PersistentXComStore(self.store, dag.dag_id, run_id)
        ordered_tasks = dag.topological_sort()

        while True:
            task_states = self.store.list_task_states(dag.dag_id, run_id)
            if all(state in TERMINAL_STATES for state in task_states.values()):
                break
            progressed = False
            for task in ordered_tasks:
                current_state = task_states[task.task_id]
                if current_state in TERMINAL_STATES:
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
                self.store.set_dag_run_state(dag.dag_id, run_id, DagRunState.FAILED)
                raise RuntimeError(f"DagRun {dag.dag_id}/{run_id} 无法继续推进。")

        final_states = self.store.list_task_states(dag.dag_id, run_id)
        final_state = (
            DagRunState.FAILED
            if any(
                state in {TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED}
                for state in final_states.values()
            )
            else DagRunState.SUCCESS
        )
        self.store.set_dag_run_state(dag.dag_id, run_id, final_state)
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
        )
        final_states = executor.run()
        final_state = (
            DagRunState.FAILED
            if any(
                state in {TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED}
                for state in final_states.values()
            )
            else DagRunState.SUCCESS
        )
        self.store.set_dag_run_state(dag.dag_id, run_id, final_state)
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
        if previous.state in {TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED}:
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
    ) -> None:
        retries = _resolve_retries(dag, task)
        while True:
            current_state = self.store.get_task_state(dag.dag_id, run_id, task.task_id)
            if current_state in TERMINAL_STATES:
                return
            try_number = self.store.start_task_try(
                dag.dag_id,
                run_id,
                task.task_id,
                owner=self.runner_id,
                lease_seconds=self.task_lease_seconds,
            )
            if try_number is None:
                return
            heartbeat = _HeartbeatThread(
                store=self.store,
                dag_id=dag.dag_id,
                run_id=run_id,
                task_id=task.task_id,
                try_number=try_number,
                owner=self.runner_id,
                lease_seconds=self.task_lease_seconds,
            )
            heartbeat.start()
            log_path = self._log_path(dag.dag_id, run_id, task.task_id, try_number)
            self.store.set_task_try_log_path(
                dag.dag_id,
                run_id,
                task.task_id,
                try_number,
                str(log_path),
            )
            try:
                result = self._run_single_attempt(
                    dag=dag,
                    task=task,
                    run_id=run_id,
                    logical_date=logical_date,
                    conf=conf,
                    xcom_store=xcom_store,
                    log_path=log_path,
                )
                heartbeat.stop()
                _push_result(task, result, xcom_store)
                self.store.commit_success(
                    dag_id=dag.dag_id,
                    run_id=run_id,
                    task_id=task.task_id,
                    try_number=try_number,
                    xcom_values=xcom_store.values_for_task(task.task_id),
                )
                xcom_store.clear_task(task.task_id)
                return
            except Exception as exc:
                heartbeat.stop()
                xcom_store.clear_task(task.task_id)
                next_state = self.store.commit_failure(
                    dag_id=dag.dag_id,
                    run_id=run_id,
                    task_id=task.task_id,
                    try_number=try_number,
                    error=exc,
                    retries=retries,
                )
                if next_state == TaskInstanceState.FAILED:
                    return

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
        interval_start, interval_end = _data_interval(dag, logical_date)
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

    def _log_path(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> Path:
        safe_run_id = run_id.replace("/", "_").replace(":", "_")
        return (
            self.log_root
            / dag_id
            / safe_run_id
            / task_id
            / "-1"
            / f"{try_number}.log"
        )


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


class _HeartbeatThread:
    def __init__(
        self,
        *,
        store: SQLiteStateStore,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int,
        owner: str,
        lease_seconds: int,
    ) -> None:
        self.store = store
        self.dag_id = dag_id
        self.run_id = run_id
        self.task_id = task_id
        self.try_number = try_number
        self.owner = owner
        self.lease_seconds = lease_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._renew()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)

    def _run(self) -> None:
        interval = max(0.2, min(1.0, self.lease_seconds / 3))
        while not self._stop.wait(interval):
            self._renew()

    def _renew(self) -> None:
        self.store.renew_task_lease(
            dag_id=self.dag_id,
            run_id=self.run_id,
            task_id=self.task_id,
            try_number=self.try_number,
            owner=self.owner,
            lease_seconds=self.lease_seconds,
        )
