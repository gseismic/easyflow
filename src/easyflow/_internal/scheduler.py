from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from collections.abc import Sequence
from pathlib import Path

from easyflow._internal.dag_parser import DagSource, load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.schedules import backfill_intervals, scheduler_intervals
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.sdk.states import DagRunState


@dataclass(frozen=True)
class ScheduledRunSummary:
    run_id: str
    state: DagRunState
    dag_id: str = ""


@dataclass(frozen=True)
class SchedulerIssue:
    dag_id: str
    file_path: Path
    message: str


@dataclass(frozen=True)
class BackfillSummary:
    backfill_id: str
    runs: list[ScheduledRunSummary]


def run_scheduler_once(
    *,
    dag_id: str,
    file_path: str | Path,
    store: SQLiteStateStore,
    now: datetime | None = None,
    log_root: str | Path = "./.easyflow/logs",
    use_lease: bool = True,
    owner: str | None = None,
    lease_seconds: int = 30,
    executor: str = "sequential",
    parallelism: int = 1,
) -> list[ScheduledRunSummary]:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now 必须带时区。")
    dag = load_dag_from_file(file_path, dag_id)
    source = DagSource(dag_id=dag.dag_id, file_path=Path(file_path).resolve(), dag=dag)
    if not use_lease:
        return _run_scheduler_source_once(
            source=source,
            store=store,
            now=now,
            log_root=log_root,
            executor=executor,
            parallelism=parallelism,
        )
    summaries, issues = run_scheduler_once_for_sources(
        sources=[source],
        store=store,
        now=now,
        log_root=log_root,
        owner=owner,
        lease_seconds=lease_seconds,
        executor=executor,
        parallelism=parallelism,
    )
    if issues:
        raise RuntimeError(issues[0].message)
    return summaries


def run_scheduler_once_for_sources(
    *,
    sources: Sequence[DagSource],
    store: SQLiteStateStore,
    now: datetime | None = None,
    log_root: str | Path = "./.easyflow/logs",
    owner: str | None = None,
    lease_seconds: int = 30,
    executor: str = "sequential",
    parallelism: int = 1,
) -> tuple[list[ScheduledRunSummary], list[SchedulerIssue]]:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now 必须带时区。")
    owner = owner or f"scheduler-{os.getpid()}"
    lease = store.acquire_scheduler_lease(
        owner=owner,
        lease_seconds=lease_seconds,
        now=now,
    )
    if not lease.acquired:
        raise RuntimeError(
            f"scheduler lease 已被 {lease.owner!r} 持有，过期时间 {lease.lease_expires_at}。"
        )
    try:
        summaries: list[ScheduledRunSummary] = []
        issues: list[SchedulerIssue] = []
        for source in sorted(sources, key=lambda item: (item.dag_id, item.file_path.as_posix())):
            try:
                summaries.extend(
                    _run_scheduler_source_once(
                        source=source,
                        store=store,
                        now=now,
                        log_root=log_root,
                        executor=executor,
                        parallelism=parallelism,
                    )
                )
            except Exception as exc:  # noqa: BLE001 - 单个 DAG 调度失败不阻断其他 DAG。
                issues.append(SchedulerIssue(source.dag_id, source.file_path, str(exc)))
        return summaries, issues
    finally:
        store.release_scheduler_lease(owner=owner)


def _run_scheduler_source_once(
    *,
    source: DagSource,
    store: SQLiteStateStore,
    now: datetime,
    log_root: str | Path,
    executor: str,
    parallelism: int,
) -> list[ScheduledRunSummary]:
    dag = source.dag
    runner = PersistentDagRunner(store, log_root=log_root)
    summaries: list[ScheduledRunSummary] = []
    max_active_runs = dag.max_active_runs or 1
    for interval in scheduler_intervals(dag, now):
        if store.count_active_dag_runs(dag_id=dag.dag_id) >= max_active_runs:
            break
        if store.dag_run_exists_for_logical_date(
            dag_id=dag.dag_id,
            logical_date=interval.logical_date,
            run_type="scheduled",
        ):
            continue
        run_id = runner.create_scheduled_run(dag, file_path=source.file_path, interval=interval)
        result = runner.run_existing(
            dag,
            run_id=run_id,
            file_path=source.file_path,
            executor=executor,
            parallelism=parallelism,
        )
        summaries.append(
            ScheduledRunSummary(
                dag_id=dag.dag_id,
                run_id=result.run_id,
                state=result.state,
            )
        )
    return summaries


def run_scheduler_loop(
    *,
    dag_id: str,
    file_path: str | Path,
    store: SQLiteStateStore,
    log_root: str | Path = "./.easyflow/logs",
    owner: str | None = None,
    lease_seconds: int = 30,
    interval_seconds: float = 5.0,
    max_loops: int | None = None,
    executor: str = "sequential",
    parallelism: int = 1,
) -> list[ScheduledRunSummary]:
    owner = owner or f"scheduler-{os.getpid()}"
    all_summaries: list[ScheduledRunSummary] = []
    loops = 0
    while True:
        now = datetime.now(timezone.utc)
        lease = store.acquire_scheduler_lease(
            owner=owner,
            lease_seconds=lease_seconds,
            now=now,
        )
        if not lease.acquired:
            raise RuntimeError(
                f"scheduler lease 已被 {lease.owner!r} 持有，过期时间 {lease.lease_expires_at}。"
            )
        all_summaries.extend(
            run_scheduler_once(
                dag_id=dag_id,
                file_path=file_path,
                store=store,
                now=now,
                log_root=log_root,
                use_lease=False,
                owner=owner,
                lease_seconds=lease_seconds,
                executor=executor,
                parallelism=parallelism,
            )
        )
        loops += 1
        if max_loops is not None and loops >= max_loops:
            store.release_scheduler_lease(owner=owner)
            return all_summaries
        time.sleep(interval_seconds)


def create_backfill(
    *,
    dag_id: str,
    file_path: str | Path,
    store: SQLiteStateStore,
    start_date: date,
    end_date: date,
    reprocessing_behavior: str = "none",
    max_active_runs: int | None = None,
    backfill_id: str | None = None,
    log_root: str | Path = "./.easyflow/logs",
    executor: str = "sequential",
    parallelism: int = 1,
) -> BackfillSummary:
    if reprocessing_behavior not in {"none", "failed", "completed"}:
        raise ValueError("reprocessing-behavior 必须是 none、failed 或 completed。")
    dag = load_dag_from_file(file_path, dag_id)
    intervals = backfill_intervals(dag.schedule, start_date, end_date)
    if not intervals:
        backfill_id = backfill_id or f"backfill__{uuid.uuid4().hex}"
        return BackfillSummary(backfill_id=backfill_id, runs=[])
    backfill_id = backfill_id or f"backfill__{uuid.uuid4().hex}"
    store.create_backfill(
        backfill_id=backfill_id,
        dag_id=dag.dag_id,
        start_logical_date=intervals[0].logical_date,
        end_logical_date=intervals[-1].logical_date,
        reprocessing_behavior=reprocessing_behavior,
        max_active_runs=max_active_runs,
    )
    runner = PersistentDagRunner(store, log_root=log_root)
    summaries: list[ScheduledRunSummary] = []
    effective_max_active_runs = max_active_runs or dag.max_active_runs or 1
    for interval in intervals:
        if store.count_active_dag_runs(dag_id=dag.dag_id) >= effective_max_active_runs:
            break
        if not _should_create_backfill_run(
            store=store,
            dag_id=dag.dag_id,
            logical_date=interval.logical_date,
            reprocessing_behavior=reprocessing_behavior,
        ):
            continue
        run_id = runner.create_backfill_run(
            dag,
            file_path=file_path,
            interval=interval,
            backfill_id=backfill_id,
        )
        result = runner.run_existing(
            dag,
            run_id=run_id,
            file_path=file_path,
            executor=executor,
            parallelism=parallelism,
        )
        summaries.append(
            ScheduledRunSummary(
                dag_id=dag.dag_id,
                run_id=result.run_id,
                state=result.state,
            )
        )
    return BackfillSummary(backfill_id=backfill_id, runs=summaries)


def _should_create_backfill_run(
    *,
    store: SQLiteStateStore,
    dag_id: str,
    logical_date: datetime,
    reprocessing_behavior: str,
) -> bool:
    latest_state = store.latest_dag_run_state_for_logical_date(
        dag_id=dag_id,
        logical_date=logical_date,
    )
    if latest_state is None:
        return True
    if reprocessing_behavior == "none":
        return False
    if reprocessing_behavior == "failed":
        return latest_state == DagRunState.FAILED
    if reprocessing_behavior == "completed":
        return latest_state in {DagRunState.SUCCESS, DagRunState.FAILED}
    return False
