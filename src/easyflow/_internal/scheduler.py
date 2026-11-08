from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from easyflow._internal.dag_parser import DagSource, load_dag_snapshot
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.schedules import (
    backfill_intervals,
    dag_timezone,
    scheduler_intervals,
)
from easyflow._internal.state_store import (
    BackfillRunNotEligible,
    DagPaused,
    DagRunCapacityError,
    ScheduledRunExists,
    SchedulerFence,
    SchedulerLeaseLost,
    SQLiteStateStore,
)
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
    complete: bool = True
    pending: tuple[datetime, ...] = ()


@dataclass(frozen=True)
class BackfillPreview:
    logical_date: datetime
    latest_state: DagRunState | None
    eligible: bool


def _backfill_intervals(dag: object, start_date: date, end_date: date) -> list:
    intervals = backfill_intervals(
        dag.schedule, start_date, end_date, dag_timezone(dag), anchor=dag.start_date
    )
    return [
        interval
        for interval in intervals
        if (
            dag.start_date is None
            or interval.logical_date.astimezone(timezone.utc)
            >= dag.start_date.astimezone(timezone.utc)
        )
        and (
            dag.end_date is None
            or interval.logical_date.astimezone(timezone.utc)
            <= dag.end_date.astimezone(timezone.utc)
        )
    ]


def preview_backfill(
    *,
    dag: object,
    store: SQLiteStateStore,
    start_date: date,
    end_date: date,
    reprocessing_behavior: str = "none",
) -> list[BackfillPreview]:
    if reprocessing_behavior not in {"none", "failed", "completed"}:
        raise ValueError("reprocessing-behavior 必须是 none、failed 或 completed。")
    result = []
    for interval in _backfill_intervals(dag, start_date, end_date):
        latest = store.latest_dag_run_state_for_logical_date(
            dag_id=dag.dag_id, logical_date=interval.logical_date
        )
        eligible = (
            latest is None
            or reprocessing_behavior == "failed"
            and latest == DagRunState.FAILED
            or reprocessing_behavior == "completed"
            and latest in {DagRunState.SUCCESS, DagRunState.FAILED}
        )
        result.append(BackfillPreview(interval.logical_date, latest, eligible))
    return result


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
    if not use_lease:
        raise ValueError("不允许禁用 scheduler lease。")
    dag = load_dag_snapshot(file_path, dag_id)
    source = DagSource(dag_id=dag.dag_id, file_path=Path(file_path).resolve(), dag=dag)
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
    with scheduler_session(
        store=store, owner=owner, lease_seconds=lease_seconds
    ) as heartbeat:
        return _run_sources_with_lease(
            sources=sources,
            store=store,
            now=now,
            log_root=log_root,
            executor=executor,
            parallelism=parallelism,
            heartbeat=heartbeat,
        )


@contextmanager
def scheduler_session(
    *, store: SQLiteStateStore, owner: str | None, lease_seconds: int
) -> Iterator[_SchedulerHeartbeat]:
    if lease_seconds <= 0:
        raise ValueError("lease_seconds 必须大于 0。")
    owner = f"{owner or f'scheduler-{os.getpid()}'}-{uuid.uuid4().hex}"
    lease = store.acquire_scheduler_lease(
        owner=owner,
        lease_seconds=lease_seconds,
    )
    if not lease.acquired:
        raise RuntimeError(
            f"scheduler lease 已被 {lease.owner!r} 持有，过期时间 {lease.lease_expires_at}。"
        )
    heartbeat = _SchedulerHeartbeat(
        store, owner, lease_seconds, datetime.now(timezone.utc)
    )
    try:
        heartbeat.start()
        yield heartbeat
    finally:
        heartbeat.stop()
        store.release_scheduler_lease(owner=owner)


def _run_sources_with_lease(
    *,
    sources: Sequence[DagSource],
    store: SQLiteStateStore,
    now: datetime,
    log_root: str | Path,
    executor: str,
    parallelism: int,
    heartbeat: _SchedulerHeartbeat,
) -> tuple[list[ScheduledRunSummary], list[SchedulerIssue]]:
    with SchedulerCoordinator(
        store, heartbeat, log_root, executor, parallelism
    ) as coordinator:
        coordinator.submit(sources, now)
        return coordinator.collect(wait=True)


class SchedulerCoordinator:
    def __init__(
        self,
        store: SQLiteStateStore,
        heartbeat: _SchedulerHeartbeat,
        log_root: str | Path,
        executor: str,
        parallelism: int,
    ) -> None:
        self.store = store
        self.heartbeat = heartbeat
        self.log_root = log_root
        self.executor = executor
        self.parallelism = parallelism
        self.stopped = threading.Event()
        self.pool = ThreadPoolExecutor(
            max_workers=64, thread_name_prefix="easyflow-dag"
        )
        self.pending: dict[
            str, tuple[DagSource, Future[list[ScheduledRunSummary]]]
        ] = {}

    def __enter__(self) -> SchedulerCoordinator:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stopped.set()
        self.pool.shutdown(wait=True, cancel_futures=True)

    def submit(self, sources: Sequence[DagSource], now: datetime) -> None:
        if self.heartbeat.lost.is_set():
            raise SchedulerLeaseLost("scheduler lease 已丢失，停止调度。")
        for source in sources:
            if source.dag_id in self.pending:
                continue
            future = self.pool.submit(
                _run_scheduler_source_once,
                source=source,
                store=self.store,
                now=now,
                log_root=self.log_root,
                executor=self.executor,
                parallelism=self.parallelism,
                should_continue=lambda: (
                    not self.heartbeat.lost.is_set() and not self.stopped.is_set()
                ),
                scheduler_fence=self.heartbeat.fence,
            )
            self.pending[source.dag_id] = (source, future)

    def collect(
        self, *, wait: bool = False
    ) -> tuple[list[ScheduledRunSummary], list[SchedulerIssue]]:
        summaries: list[ScheduledRunSummary] = []
        issues: list[SchedulerIssue] = []
        futures = {future: source for source, future in self.pending.values()}
        ready = (
            as_completed(futures)
            if wait
            else (future for future in futures if future.done())
        )
        for future in ready:
            source = futures[future]
            self.pending.pop(source.dag_id, None)
            try:
                summaries.extend(future.result())
            except SchedulerLeaseLost:
                raise
            except Exception as exc:  # noqa: BLE001 - 单个 DAG 调度失败不阻断其他 DAG。
                issues.append(SchedulerIssue(source.dag_id, source.file_path, str(exc)))
        if self.heartbeat.lost.is_set():
            raise SchedulerLeaseLost("scheduler lease 已丢失，停止调度。")
        return sorted(summaries, key=lambda item: (item.dag_id, item.run_id)), issues


def _run_scheduler_source_once(
    *,
    source: DagSource,
    store: SQLiteStateStore,
    now: datetime,
    log_root: str | Path,
    executor: str,
    parallelism: int,
    should_continue: Callable[[], bool] | None = None,
    scheduler_fence: SchedulerFence | None = None,
) -> list[ScheduledRunSummary]:
    dag = source.dag
    allowed = should_continue or (lambda: True)
    runner = PersistentDagRunner(
        store,
        log_root=log_root,
        should_continue=allowed,
        scheduler_fence=scheduler_fence,
    )
    summaries: list[ScheduledRunSummary] = []
    while allowed():
        if store.is_dag_paused(dag.dag_id):
            break
        run_ids = store.list_active_dag_runs(dag.dag_id)
        for interval in scheduler_intervals(
            dag,
            now,
            last_automated_date=store.latest_automated_logical_date(dag.dag_id),
        ):
            if not allowed():
                break
            try:
                run_ids.append(
                    runner.create_scheduled_run(
                        dag, file_path=source.file_path, interval=interval
                    )
                )
            except ScheduledRunExists:
                continue
            except DagPaused:
                break
            except DagRunCapacityError:
                break
        if not run_ids:
            break
        with ThreadPoolExecutor(max_workers=len(run_ids)) as pool:
            futures = [
                pool.submit(
                    runner.run_existing,
                    dag,
                    run_id=run_id,
                    file_path=source.file_path,
                    executor=executor,
                    parallelism=parallelism,
                    wait_for_retry=False,
                )
                for run_id in run_ids
            ]
            results = [future.result() for future in futures]
        summaries.extend(
            ScheduledRunSummary(
                dag_id=dag.dag_id, run_id=result.run_id, state=result.state
            )
            for result in results
        )
        if any(
            result.state in {DagRunState.QUEUED, DagRunState.RUNNING}
            for result in results
        ):
            break
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
    all_summaries: list[ScheduledRunSummary] = []
    loops = 0
    with scheduler_session(
        store=store, owner=owner, lease_seconds=lease_seconds
    ) as heartbeat:
        with SchedulerCoordinator(
            store, heartbeat, log_root, executor, parallelism
        ) as coordinator:
            while True:
                summaries, issues = coordinator.collect()
                if issues:
                    raise RuntimeError(issues[0].message)
                if max_loops is not None:
                    all_summaries.extend(summaries)
                dag = load_dag_snapshot(file_path, dag_id)
                source = DagSource(
                    dag_id=dag.dag_id, file_path=Path(file_path).resolve(), dag=dag
                )
                coordinator.submit([source], datetime.now(timezone.utc))
                loops += 1
                if max_loops is not None and loops >= max_loops:
                    summaries, issues = coordinator.collect(wait=True)
                    if issues:
                        raise RuntimeError(issues[0].message)
                    return all_summaries + summaries
                if heartbeat.lost.wait(interval_seconds):
                    raise SchedulerLeaseLost("scheduler lease 已丢失，停止调度。")


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
    dag = load_dag_snapshot(file_path, dag_id)
    if max_active_runs is not None and max_active_runs < 1:
        raise ValueError("max_active_runs 必须大于 0。")
    intervals = _backfill_intervals(dag, start_date, end_date)
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
    index = 0
    batch_size = min(max_active_runs or 1, len(intervals))
    with ThreadPoolExecutor(max_workers=batch_size) as pool:
        while index < len(intervals):
            batch = []
            capacity_full = False
            while index < len(intervals) and len(batch) < batch_size:
                interval = intervals[index]
                try:
                    run_id = runner.create_backfill_run(
                        dag,
                        file_path=file_path,
                        interval=interval,
                        backfill_id=backfill_id,
                        max_active_runs=max_active_runs,
                        reprocessing_behavior=reprocessing_behavior,
                    )
                except BackfillRunNotEligible:
                    index += 1
                    continue
                except DagRunCapacityError:
                    capacity_full = True
                    break
                index += 1
                future = pool.submit(
                    runner.run_existing,
                    dag,
                    run_id=run_id,
                    file_path=file_path,
                    executor=executor,
                    parallelism=parallelism,
                )
                batch.append((interval, future))
            pending = []
            for interval, future in batch:
                result = future.result()
                summaries.append(
                    ScheduledRunSummary(
                        dag_id=dag.dag_id, run_id=result.run_id, state=result.state
                    )
                )
                if result.state in {DagRunState.QUEUED, DagRunState.RUNNING}:
                    pending.append(interval.logical_date)
            if capacity_full or pending:
                pending.extend(item.logical_date for item in intervals[index:])
                return BackfillSummary(backfill_id, summaries, False, tuple(pending))
    return BackfillSummary(backfill_id=backfill_id, runs=summaries)


class _SchedulerHeartbeat:
    def __init__(
        self, store: SQLiteStateStore, owner: str, lease_seconds: int, now: datetime
    ) -> None:
        self.store = store
        self.owner = owner
        self.lease_seconds = lease_seconds
        self.started = time.monotonic()
        self.fence = SchedulerFence(owner, now, self.started)
        self.lost = threading.Event()
        self._stopping = threading.Event()
        self._ready = threading.Event()
        self._process: subprocess.Popen[bytes] | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        environment = os.environ.copy()
        source_root = str(Path(__file__).resolve().parents[2])
        existing_pythonpath = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            source_root + os.pathsep + existing_pythonpath
            if existing_pythonpath
            else source_root
        )
        self._process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "easyflow._internal.scheduler_heartbeat",
                self.store.database_url,
                self.owner,
                str(self.lease_seconds),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=environment,
        )
        self._thread = threading.Thread(target=self._monitor, daemon=True)
        self._thread.start()
        if (
            not self._ready.wait(max(2, min(10, self.lease_seconds)))
            or self.lost.is_set()
        ):
            self.lost.set()
            raise SchedulerLeaseLost("scheduler lease 续租进程启动失败。")

    def stop(self) -> None:
        self._stopping.set()
        process = self._process
        if process is None:
            return
        if process.stdin is not None:
            try:
                process.stdin.close()
            except BrokenPipeError:
                pass
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if self._thread is not None:
            self._thread.join(timeout=2)
        if process.stdout is not None:
            process.stdout.close()

    def _monitor(self) -> None:
        assert self._process is not None and self._process.stdout is not None
        try:
            if self._process.stdout.readline().strip() == b"ready":
                self._ready.set()
                self._process.wait()
            else:
                self.lost.set()
        finally:
            if not self._stopping.is_set():
                self.lost.set()
            self._ready.set()
