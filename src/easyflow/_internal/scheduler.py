from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timezone
from pathlib import Path

from easyflow._internal.dag_parser import (
    DagSource,
    DagSourceChanged,
    dag_source_is_current,
    load_dag_snapshot,
    refresh_dag_source,
)
from easyflow._internal.persistent_runner import (
    PersistentDagRunner,
    PersistentRunResult,
    validate_executor,
)
from easyflow._internal.schedules import (
    MAX_CATCHUP_PER_SCAN,
    backfill_intervals,
    backfill_logical_date_ranges,
    dag_timezone,
    iter_backfill_intervals,
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
    pending_ranges: tuple[tuple[datetime, datetime], ...] = ()
    interruption: str | None = None

    @property
    def pending_range(self) -> tuple[datetime, datetime] | None:
        if not self.pending_ranges:
            return None
        return self.pending_ranges[0][0], self.pending_ranges[-1][1]


@dataclass(frozen=True)
class BackfillPreview:
    logical_date: datetime
    latest_state: DagRunState | None
    eligible: bool


MAX_BACKFILL_PREVIEW_INTERVALS = 1000


def _backfill_intervals(
    dag: object,
    start_date: date | datetime,
    end_date: date | datetime,
    *,
    max_intervals: int | None = None,
) -> list:
    return backfill_intervals(
        dag.schedule,
        start_date,
        end_date,
        dag_timezone(dag),
        anchor=dag.start_date,
        max_intervals=max_intervals,
        start_bound=dag.start_date,
        end_bound=dag.end_date,
    )


def preview_backfill(
    *,
    dag: object,
    store: SQLiteStateStore,
    start_date: date | datetime,
    end_date: date | datetime,
    reprocessing_behavior: str = "none",
) -> list[BackfillPreview]:
    if reprocessing_behavior not in {"none", "failed", "completed"}:
        raise ValueError("reprocessing-behavior 必须是 none、failed 或 completed。")
    intervals = _backfill_intervals(
        dag, start_date, end_date, max_intervals=MAX_BACKFILL_PREVIEW_INTERVALS
    )
    if not intervals:
        return []
    latest_states = store.latest_dag_run_states_for_range(
        dag_id=dag.dag_id,
        start=intervals[0].logical_date,
        end=intervals[-1].logical_date,
    )
    result = []
    for interval in intervals:
        latest = latest_states.get(
            interval.logical_date.astimezone(timezone.utc).isoformat()
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
    validate_executor(executor)
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
    validate_executor(executor)
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
    stopped = threading.Event()
    runner = PersistentDagRunner(
        store,
        log_root=log_root,
        should_continue=lambda: not heartbeat.lost.is_set() and not stopped.is_set(),
        scheduler_fence=heartbeat.fence,
    )
    summaries: list[ScheduledRunSummary] = []
    issues: list[SchedulerIssue] = []
    with _run_pool(stopped, max_workers=64, thread_name_prefix="easyflow-dag") as pool:
        futures = {
            pool.submit(
                _run_scheduler_source_once,
                source=source,
                runner=runner,
                now=now,
                executor=executor,
                parallelism=parallelism,
            ): source
            for source in sources
        }
        for future in as_completed(futures):
            source = futures[future]
            try:
                summaries.extend(future.result())
            except SchedulerLeaseLost:
                raise
            except Exception as exc:  # noqa: BLE001 - 单个 DAG 调度失败不阻断其他 DAG。
                issues.append(SchedulerIssue(source.dag_id, source.file_path, str(exc)))
    if heartbeat.lost.is_set():
        raise SchedulerLeaseLost("scheduler lease 已丢失，停止调度。")
    return sorted(summaries, key=lambda item: (item.dag_id, item.run_id)), issues


@contextmanager
def _run_pool(
    stopped: threading.Event, *, max_workers: int, thread_name_prefix: str
) -> Iterator[ThreadPoolExecutor]:
    pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=thread_name_prefix)
    try:
        yield pool
    finally:
        # worker 有独立进程组，先通知执行器停止，再等待调度线程退出。
        stopped.set()
        pool.shutdown(wait=True, cancel_futures=True)


class SchedulerCoordinator:
    def __init__(
        self,
        store: SQLiteStateStore,
        heartbeat: _SchedulerHeartbeat,
        log_root: str | Path,
        executor: str,
        parallelism: int,
    ) -> None:
        validate_executor(executor)
        self.heartbeat = heartbeat
        self.executor = executor
        self.parallelism = parallelism
        self.stopped = threading.Event()
        self.runner = PersistentDagRunner(
            store,
            log_root=log_root,
            should_continue=lambda: (
                not heartbeat.lost.is_set() and not self.stopped.is_set()
            ),
            scheduler_fence=heartbeat.fence,
        )
        self.pool = ThreadPoolExecutor(
            max_workers=64, thread_name_prefix="easyflow-run"
        )
        self.pending: dict[
            tuple[str, str], tuple[DagSource, Future[PersistentRunResult]]
        ] = {}
        self.issues: list[SchedulerIssue] = []

    def __enter__(self) -> SchedulerCoordinator:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stopped.set()
        self.pool.shutdown(wait=True, cancel_futures=True)

    def submit(self, sources: Sequence[DagSource], now: datetime) -> None:
        if self.heartbeat.lost.is_set():
            raise SchedulerLeaseLost("scheduler lease 已丢失，停止调度。")
        for source in sources:
            try:
                run_ids, _ = _scan_scheduled_runs(source, self.runner, now)
            except SchedulerLeaseLost:
                raise
            except Exception as exc:  # noqa: BLE001 - 继续扫描其余 DAG。
                self.issues.append(
                    SchedulerIssue(source.dag_id, source.file_path, str(exc))
                )
                continue
            for run_id in run_ids:
                key = (source.dag_id, run_id)
                if key in self.pending:
                    continue
                future = self.pool.submit(
                    self.runner.run_existing,
                    source.dag,
                    run_id=run_id,
                    file_path=source.file_path,
                    executor=self.executor,
                    parallelism=self.parallelism,
                    wait_for_retry=False,
                )
                self.pending[key] = (source, future)

    def collect(
        self, *, wait: bool = False
    ) -> tuple[list[ScheduledRunSummary], list[SchedulerIssue]]:
        summaries: list[ScheduledRunSummary] = []
        issues, self.issues = self.issues, []
        futures = {future: key for key, (_, future) in self.pending.items()}
        ready = (
            as_completed(futures)
            if wait
            else (future for future in futures if future.done())
        )
        for future in ready:
            source, _ = self.pending.pop(futures[future])
            try:
                result = future.result()
                summaries.append(
                    ScheduledRunSummary(result.run_id, result.state, result.dag_id)
                )
            except SchedulerLeaseLost:
                raise
            except Exception as exc:  # noqa: BLE001 - 单个 DAG 调度失败不阻断其他 DAG。
                issues.append(SchedulerIssue(source.dag_id, source.file_path, str(exc)))
        if self.heartbeat.lost.is_set():
            raise SchedulerLeaseLost("scheduler lease 已丢失，停止调度。")
        return sorted(summaries, key=lambda item: (item.dag_id, item.run_id)), issues


def _scan_scheduled_runs(
    source: DagSource, runner: PersistentDagRunner, now: datetime
) -> tuple[list[str], bool]:
    dag = source.dag
    store = runner.store
    if not runner.should_continue() or store.is_dag_paused(dag.dag_id):
        return [], False
    run_ids = store.list_active_dag_runs(dag.dag_id)
    limit = dag.max_active_runs or 1
    if len(run_ids) >= limit and store.count_active_dag_runs(
        dag_id=dag.dag_id, include_backfill=False
    ) >= limit:
        return run_ids, False
    intervals = scheduler_intervals(
        dag,
        now,
        last_automated_date=store.latest_automated_logical_date(dag.dag_id),
    )
    backfilled = (
        store.backfilled_logical_dates(
            dag.dag_id, intervals[0].logical_date, intervals[-1].logical_date
        )
        if intervals
        else set()
    )
    last_skipped = None
    scanned_all = False
    for interval in intervals:
        if not runner.should_continue():
            break
        if interval.logical_date.astimezone(timezone.utc).isoformat() in backfilled:
            last_skipped = interval.logical_date
            continue
        try:
            run_ids.append(
                runner.create_scheduled_run(
                    dag, file_path=source.file_path, interval=interval
                )
            )
        except ScheduledRunExists:
            last_skipped = interval.logical_date
            continue
        except (DagPaused, DagRunCapacityError):
            break
        last_skipped = None
    else:
        scanned_all = True
    if last_skipped is not None and runner.should_continue():
        if not dag_source_is_current(dag, source.file_path):
            raise DagSourceChanged("推进调度游标前 DAG 源码或本地依赖已改变，请重新加载。")
        store.advance_schedule_cursor(
            dag.dag_id, last_skipped, scheduler_fence=runner.scheduler_fence
        )
    return run_ids, scanned_all and len(intervals) == MAX_CATCHUP_PER_SCAN


def _run_scheduler_source_once(
    *,
    source: DagSource,
    runner: PersistentDagRunner,
    now: datetime,
    executor: str,
    parallelism: int,
) -> list[ScheduledRunSummary]:
    source = refresh_dag_source(source)
    dag = source.dag
    summaries: list[ScheduledRunSummary] = []
    while runner.should_continue():
        run_ids, more_intervals = _scan_scheduled_runs(source, runner, now)
        if not run_ids:
            if more_intervals:
                continue
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
    validate_executor(executor)
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
    start_date: date | datetime,
    end_date: date | datetime,
    reprocessing_behavior: str = "none",
    max_active_runs: int | None = None,
    backfill_id: str | None = None,
    log_root: str | Path = "./.easyflow/logs",
    executor: str = "sequential",
    parallelism: int = 1,
) -> BackfillSummary:
    validate_executor(executor)
    if reprocessing_behavior not in {"none", "failed", "completed"}:
        raise ValueError("reprocessing-behavior 必须是 none、failed 或 completed。")
    dag = load_dag_snapshot(file_path, dag_id)
    if max_active_runs is not None and max_active_runs < 1:
        raise ValueError("max_active_runs 必须大于 0。")
    ranges = backfill_logical_date_ranges(
        dag.schedule,
        start_date,
        end_date,
        dag_timezone(dag),
        anchor=dag.start_date,
        start_bound=dag.start_date,
        end_bound=dag.end_date,
    )
    backfill_id = backfill_id or f"backfill__{uuid.uuid4().hex}"
    if not ranges:
        return BackfillSummary(backfill_id=backfill_id, runs=[])
    store.create_backfill(
        backfill_id=backfill_id,
        dag_id=dag.dag_id,
        start_logical_date=ranges[0][0],
        end_logical_date=ranges[-1][1],
        reprocessing_behavior=reprocessing_behavior,
        max_active_runs=max_active_runs,
    )
    stopped = threading.Event()
    runner = PersistentDagRunner(store, log_root=log_root, should_continue=lambda: not stopped.is_set())
    summaries: list[ScheduledRunSummary] = []
    intervals = (
        interval
        for first, last in ranges
        for interval in iter_backfill_intervals(dag.schedule, first, last)
    )
    current = next(intervals, None)
    batch_size = max_active_runs or 1
    with _run_pool(stopped, max_workers=batch_size, thread_name_prefix="easyflow-backfill") as pool:
        while current is not None:
            batch = []
            capacity_full = False
            interruption = None
            for _ in range(batch_size):
                if current is None:
                    break
                interval = current
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
                    current = next(intervals, None)
                    continue
                except DagRunCapacityError:
                    capacity_full = True
                    break
                except DagSourceChanged as exc:
                    interruption = str(exc)
                    break
                future = pool.submit(
                    runner.run_existing,
                    dag,
                    run_id=run_id,
                    file_path=file_path,
                    executor=executor,
                    parallelism=parallelism,
                )
                batch.append((interval, future))
                current = next(intervals, None)
            # 先等同批所有前序运行取得结果，再按日期重查被依赖阻挡的运行。
            batch_results = [(interval, future.result()) for interval, future in batch]
            pending = []
            for interval, result in batch_results:
                if interruption is None and result.state in {
                    DagRunState.QUEUED, DagRunState.RUNNING
                }:
                    result = runner.run_existing(
                        dag,
                        run_id=result.run_id,
                        file_path=file_path,
                        executor=executor,
                        parallelism=parallelism,
                    )
                summaries.append(
                    ScheduledRunSummary(
                        dag_id=dag.dag_id, run_id=result.run_id, state=result.state
                    )
                )
                if result.state in {DagRunState.QUEUED, DagRunState.RUNNING}:
                    pending.append(interval.logical_date)
            if (
                interruption is not None
                or capacity_full
                or pending
                or (current is not None and store.is_dag_paused(dag_id))
            ):
                remaining = ()
                if current is not None:
                    next_at = current.logical_date.astimezone(UTC)
                    remaining = tuple(
                        (first if first.astimezone(UTC) > next_at else current.logical_date, last)
                        for first, last in ranges
                        if last.astimezone(UTC) >= next_at
                    )
                return BackfillSummary(
                    backfill_id,
                    summaries,
                    complete=False,
                    pending=tuple(pending),
                    pending_ranges=remaining,
                    interruption=interruption,
                )
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
