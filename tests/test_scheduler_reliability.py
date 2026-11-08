from __future__ import annotations

import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import pytest

from easyflow._internal.dag_parser import load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.scheduler import create_backfill, run_scheduler_once
from easyflow._internal.schedules import interval_for_logical_date
from easyflow._internal.state_store import DagRunCapacityError, SQLiteStateStore
from easyflow.cli import main
from easyflow.sdk import DAG

UTC = timezone.utc


def _write(path, text):
    path.write_text(text, encoding="utf-8")
    return path


def test_scheduler_resumes_cleared_and_orphaned_runs(tmp_path):
    path = _write(
        tmp_path / "job.py",
        """
from easyflow.sdk import DAG, task
with DAG("resume", schedule="@daily") as dag:
    @task(retries=1)
    def work(): return "done"
    work()
""",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    dag = load_dag_from_file(path, "resume")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    first = interval_for_logical_date("@daily", datetime(2026, 7, 1, tzinfo=UTC))
    run = runner.create_scheduled_run(dag, file_path=path, interval=first)
    store.start_task_try(
        "resume", run, "work", now=datetime(2026, 7, 1, tzinfo=UTC), lease_seconds=1
    )
    now = datetime(2026, 7, 3, 12, tzinfo=UTC)
    summaries = run_scheduler_once(
        dag_id="resume",
        file_path=path,
        store=store,
        now=now,
        log_root=tmp_path / "logs",
    )
    assert summaries[0].run_id == run and summaries[0].state.value == "success"
    assert store.clear_task_instances(dag_id="resume", run_id=run).to_clear == ["work"]
    summaries = run_scheduler_once(
        dag_id="resume",
        file_path=path,
        store=store,
        now=now,
        log_root=tmp_path / "logs",
    )
    assert summaries[0].run_id == run and summaries[0].state.value == "success"
    with sqlite3.connect(store.path) as db:
        assert (
            db.execute(
                "SELECT COUNT(*) FROM task_try WHERE run_id=?", (run,)
            ).fetchone()[0]
            == 3
        )


def test_depends_on_past_waits_nonterminal_until_previous_succeeds(tmp_path):
    path = _write(
        tmp_path / "past.py",
        """
from easyflow.sdk import DAG, task
with DAG("past", schedule="@daily", max_active_runs=2) as dag:
    @task(depends_on_past=True)
    def work(): return "ok"
    work()
""",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    dag = load_dag_from_file(path, "past")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    first = runner.create_scheduled_run(
        dag,
        file_path=path,
        interval=interval_for_logical_date("@daily", datetime(2026, 7, 1, tzinfo=UTC)),
    )
    second = runner.create_scheduled_run(
        dag,
        file_path=path,
        interval=interval_for_logical_date("@daily", datetime(2026, 7, 2, tzinfo=UTC)),
    )
    waiting = runner.run_existing(dag, run_id=second, file_path=path)
    assert waiting.state.value == "running" and waiting.task_states["work"] is None
    assert (
        runner.run_existing(dag, run_id=first, file_path=path).state.value == "success"
    )
    assert (
        runner.run_existing(dag, run_id=second, file_path=path).state.value == "success"
    )


def test_capacity_is_atomic_for_simultaneous_manual_creations(tmp_path):
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    dag = DAG("race", max_active_runs=1)
    store.init_db()
    barrier = threading.Barrier(2)

    def create(index):
        runner = PersistentDagRunner(SQLiteStateStore(store.database_url))
        barrier.wait()
        try:
            return runner.create_manual_run(
                dag, file_path=None, logical_date=datetime.now(UTC), run_id=f"r{index}"
            )
        except DagRunCapacityError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        result = list(pool.map(create, range(2)))
    assert sum(value is not None for value in result) == 1
    assert store.count_active_dag_runs(dag_id="race") == 1


def test_backfill_capacity_is_independent_of_manual_trigger(tmp_path, capsys):
    path = _write(
        tmp_path / "capacity.py",
        'from easyflow.sdk import DAG\ndag = DAG("limited", schedule="@daily", max_active_runs=1)\n',
    )
    url = f"sqlite:///{tmp_path / 'db.sqlite'}"
    store = SQLiteStateStore(url)
    dag = load_dag_from_file(path, "limited")
    PersistentDagRunner(store).create_manual_run(
        dag,
        file_path=path,
        logical_date=datetime(2026, 6, 1, tzinfo=UTC),
        run_id="busy",
    )
    summary = create_backfill(
        dag_id="limited",
        file_path=path,
        store=store,
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 2),
        max_active_runs=2,
    )
    assert summary.complete and len(summary.runs) == 2
    assert (
        main(
            [
                "backfill",
                "create",
                "--dag-id",
                "limited",
                "--file",
                str(path),
                "--start-date",
                "2026-07-03",
                "--end-date",
                "2026-07-03",
                "--database-url",
                url,
            ]
        )
        == 0
    )
    assert "backfill_id=" in capsys.readouterr().out
    assert (
        main(["dags", "trigger", "limited", "--file", str(path), "--database-url", url])
        == 1
    )
    assert "max_active_runs" in capsys.readouterr().err


def test_long_task_keeps_scheduler_lease_and_owner_is_exclusive(tmp_path):
    started = tmp_path / "started"
    path = _write(
        tmp_path / "long.py",
        f"""
from pathlib import Path
import time
from easyflow.sdk import DAG, task
with DAG("long", schedule="@once") as dag:
    @task
    def work():
        Path({str(started)!r}).write_text("running")
        time.sleep(1.6)
    work()
""",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    errors = []

    def run():
        try:
            run_scheduler_once(
                dag_id="long",
                file_path=path,
                store=store,
                lease_seconds=1,
                owner="same",
                log_root=tmp_path / "logs",
            )
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    deadline = time.monotonic() + 5
    while not started.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert started.exists()
    time.sleep(1.1)
    with pytest.raises(RuntimeError, match="scheduler lease"):
        run_scheduler_once(
            dag_id="long",
            file_path=path,
            store=store,
            lease_seconds=1,
            owner="same",
            log_root=tmp_path / "logs",
        )
    thread.join(5)
    assert not thread.is_alive() and not errors


def test_lost_scheduler_lease_fences_next_task(tmp_path):
    started, second = tmp_path / "started", tmp_path / "second"
    path = _write(
        tmp_path / "fenced.py",
        f"""
from pathlib import Path
import time
from easyflow.sdk import DAG, task
with DAG("fenced", schedule="@once") as dag:
    @task
    def first():
        Path({str(started)!r}).write_text("running")
        time.sleep(0.8)
    @task
    def last(): Path({str(second)!r}).write_text("wrong")
    first() >> last()
""",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    errors = []

    def run():
        try:
            run_scheduler_once(
                dag_id="fenced",
                file_path=path,
                store=store,
                lease_seconds=1,
                log_root=tmp_path / "logs",
            )
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    deadline = time.monotonic() + 5
    while not started.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert started.exists()
    with sqlite3.connect(store.path) as db:
        db.execute("UPDATE scheduler_lease SET owner = 'replacement'")
    thread.join(5)
    assert not thread.is_alive() and errors and "lease" in str(errors[0])
    assert not second.exists()


def _compete_for_capacity(url, path, barrier, queue, kind):
    from easyflow._internal.dag_parser import load_dag_from_file
    from easyflow._internal.persistent_runner import PersistentDagRunner
    from easyflow._internal.schedules import interval_for_logical_date
    from easyflow._internal.state_store import DagRunCapacityError, SQLiteStateStore

    store = SQLiteStateStore(url)
    dag = load_dag_from_file(path, "process_race")
    runner = PersistentDagRunner(store)
    logical = datetime(2026, 7, 1, tzinfo=timezone.utc)
    barrier.wait()
    try:
        if kind == "manual":
            runner.create_manual_run(
                dag,
                file_path=path,
                logical_date=logical - timedelta(days=1),
                run_id="manual",
            )
        else:
            runner.create_backfill_run(
                dag,
                file_path=path,
                interval=interval_for_logical_date("@daily", logical),
                backfill_id="competition",
                reprocessing_behavior="none",
            )
        queue.put("created")
    except DagRunCapacityError:
        queue.put("full")


def test_backfill_and_manual_capacity_are_independent_across_processes(tmp_path):
    import multiprocessing

    path = _write(
        tmp_path / "process_race.py",
        'from easyflow.sdk import DAG\ndag = DAG("process_race", schedule="@daily", max_active_runs=1)\n',
    )
    url = f"sqlite:///{tmp_path / 'db.sqlite'}"
    store = SQLiteStateStore(url)
    store.init_db()
    ctx = multiprocessing.get_context("spawn")
    barrier, queue = ctx.Barrier(2), ctx.Queue()
    processes = [
        ctx.Process(
            target=_compete_for_capacity, args=(url, str(path), barrier, queue, kind)
        )
        for kind in ("manual", "backfill")
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(8)
        assert process.exitcode == 0
    assert sorted([queue.get(timeout=2), queue.get(timeout=2)]) == [
        "created",
        "created",
    ]
    assert store.count_active_dag_runs(dag_id="process_race") == 2


def test_daemon_keeps_lease_during_idle_interval(tmp_path):
    from easyflow._internal.scheduler import run_scheduler_loop

    path = _write(
        tmp_path / "idle.py", 'from easyflow.sdk import DAG\ndag = DAG("idle")\n'
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    errors = []

    def run():
        try:
            run_scheduler_loop(
                dag_id="idle",
                file_path=path,
                store=store,
                lease_seconds=1,
                interval_seconds=1.5,
                max_loops=2,
            )
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if store.path.exists():
            with sqlite3.connect(store.path) as db:
                try:
                    row = db.execute("SELECT owner FROM scheduler_lease").fetchone()
                except sqlite3.OperationalError:
                    row = None
            if row:
                break
        time.sleep(0.01)
    else:
        pytest.fail("scheduler 未取得租约")
    time.sleep(1.1)
    with pytest.raises(RuntimeError, match="scheduler lease"):
        run_scheduler_once(dag_id="idle", file_path=path, store=store, lease_seconds=1)
    thread.join(5)
    assert not thread.is_alive() and not errors


def test_backfill_limit_counts_its_own_runs_and_dag_limit_excludes_backfills(tmp_path):
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    dag = DAG("scope", schedule="@daily", max_active_runs=3)
    runner = PersistentDagRunner(store)
    logical = datetime(2026, 7, 1, tzinfo=UTC)
    runner.create_manual_run(dag, file_path=None, logical_date=logical, run_id="manual")
    runner.create_backfill_run(
        dag,
        file_path=None,
        interval=interval_for_logical_date("@daily", logical),
        backfill_id="b1",
        max_active_runs=1,
    )
    with pytest.raises(DagRunCapacityError, match="backfill"):
        runner.create_backfill_run(
            dag,
            file_path=None,
            interval=interval_for_logical_date("@daily", logical + timedelta(days=1)),
            backfill_id="b1",
            max_active_runs=1,
        )
    runner.create_backfill_run(
        dag,
        file_path=None,
        interval=interval_for_logical_date("@daily", logical + timedelta(days=2)),
        backfill_id="b2",
        max_active_runs=1,
    )
    runner.create_manual_run(
        dag, file_path=None, logical_date=logical, run_id="manual2"
    )
    runner.create_manual_run(
        dag, file_path=None, logical_date=logical, run_id="manual3"
    )
    with pytest.raises(DagRunCapacityError, match="DAG"):
        runner.create_manual_run(
            dag, file_path=None, logical_date=logical, run_id="manual4"
        )


def test_transaction_fence_rejects_claim_after_lease_replaced(tmp_path):
    from easyflow._internal.scheduler import scheduler_session
    from easyflow._internal.state_store import SchedulerLeaseLost

    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    with DAG("fence") as dag:
        from easyflow.providers.standard.operators.empty import EmptyOperator

        EmptyOperator(task_id="work")
    runner = PersistentDagRunner(store)
    run = runner.create_manual_run(dag, file_path=None, logical_date=datetime.now(UTC))
    with scheduler_session(store=store, owner="first", lease_seconds=10) as heartbeat:
        with sqlite3.connect(store.path) as db:
            db.execute("UPDATE scheduler_lease SET owner='replacement'")
        with pytest.raises(SchedulerLeaseLost):
            store.start_task_try("fence", run, "work", scheduler_fence=heartbeat.fence)
        with pytest.raises(SchedulerLeaseLost):
            PersistentDagRunner(
                store, scheduler_fence=heartbeat.fence
            ).create_manual_run(
                DAG("another"), file_path=None, logical_date=datetime.now(UTC)
            )
    assert store.get_task_state("fence", run, "work") is None


def test_scheduler_local_worker_receives_valid_fence(tmp_path):
    path = _write(
        tmp_path / "local_scheduled.py",
        """
from easyflow.sdk import DAG, task
with DAG("local_scheduled", schedule="@once") as dag:
    @task
    def work(): return "ok"
    work()
""",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    runs = run_scheduler_once(
        dag_id="local_scheduled",
        file_path=path,
        store=store,
        executor="local",
        lease_seconds=3,
        log_root=tmp_path / "logs",
    )
    assert len(runs) == 1 and runs[0].state.value == "success"
    assert (
        store.pull_xcom("local_scheduled", runs[0].run_id, "work", "return_value")
        == "ok"
    )


def test_local_executor_stops_launching_tasks_after_lease_loss(tmp_path):
    started, second = tmp_path / "local_started", tmp_path / "local_second"
    path = _write(
        tmp_path / "local_fenced.py",
        f"""
from pathlib import Path
import time
from easyflow.sdk import DAG, task
with DAG("local_fenced", schedule="@once") as dag:
    @task
    def first():
        Path({str(started)!r}).write_text("running")
        time.sleep(1.2)
    @task
    def last(): Path({str(second)!r}).write_text("wrong")
    first() >> last()
""",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    errors = []

    def run():
        try:
            run_scheduler_once(
                dag_id="local_fenced",
                file_path=path,
                store=store,
                lease_seconds=1,
                executor="local",
                log_root=tmp_path / "logs",
            )
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    deadline = time.monotonic() + 5
    while not started.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert started.exists()
    with sqlite3.connect(store.path) as db:
        db.execute("UPDATE scheduler_lease SET owner = 'replacement'")
    thread.join(6)
    assert not thread.is_alive() and errors and "lease" in str(errors[0])
    assert not second.exists()


def test_single_scan_cannot_disable_scheduler_lease(tmp_path):
    path = _write(
        tmp_path / "no_bypass.py",
        'from easyflow.sdk import DAG\ndag = DAG("no_bypass")\n',
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    with pytest.raises(ValueError, match="不允许禁用"):
        run_scheduler_once(
            dag_id="no_bypass", file_path=path, store=store, use_lease=False
        )


def _run_gil_holding_scheduler(url, path, logs, queue):
    try:
        result = run_scheduler_once(
            dag_id="gil_job",
            file_path=path,
            store=SQLiteStateStore(url),
            lease_seconds=1,
            log_root=logs,
        )
        queue.put([run.state.value for run in result])
    except Exception as exc:
        queue.put(str(exc))


@pytest.mark.skipif(__import__("os").name == "nt", reason="测试使用 POSIX sleep 符号")
def test_scheduler_renewal_survives_task_holding_gil(tmp_path):
    import multiprocessing

    started = tmp_path / "gil_started"
    path = _write(
        tmp_path / "gil_job.py",
        f"""
from pathlib import Path
import ctypes
from easyflow.sdk import DAG, task
with DAG("gil_job", schedule="@once") as dag:
    @task
    def work():
        Path({str(started)!r}).write_text("running")
        libc = ctypes.PyDLL(None)
        libc.sleep.argtypes = [ctypes.c_uint]
        libc.sleep(2)
    work()
""",
    )
    url = f"sqlite:///{tmp_path / 'db.sqlite'}"
    store = SQLiteStateStore(url)
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    process = ctx.Process(
        target=_run_gil_holding_scheduler,
        args=(url, str(path), str(tmp_path / "logs"), queue),
    )
    process.start()
    try:
        deadline = time.monotonic() + 6
        while not started.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert started.exists()
        time.sleep(1.25)
        assert not store.acquire_scheduler_lease(
            owner="competitor", lease_seconds=1
        ).acquired
        process.join(8)
        assert process.exitcode == 0
        assert queue.get(timeout=2) == ["success"]
    finally:
        if process.is_alive():
            process.terminate()
            process.join()


def _hold_scheduler_session(url, queue):
    from easyflow._internal.scheduler import scheduler_session

    with scheduler_session(
        store=SQLiteStateStore(url), owner="temporary", lease_seconds=1
    ) as heartbeat:
        queue.put(heartbeat.fence.owner)
        time.sleep(60)


def test_renewal_process_stops_when_scheduler_parent_dies(tmp_path):
    import multiprocessing

    url = f"sqlite:///{tmp_path / 'db.sqlite'}"
    store = SQLiteStateStore(url)
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    process = ctx.Process(target=_hold_scheduler_session, args=(url, queue))
    process.start()
    owner = queue.get(timeout=8)
    process.terminate()
    process.join(5)
    assert not process.is_alive()
    try:
        deadline = time.monotonic() + 5
        acquired = False
        while time.monotonic() < deadline:
            if store.acquire_scheduler_lease(
                owner="replacement", lease_seconds=1
            ).acquired:
                acquired = True
                break
            time.sleep(0.1)
        assert acquired
    finally:
        store.release_scheduler_lease(owner=owner)
        store.release_scheduler_lease(owner="replacement")


def test_backfill_reprocessing_rules_are_applied_in_creation_transaction(tmp_path):
    marker = tmp_path / "allowed"
    path = _write(
        tmp_path / "reprocess.py",
        f"""
from pathlib import Path
from easyflow.sdk import DAG, task
with DAG("reprocess", schedule="@daily") as dag:
    @task
    def work():
        if not Path({str(marker)!r}).exists(): raise RuntimeError("first fails")
        return "ok"
    work()
""",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    dag = load_dag_from_file(path, "reprocess")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run = runner.create_manual_run(
        dag, file_path=path, logical_date=datetime(2026, 7, 1, tzinfo=UTC)
    )
    assert runner.run_existing(dag, run_id=run, file_path=path).state.value == "failed"
    marker.write_text("ok")

    def backfill(behavior):
        return create_backfill(
            dag_id="reprocess",
            file_path=path,
            store=store,
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 1),
            reprocessing_behavior=behavior,
            log_root=tmp_path / "logs",
        )

    assert backfill("none").runs == []
    assert [item.state.value for item in backfill("failed").runs] == ["success"]
    assert backfill("failed").runs == []
    assert [item.state.value for item in backfill("completed").runs] == ["success"]


def _compete_for_backfill_date(url, path, barrier, queue, backfill_id):
    from easyflow._internal.state_store import BackfillRunNotEligible

    dag = load_dag_from_file(path, "backfill_race")
    runner = PersistentDagRunner(SQLiteStateStore(url))
    interval = interval_for_logical_date("@daily", datetime(2026, 7, 1, tzinfo=UTC))
    barrier.wait()
    try:
        runner.create_backfill_run(
            dag,
            file_path=path,
            interval=interval,
            backfill_id=backfill_id,
            reprocessing_behavior="none",
        )
        queue.put("created")
    except BackfillRunNotEligible:
        queue.put("skipped")


def test_two_backfills_do_not_race_reprocessing_none(tmp_path):
    import multiprocessing

    path = _write(
        tmp_path / "backfill_race.py",
        'from easyflow.sdk import DAG\ndag = DAG("backfill_race", schedule="@daily", max_active_runs=2)\n',
    )
    url = f"sqlite:///{tmp_path / 'db.sqlite'}"
    store = SQLiteStateStore(url)
    store.init_db()
    ctx = multiprocessing.get_context("spawn")
    barrier, queue = ctx.Barrier(2), ctx.Queue()
    processes = [
        ctx.Process(
            target=_compete_for_backfill_date,
            args=(url, str(path), barrier, queue, name),
        )
        for name in ("b1", "b2")
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(8)
        assert process.exitcode == 0
    assert sorted([queue.get(timeout=2), queue.get(timeout=2)]) == [
        "created",
        "skipped",
    ]
    assert store.count_dag_runs(dag_id="backfill_race", run_type="backfill") == 1
