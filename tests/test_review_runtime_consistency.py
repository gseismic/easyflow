from __future__ import annotations

import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest

from easyflow._internal import scheduler
from easyflow._internal.dag_parser import (
    AsyncDagDiscovery,
    DagSource,
    DagSourceChanged,
    load_dag_from_file,
    load_dag_snapshot,
)
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.schedules import interval_for_logical_date
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.cli import main
from easyflow.providers.standard.operators.empty import EmptyOperator
from easyflow.sdk import DAG, task

NOW = datetime(2026, 7, 1, tzinfo=UTC)


def test_scheduled_creation_rejects_stale_source_without_consuming_date(tmp_path, monkeypatch):
    helper = tmp_path / "review_config.py"
    helper.write_text("VALUE = 'v1'\n", encoding="utf-8")
    source = _source(
        tmp_path / "scheduled.py", "fresh",
        """from review_config import VALUE
from easyflow.sdk import DAG, task
with DAG('fresh', schedule='@daily') as dag:
    @task
    def work():
        return VALUE
    work()
""",
    )
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    interval = interval_for_logical_date(source.dag.schedule, NOW)
    discovery = AsyncDagDiscovery(tmp_path, file_path=source.file_path)
    try:
        before = discovery.scan(wait_seconds=2).sources[0]
        create_run = store.create_dag_run

        def edit_at_write(**kwargs):
            helper.write_text("VALUE = 'v2'\n", encoding="utf-8")
            return create_run(**kwargs)

        with monkeypatch.context() as context:
            context.setattr(store, "create_dag_run", edit_at_write)
            with pytest.raises(DagSourceChanged, match="本地依赖"):
                runner.create_scheduled_run(before.dag, file_path=before.file_path, interval=interval)
        assert store.latest_automated_logical_date("fresh") is None
        with (
            scheduler.scheduler_session(store=store, owner=None, lease_seconds=10) as heartbeat,
            scheduler.SchedulerCoordinator(store, heartbeat, tmp_path / "logs", "local", 1) as coordinator,
        ):
            current = discovery.scan(wait_seconds=2)
            assert not current.pending
            coordinator.submit(current.sources, NOW)
            summaries, issues = coordinator.collect(wait=True)
    finally:
        discovery.close()
    assert not issues and len(summaries) == 1
    assert summaries[0].state.value == "success"
    assert store.pull_xcom("fresh", summaries[0].run_id, "work", "return_value") == "v2"
    assert store.latest_automated_logical_date("fresh") == NOW


@pytest.mark.parametrize("active_type", ["manual", "backfill"])
def test_saturated_scheduled_capacity_skips_catchup_expansion(tmp_path, monkeypatch, active_type):
    with DAG("catchup", schedule="* * * * *", start_date=NOW - timedelta(days=5), catchup=True) as dag:
        EmptyOperator(task_id="work")
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    interval = interval_for_logical_date(dag.schedule, NOW + timedelta(days=1))
    if active_type == "manual":
        active = runner.create_manual_run(dag, file_path=None, logical_date=interval.logical_date)
    else:
        active = runner.create_backfill_run(dag, file_path=None, interval=interval, backfill_id="backfill")
    path = tmp_path / "catchup.py"
    path.touch()
    expanded = []
    def one_interval(dag, now, **kwargs):
        expanded.append(dag.dag_id)
        return [interval_for_logical_date(dag.schedule, now)]
    monkeypatch.setattr(scheduler, "scheduler_intervals", one_interval)
    run_ids, _ = scheduler._scan_scheduled_runs(DagSource(dag.dag_id, path, dag), runner, NOW)
    assert run_ids[0] == active
    if active_type == "manual":
        assert expanded == [] and run_ids == [active]
    else:
        assert expanded == [dag.dag_id] and len(run_ids) == 2
        assert store.latest_automated_logical_date(dag.dag_id) == NOW


def test_orphan_recovery_writes_only_for_expired_candidates(tmp_path, monkeypatch):
    with DAG("leases") as dag:
        for i in range(500):
            EmptyOperator(task_id=f"task_{i}", retries=1)
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(dag, file_path=None, logical_date=NOW)
    store.configure_parallelism(2)
    store.start_task_try(dag.dag_id, run_id, "task_0", now=NOW, lease_seconds=5)
    store.start_task_try(dag.dag_id, run_id, "task_1", now=NOW, lease_seconds=20)
    statements = []
    original_connect = store._connect
    @contextmanager
    def traced_connect():
        with original_connect() as connection:
            connection.set_trace_callback(statements.append)
            yield connection
    monkeypatch.setattr(store, "_connect", traced_connect)
    assert not store.recover_orphaned_tasks_for_run(dag=dag, run_id=run_id, now=NOW)
    assert statements.count("BEGIN IMMEDIATE") == 0
    recovered = store.recover_orphaned_tasks_for_run(
        dag=dag, run_id=run_id, now=NOW + timedelta(seconds=5)
    )
    assert statements.count("BEGIN IMMEDIATE") == 1
    assert list(recovered) == ["task_0"]
    assert recovered["task_0"].try_number == 1
    assert recovered["task_0"].state.value == "up_for_retry"
    assert store.get_task_state(dag.dag_id, run_id, "task_1").value == "running"


def _source(path, dag_id, source):
    path.write_text(source, encoding="utf-8")
    return DagSource(dag_id, path, load_dag_snapshot(path, dag_id))


def _wait_for_file(path):
    deadline = time.monotonic() + 12
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert path.exists(), f"任务未写入 {path.name}"


def test_daemon_scans_next_interval_with_same_dag_run_in_progress(tmp_path):
    first_started = tmp_path / "first-started"
    release = tmp_path / "release"
    source = _source(
        tmp_path / "intervals.py",
        "intervals",
        f"""
from datetime import timedelta
from pathlib import Path
import time
from easyflow.sdk import DAG, task

with DAG("intervals", schedule=timedelta(hours=1), max_active_runs=2, catchup=False) as dag:
    @task
    def work(logical_date):
        if logical_date.hour == 0:
            Path({str(first_started)!r}).touch()
            deadline = time.monotonic() + 20
            while not Path({str(release)!r}).exists():
                if time.monotonic() > deadline: raise RuntimeError("first run timed out")
                time.sleep(0.02)
    work()
""",
    )
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    following = NOW + timedelta(hours=1)
    first_id = f"scheduled__{NOW.isoformat()}"
    second_id = f"scheduled__{following.isoformat()}"
    with (
        scheduler.scheduler_session(
            store=store, owner=None, lease_seconds=10
        ) as heartbeat,
        scheduler.SchedulerCoordinator(
            store, heartbeat, tmp_path / "logs", "local", 2
        ) as coordinator,
    ):
        try:
            coordinator.submit([source], NOW)
            _wait_for_file(first_started)
            coordinator.submit([source], following)
            coordinator.submit([source], following)
            assert store.get_dag_run("intervals", second_id) is not None
            assert store.get_dag_run("intervals", first_id).state == "running"
            assert store.count_dag_runs(dag_id="intervals", run_type="scheduled") == 2
        finally:
            release.touch()
        results, issues = coordinator.collect(wait=True)
    assert not issues
    assert [(item.run_id, item.state.value) for item in results] == [
        (first_id, "success"),
        (second_id, "success"),
    ]


def test_clear_preserves_xcom_for_previously_dispatched_downstream(tmp_path):
    with DAG("dispatched") as dag:

        @task
        def first():
            return "original"

        @task
        def second(value):
            return value

        second(first())

    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(dag, file_path=None, logical_date=NOW)
    arguments = {"dag_id": dag.dag_id, "run_id": run_id, "task_ids": ["first"]}
    attempt = store.start_task_try(dag.dag_id, run_id, "first")
    store.commit_task_terminal(
        dag_id=dag.dag_id,
        run_id=run_id,
        task_id="first",
        try_number=attempt,
        xcom_values={"return_value": "original"},
    )
    # 即使调度器尚未完成入队，也不能与它已读取的任务状态并发清理。
    with store.run_lock(dag.dag_id, run_id):
        assert store.clear_task_instances(**arguments).blocked
    assert store.queue_task(dag.dag_id, run_id, "second")
    assert store.preview_clear_task_instances(**arguments).blocked
    assert store.clear_task_instances(**arguments).blocked
    assert store.pull_xcom(dag.dag_id, run_id, "first", "return_value") == "original"
    assert runner.run_existing(dag, run_id=run_id).state.value == "success"
    assert store.pull_xcom(dag.dag_id, run_id, "second", "return_value") == "original"


def test_local_constant_import_is_scoped_and_fingerprinted(tmp_path):
    dags = tmp_path / "dags"
    dags.mkdir()
    helper = dags / "review_settings.py"
    helper.write_text('LABEL = "v1"\n', encoding="utf-8")
    source = _source(
        dags / "flow.py",
        "local_constants",
        """
from easyflow.sdk import DAG, task
from review_settings import LABEL

with DAG("local_constants") as dag:
    @task
    def sample():
        return LABEL
    sample()
""",
    )
    assert str(dags) not in sys.path
    assert str(helper) in source.dag._easyflow_dependency_hashes
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_arguments = {"file_path": source.file_path, "logical_date": NOW}
    runner.create_manual_run(source.dag, run_id="original", **run_arguments)
    assert (
        runner.run_existing(
            source.dag,
            run_id="original",
            file_path=source.file_path,
            executor="local",
        ).state.value
        == "success"
    )
    assert store.pull_xcom(source.dag_id, "original", "sample", "return_value") == "v1"

    runner.create_manual_run(source.dag, run_id="old", **run_arguments)
    helper.write_text('LABEL = "v2"\n', encoding="utf-8")
    current = load_dag_snapshot(source.file_path, source.dag_id)
    assert current._easyflow_dependency_hashes != source.dag._easyflow_dependency_hashes
    assert (
        runner.run_existing(
            current,
            run_id="old",
            file_path=source.file_path,
            executor="local",
        ).state.value
        == "failed"
    )
    assert store.get_task_state(source.dag_id, "old", "sample") is None

    other = tmp_path / "other"
    other.mkdir()
    (other / helper.name).write_text('LABEL = "other"\n', encoding="utf-8")
    other_file = other / "flow.py"
    other_file.write_bytes(source.file_path.read_bytes())
    first_loaded = load_dag_from_file(source.file_path, source.dag_id)
    other_loaded = load_dag_from_file(other_file, source.dag_id)
    assert first_loaded.test(NOW).task_results["sample"] == "v2"
    assert other_loaded.test(NOW).task_results["sample"] == "other"
    assert str(dags) not in sys.path and str(other) not in sys.path


def test_late_heartbeat_cannot_reclaim_a_slot(tmp_path):
    with DAG("leases") as dag:
        for name in ["first", "next"]:
            task(task_id=name)(lambda: None)()
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    run_id = PersistentDagRunner(store).create_manual_run(
        dag,
        file_path=None,
        logical_date=NOW,
    )
    assert (
        store.start_task_try(dag.dag_id, run_id, "first", now=NOW, lease_seconds=10)
        == 1
    )
    renew = {
        "dag_id": dag.dag_id,
        "run_id": run_id,
        "task_id": "first",
        "try_number": 1,
    }
    assert store.renew_task_lease(
        **renew, now=NOW + timedelta(seconds=9), lease_seconds=10
    )
    deadline = NOW + timedelta(seconds=19)
    assert store.start_task_try(dag.dag_id, run_id, "next", now=deadline) == 1
    for when in (deadline, deadline + timedelta(microseconds=1)):
        assert not store.renew_task_lease(**renew, now=when)
        with sqlite3.connect(store.path) as connection:
            assert connection.execute(
                "SELECT task_id FROM task_instance WHERE state = 'running' "
                "AND lease_expires_at > ?",
                (when.isoformat(),),
            ).fetchall() == [("next",)]


def test_task_exit_code_75_uses_task_failure_and_retry_not_scheduler_lease(tmp_path):
    marker = tmp_path / "retry-marker"
    source = _source(
        tmp_path / "exit.py",
        "exiting",
        f"""
from datetime import timedelta
import os
from pathlib import Path
from easyflow.sdk import DAG, task

with DAG("exiting", schedule="@once") as dag:
    @task(retries=1, retry_delay=timedelta(0))
    def hard_exit():
        marker = Path({str(marker)!r})
        if not marker.exists():
            marker.touch()
            os._exit(75)
        return "retried"
    @task
    def soft_exit():
        raise SystemExit(75)
    @task
    def independent():
        return "finished"
    hard_exit()
    soft_exit()
    independent()
""",
    )
    other = _source(
        tmp_path / "other.py",
        "other",
        'from easyflow.sdk import DAG\ndag = DAG("other", schedule="@once")\n',
    )
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    with (
        scheduler.scheduler_session(
            store=store, owner=None, lease_seconds=10
        ) as heartbeat,
        scheduler.SchedulerCoordinator(
            store, heartbeat, tmp_path / "logs", "local", 3
        ) as coordinator,
    ):
        coordinator.submit([source], NOW)
        runs, issues = coordinator.collect(wait=True)
        assert not issues
        run = runs[0]
        assert run.state.value == "failed"
        assert (
            store.pull_xcom("exiting", run.run_id, "hard_exit", "return_value")
            == "retried"
        )
        assert (
            store.pull_xcom("exiting", run.run_id, "independent", "return_value")
            == "finished"
        )
        with sqlite3.connect(store.path) as connection:
            failed = connection.execute(
                "SELECT task_id, error_message FROM task_try WHERE run_id = ? AND state = 'failed'",
                (run.run_id,),
            ).fetchall()
        assert {task_id for task_id, _ in failed} == {"soft_exit", "hard_exit"}
        assert all("75" in message for _, message in failed)
        coordinator.submit([other], NOW)
        runs, issues = coordinator.collect(wait=True)
        assert not issues and runs[0].state.value == "success"


@pytest.mark.parametrize("fail", [False, True])
def test_cli_trigger_waits_for_run_taken_by_scheduler(
    tmp_path, monkeypatch, capsys, fail
):
    started = tmp_path / "task-starts"
    release = tmp_path / "release"
    source = _source(
        tmp_path / "trigger.py",
        "trigger",
        f"""
from pathlib import Path
import time
from easyflow.sdk import DAG, task

with DAG("trigger") as dag:
    @task
    def work():
        with Path({str(started)!r}).open("a") as marker:
            marker.write("started\\n")
        deadline = time.monotonic() + 20
        while not Path({str(release)!r}).exists():
            if time.monotonic() > deadline: raise RuntimeError("trigger timed out")
            time.sleep(0.02)
        if {fail}: raise RuntimeError("task failure")
    work()
""",
    )
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    original_create = PersistentDagRunner.create_manual_run
    original_lock = SQLiteStateStore.run_lock
    contended = threading.Event()

    @contextmanager
    def observed_lock(self, dag_id, run_id):
        with original_lock(self, dag_id, run_id) as acquired:
            if not acquired:
                contended.set()
            yield acquired

    monkeypatch.setattr(SQLiteStateStore, "run_lock", observed_lock)
    with ThreadPoolExecutor(max_workers=2) as pool:
        scheduled = []

        def create_and_schedule(self, dag, **options):
            run_id = original_create(self, dag, **options)
            scheduled.append(
                pool.submit(
                    scheduler.run_scheduler_once_for_sources,
                    sources=[source],
                    store=store,
                    now=NOW,
                    log_root=tmp_path / "logs",
                )
            )
            _wait_for_file(started)
            return run_id

        monkeypatch.setattr(
            PersistentDagRunner, "create_manual_run", create_and_schedule
        )
        cli = pool.submit(
            main,
            [
                "dags",
                "trigger",
                "trigger",
                "--file",
                str(source.file_path),
                "--run-id",
                "contended",
                "--database-url",
                store.database_url,
                "--log-root",
                str(tmp_path / "logs"),
            ],
        )
        try:
            assert contended.wait(15), "CLI 未遇到运行锁竞争"
            assert not cli.done()
        finally:
            release.touch()
        assert cli.result(timeout=15) == int(fail)
        runs, issues = scheduled[0].result(timeout=15)
    state = "failed" if fail else "success"
    assert not issues and runs[0].state.value == state
    assert f"trigger contended {state}" in capsys.readouterr().out
    assert started.read_text(encoding="utf-8").splitlines() == ["started"]


def test_reads_use_committed_snapshot_while_writer_holds_lock(tmp_path):
    with DAG("reads") as dag:
        task(task_id="work")(lambda: None)()
    store = SQLiteStateStore(str(tmp_path / "nested" / "state.db"))
    run_id = PersistentDagRunner(store).create_manual_run(
        dag,
        file_path=None,
        logical_date=NOW,
    )
    reader = SQLiteStateStore(store.database_url)

    def read():
        return reader.get_dag_run(dag.dag_id, run_id).state, reader.list_task_states(
            dag.dag_id,
            run_id,
        )

    with ThreadPoolExecutor() as pool, sqlite3.connect(store.path) as writer:
        assert writer.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE dag_run SET state = 'running'")
        writer.execute("UPDATE task_instance SET state = 'running'")
        assert pool.submit(read).result(timeout=2) == ("queued", {"work": None})


def test_sync_trigger_waits_to_recover_an_unfinished_attempt(tmp_path, monkeypatch):
    source = _source(
        tmp_path / "recovering.py",
        "recovering",
        """
from easyflow.sdk import DAG, task
with DAG("recovering") as dag:
    @task(retries=1)
    def work():
        return "recovered"
    work()
""",
    )
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    original_create = PersistentDagRunner.create_manual_run

    def create_with_unfinished_attempt(self, dag, **options):
        run_id = original_create(self, dag, **options)
        self.store.start_task_try(dag.dag_id, run_id, "work", lease_seconds=2)
        return run_id

    monkeypatch.setattr(
        PersistentDagRunner, "create_manual_run", create_with_unfinished_attempt
    )
    assert (
        main(
            [
                "dags",
                "trigger",
                "recovering",
                "--file",
                str(source.file_path),
                "--run-id",
                "interrupted",
                "--database-url",
                store.database_url,
                "--log-root",
                str(tmp_path / "logs"),
            ]
        )
        == 0
    )
    assert (
        store.pull_xcom("recovering", "interrupted", "work", "return_value")
        == "recovered"
    )
    with sqlite3.connect(store.path) as connection:
        assert connection.execute(
            "SELECT state FROM task_try WHERE run_id = 'interrupted' ORDER BY try_number"
        ).fetchall() == [("failed",), ("success",)]


@pytest.mark.parametrize("group", ["dag", "backfill", "legacy-backfill"])
def test_clear_preview_and_execution_share_capacity_check(tmp_path, group):
    with DAG("capacity", max_active_runs=1) as dag:
        task(task_id="work")(lambda: "saved")()
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    if group != "dag":
        store.create_backfill(
            backfill_id="group",
            dag_id=dag.dag_id,
            start_logical_date=NOW,
            end_logical_date=NOW + timedelta(days=1),
            reprocessing_behavior="none",
            max_active_runs=1,
        )

    def create(when):
        if group == "dag":
            return runner.create_manual_run(dag, file_path=None, logical_date=when)
        return runner.create_backfill_run(
            dag,
            file_path=None,
            interval=interval_for_logical_date("@daily", when),
            backfill_id="group",
            max_active_runs=1,
        )

    first = create(NOW)
    assert runner.run_existing(dag, run_id=first).state.value == "success"
    if group == "legacy-backfill":
        with sqlite3.connect(store.path) as connection:
            connection.execute("UPDATE dag_run SET backfill_max_active_runs = NULL")
    arguments = {"dag_id": dag.dag_id, "run_id": first, "task_ids": ["work"]}
    assert not store.preview_clear_task_instances(**arguments).blocked
    next_run = create(NOW + timedelta(days=1))
    rejected = store.clear_task_instances(**arguments)
    assert rejected.blocked and "max_active_runs=1" in rejected.blocked[0].reason
    assert store.preview_clear_task_instances(**arguments).blocked == rejected.blocked
    assert store.pull_xcom(dag.dag_id, first, "work", "return_value") == "saved"
    assert runner.run_existing(dag, run_id=next_run).state.value == "success"
    assert not store.preview_clear_task_instances(**arguments).blocked
    assert not store.clear_task_instances(**arguments).blocked
    assert store.get_dag_run(dag.dag_id, first).state == "queued"


def test_explicit_clear_rejects_all_when_any_task_is_missing(tmp_path):
    with DAG("typo") as dag:
        task(task_id="work")(lambda: "saved")()
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run = runner.create_manual_run(dag, file_path=None, logical_date=NOW)
    assert runner.run_existing(dag, run_id=run).state.value == "success"
    arguments = {"dag_id": dag.dag_id, "run_id": run, "task_ids": ["work", "wrk"]}
    for check in (store.preview_clear_task_instances, store.clear_task_instances):
        result = check(**arguments)
        assert result.to_clear == []
        assert [item.task_id for item in result.blocked] == ["wrk"]
    assert store.get_task_state(dag.dag_id, run, "work").value == "success"
    assert store.pull_xcom(dag.dag_id, run, "work", "return_value") == "saved"


def test_paused_backfill_consumes_only_first_batch_and_one_interval(
    tmp_path, monkeypatch
):
    source = _source(
        tmp_path / "seconds.py",
        "seconds",
        "from datetime import timedelta\nfrom easyflow.sdk import DAG\n"
        'dag = DAG("seconds", schedule=timedelta(seconds=1))\n',
    )
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.set_dag_paused("seconds", True)
    original = scheduler.iter_backfill_intervals
    consumed = []

    def counted(*arguments):
        for interval in original(*arguments):
            consumed.append(interval.logical_date)
            assert len(consumed) <= 2, "首批暂停后继续遍历了剩余区间"
            yield interval

    monkeypatch.setattr(scheduler, "iter_backfill_intervals", counted)
    end = NOW + timedelta(days=30, seconds=-1)
    arguments = {
        "dag_id": "seconds",
        "file_path": source.file_path,
        "store": store,
        "log_root": tmp_path / "logs",
    }
    paused = scheduler.create_backfill(**arguments, start_date=NOW, end_date=end)
    second = NOW + timedelta(seconds=1)
    assert not paused.complete and paused.pending == (NOW,)
    assert consumed == [NOW, second] and paused.pending_range == (second, end)
    assert store.count_dag_runs(dag_id="seconds", run_type="backfill") == 1

    monkeypatch.setattr(scheduler, "iter_backfill_intervals", original)
    store.set_dag_paused("seconds", False)
    dates = [NOW + timedelta(seconds=i) for i in range(1, 5)]
    finished = scheduler.create_backfill(
        **arguments,
        start_date=dates[0],
        end_date=dates[-1],
        max_active_runs=2,
    )
    assert finished.complete and not finished.pending and finished.pending_range is None
    assert [
        store.get_dag_run("seconds", item.run_id).logical_date for item in finished.runs
    ] == [value.isoformat() for value in dates]


def test_multiple_outputs_keep_the_full_mapping_in_return_value(tmp_path):
    output = {"return_value": 7, "batch": "demo"}
    with DAG("outputs") as dag:

        @task(multiple_outputs=True)
        def produce():
            return output

        @task
        def whole(value):
            return value

        @task
        def batch(value):
            return value

        data = produce()
        whole(data)
        batch(data["batch"])

    result = dag.test(NOW)
    assert (
        result.task_results["whole"] == output
        and result.task_results["batch"] == "demo"
    )
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(dag, file_path=None, logical_date=NOW)
    assert runner.run_existing(dag, run_id=run_id).state.value == "success"
    assert store.pull_xcom(dag.dag_id, run_id, "whole", "return_value") == output
    assert store.pull_xcom(dag.dag_id, run_id, "batch", "return_value") == "demo"


@pytest.mark.parametrize("args", [["typo"], ["dags", "list", "--wrong-option"]])
def test_cli_usage_errors_show_message_without_traceback(args):
    result = subprocess.run(
        [sys.executable, "-m", "easyflow.cli", *args],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 2
    assert args[-1] in result.stderr and "Usage" in result.stderr
    assert "Traceback" not in result.stderr and "Traceback" not in result.stdout
