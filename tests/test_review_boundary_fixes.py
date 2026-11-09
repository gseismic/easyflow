from __future__ import annotations

import hashlib
import os
import py_compile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from easyflow._internal.dag_parser import load_dag_from_file, load_dag_snapshot
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.scheduler import preview_backfill
from easyflow._internal.schedules import backfill_intervals
from easyflow._internal.state_store import SQLiteStateStore, TaskCapacityFull
from easyflow.cli import main
from easyflow.providers.standard.operators.empty import EmptyOperator
from easyflow.sdk import DAG
from easyflow.sdk.states import TaskInstanceState

UTC = timezone.utc
NOW = datetime(2026, 7, 1, tzinfo=UTC)


@pytest.mark.parametrize("namespace", [False, True])
def test_local_dependencies_ignore_stale_pyc_and_refresh_cached_packages(
    tmp_path, monkeypatch, namespace
):
    monkeypatch.syspath_prepend(str(tmp_path))
    package_name = "boundary_namespace" if namespace else "boundary_package"
    package = tmp_path / package_name
    package.mkdir()
    if not namespace:
        (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "wrapper.py").write_text("from .helper import work\n", encoding="utf-8")
    helper = package / "helper.py"
    old_source = 'def work():\n    return "OLD"\n'
    new_source = old_source.replace("OLD", "NEW")
    helper.write_text(old_source, encoding="utf-8")
    py_compile.compile(str(helper), doraise=True)
    path = tmp_path / "dag.py"
    path.write_text(
        "from easyflow.sdk import DAG\n"
        "from easyflow.providers.standard.operators.python import PythonOperator\n"
        f"from {package_name} import wrapper\n"
        'with DAG("dependencies") as dag:\n'
        '    PythonOperator(task_id="work", python_callable=wrapper.work)\n',
        encoding="utf-8",
    )
    old_dag = load_dag_from_file(path, "dependencies")
    stat = helper.stat()
    helper.write_text(new_source, encoding="utf-8")
    os.utime(helper, ns=(stat.st_atime_ns, stat.st_mtime_ns))

    dag = load_dag_from_file(path, "dependencies")
    assert dag.test(logical_date=NOW).task_results["work"] == "NEW"
    assert old_dag.test(logical_date=NOW).task_results["work"] == "OLD"
    expected = hashlib.sha256(new_source.encode()).hexdigest()
    assert dag._easyflow_dependency_hashes[str(helper.resolve())] == expected
    snapshot = load_dag_snapshot(path, "dependencies")
    assert snapshot._easyflow_dependency_hashes == dag._easyflow_dependency_hashes

    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(snapshot, file_path=path, logical_date=NOW)
    assert (
        runner.run_existing(snapshot, run_id=run_id, file_path=path).state.value
        == "success"
    )
    assert store.pull_xcom("dependencies", run_id, "work", "return_value") == "NEW"


def test_dependency_digest_records_loaded_bytes_even_if_file_changes_during_import(
    tmp_path, monkeypatch
):
    monkeypatch.syspath_prepend(str(tmp_path))
    helper = tmp_path / "boundary_self_change.py"
    replacement = 'def work():\n    return "NEW"\n'
    source = (
        "from pathlib import Path\n"
        'def work():\n    return "OLD"\n'
        f"Path(__file__).write_text({replacement!r}, encoding='utf-8')\n"
    )
    helper.write_text(source, encoding="utf-8")
    path = tmp_path / "dag.py"
    path.write_text(
        "from easyflow.sdk import DAG\n"
        "from easyflow.providers.standard.operators.python import PythonOperator\n"
        "from boundary_self_change import work\n"
        'with DAG("changed") as dag:\n'
        '    PythonOperator(task_id="work", python_callable=work)\n',
        encoding="utf-8",
    )
    dag = load_dag_from_file(path, "changed")
    assert (
        dag._easyflow_dependency_hashes[str(helper.resolve())]
        == hashlib.sha256(source.encode()).hexdigest()
    )
    assert helper.read_text() == replacement
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(dag, file_path=path, logical_date=NOW)
    result = runner.run_existing(dag, run_id=run_id, file_path=path)
    assert result.state.value == "failed"
    assert result.task_states["work"] is None


def test_resuming_local_run_counts_existing_lease_and_continues_after_completion(
    tmp_path,
):
    path = tmp_path / "dag.py"
    path.write_text(
        "from easyflow.sdk import DAG\n"
        "from easyflow.providers.standard.operators.empty import EmptyOperator\n"
        'with DAG("limited", max_active_tasks=1) as dag:\n'
        '    EmptyOperator(task_id="a_existing")\n'
        '    EmptyOperator(task_id="b_new")\n',
        encoding="utf-8",
    )
    dag = load_dag_snapshot(path, "limited")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    store.configure_parallelism(2)
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(dag, file_path=path, logical_date=NOW)
    store.start_task_try("limited", run_id, "a_existing", lease_seconds=600)
    result = runner.run_existing(
        dag, run_id=run_id, file_path=path, executor="local", parallelism=2
    )
    assert result.state.value == "running"
    assert result.task_states == {
        "a_existing": TaskInstanceState.RUNNING,
        "b_new": None,
    }
    store.commit_task_terminal(
        dag_id="limited",
        run_id=run_id,
        task_id="a_existing",
        try_number=1,
        xcom_values={},
    )
    assert (
        runner.run_existing(
            dag, run_id=run_id, file_path=path, executor="local", parallelism=2
        ).state.value
        == "success"
    )


def test_run_task_capacity_is_atomic_and_independent_of_other_runs(tmp_path):
    with DAG("limited", max_active_runs=2, max_active_tasks=1) as dag:
        EmptyOperator(task_id="a")
        EmptyOperator(task_id="b")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    store.configure_parallelism(3)
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(dag, file_path=None, logical_date=NOW)
    other_run = runner.create_manual_run(
        dag, file_path=None, logical_date=NOW, run_id="other"
    )
    barrier = threading.Barrier(2)

    def claim(task_id):
        barrier.wait(timeout=5)
        try:
            return store.start_task_try("limited", run_id, task_id, max_active_tasks=1)
        except TaskCapacityFull:
            return "full"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(claim, ("a", "b")))
    assert results.count(1) == results.count("full") == 1
    assert store.start_task_try("limited", other_run, "a", max_active_tasks=1) == 1
    blocked_task = "a" if results[0] == "full" else "b"
    # 只有过期租约被排除，其他运行中的任务不占用本运行的容量。
    assert (
        store.start_task_try(
            "limited",
            run_id,
            blocked_task,
            max_active_tasks=1,
            now=datetime.now(UTC) + timedelta(minutes=1),
        )
        == 1
    )


def test_resuming_local_run_releases_expired_task_capacity(tmp_path):
    path = tmp_path / "dag.py"
    path.write_text(
        "from easyflow.sdk import DAG\n"
        "from easyflow.providers.standard.operators.empty import EmptyOperator\n"
        'with DAG("expired", max_active_tasks=1) as dag:\n'
        '    EmptyOperator(task_id="a_lost")\n'
        '    EmptyOperator(task_id="b_ready")\n',
        encoding="utf-8",
    )
    dag = load_dag_snapshot(path, "expired")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    store.configure_parallelism(2)
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(dag, file_path=path, logical_date=NOW)
    store.start_task_try(
        "expired", run_id, "a_lost", now=datetime.now(UTC) - timedelta(minutes=2)
    )
    result = runner.run_existing(
        dag, run_id=run_id, file_path=path, executor="local", parallelism=2
    )
    assert result.state.value == "failed"
    assert result.task_states == {
        "a_lost": TaskInstanceState.FAILED,
        "b_ready": TaskInstanceState.SUCCESS,
    }


def test_backfill_accepts_precise_ranges_for_minute_and_microsecond_schedules(tmp_path):
    start = datetime(2026, 7, 1, tzinfo=timezone(timedelta(hours=8)))
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    minute = DAG("minute", schedule="* * * * *")
    preview = preview_backfill(
        dag=minute, store=store, start_date=start, end_date=start + timedelta(minutes=9)
    )
    assert len(preview) == 10
    assert preview[0].logical_date == start
    assert preview[-1].logical_date == start + timedelta(minutes=9)
    mixed = backfill_intervals(
        minute.schedule, start.date(), start + timedelta(minutes=1), start.tzinfo
    )
    assert [interval.logical_date for interval in mixed] == [
        start,
        start + timedelta(minutes=1),
    ]
    micro = DAG("micro", schedule=timedelta(microseconds=10), start_date=start)
    preview = preview_backfill(
        dag=micro,
        store=store,
        start_date=start,
        end_date=start + timedelta(microseconds=30),
    )
    assert len(preview) == 4
    assert preview[-1].logical_date == start + timedelta(microseconds=30)
    with pytest.raises(ValueError, match="最多支持 1000 个区间"):
        preview_backfill(
            dag=minute,
            store=store,
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 1),
        )
    with pytest.raises(ValueError, match="必须带时区"):
        backfill_intervals(minute.schedule, datetime(2026, 7, 1), start)
    with pytest.raises(ValueError, match="end-date"):
        backfill_intervals(minute.schedule, start + timedelta(seconds=1), start)


def test_precise_backfill_range_compares_dst_folds_by_actual_instant():
    zone = ZoneInfo("America/New_York")
    start = datetime(2026, 11, 1, 1, 30, tzinfo=zone, fold=0)
    end = datetime(2026, 11, 1, 1, 30, tzinfo=zone, fold=1)
    intervals = backfill_intervals(
        timedelta(minutes=30), start, end, zone, anchor=start
    )
    assert [interval.logical_date.astimezone(UTC).hour for interval in intervals] == [
        5,
        6,
        6,
    ]
    assert intervals[-1].logical_date.astimezone(UTC) == end.astimezone(UTC)


def test_cli_precise_backfill_preview_and_creation_use_same_inclusive_range(
    tmp_path, capsys
):
    path = tmp_path / "dag.py"
    path.write_text(
        'from easyflow.sdk import DAG\ndag = DAG("minute", schedule="* * * * *")\n',
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    arguments = [
        "backfill",
        "create",
        "--dag-id",
        "minute",
        "--file",
        str(path),
        "--start-date",
        "2026-07-01T00:00:00+08:00",
        "--end-date",
        "2026-07-01T00:02:00+08:00",
        "--database-url",
        store.database_url,
        "--log-root",
        str(tmp_path / "logs"),
    ]
    assert main([*arguments, "--dry-run"]) == 0
    assert capsys.readouterr().out.count("\tcreate\t") == 3
    assert store.count_dag_runs(dag_id="minute") == 0
    assert main(arguments) == 0
    assert store.count_dag_runs(dag_id="minute", run_type="backfill") == 3
