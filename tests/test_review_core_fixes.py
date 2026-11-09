from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from easyflow._internal.dag_parser import load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.scheduler import (
    create_backfill,
    preview_backfill,
    run_scheduler_once,
)
from easyflow._internal.schedules import interval_for_logical_date
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.cli import main
from easyflow.providers.standard.operators.empty import EmptyOperator
from easyflow.sdk import DAG
from easyflow.sdk.states import TaskInstanceState
from easyflow.web.service import WebService

UTC = timezone.utc


def test_future_backfill_does_not_advance_scheduled_cursor(tmp_path):
    path = tmp_path / "daily.py"
    path.write_text(
        "from datetime import datetime, timezone\n"
        "from easyflow.sdk import DAG\n"
        'dag = DAG("daily", schedule="@daily", catchup=True, '
        "start_date=datetime(2026, 7, 1, tzinfo=timezone.utc))\n",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    backfill = create_backfill(
        dag_id="daily",
        file_path=path,
        store=store,
        start_date=date(2026, 7, 10),
        end_date=date(2026, 7, 10),
        log_root=tmp_path / "logs",
    )
    assert backfill.complete and backfill.runs[0].state.value == "success"
    assert store.latest_automated_logical_date("daily") is None
    service = WebService(str(tmp_path), store.database_url, str(tmp_path / "logs"))
    try:
        assert (
            service.workflow_page_details(["daily"])["daily"]["last_automated"] is None
        )
    finally:
        service.discovery.close()

    run_scheduler_once(
        dag_id="daily",
        file_path=path,
        store=store,
        now=datetime(2026, 7, 3, tzinfo=UTC),
        log_root=tmp_path / "logs",
    )
    with sqlite3.connect(store.path) as connection:
        scheduled = connection.execute(
            "SELECT logical_date FROM dag_run WHERE run_type = 'scheduled' "
            "ORDER BY logical_date"
        ).fetchall()
    assert [row[0][:10] for row in scheduled] == [
        "2026-07-01",
        "2026-07-02",
        "2026-07-03",
    ]

    run_scheduler_once(
        dag_id="daily",
        file_path=path,
        store=store,
        now=datetime(2026, 7, 10, tzinfo=UTC),
        log_root=tmp_path / "logs",
    )
    with sqlite3.connect(store.path) as connection:
        scheduled = connection.execute(
            "SELECT logical_date FROM dag_run WHERE run_type = 'scheduled' "
            "ORDER BY logical_date"
        ).fetchall()
    assert [row[0][:10] for row in scheduled] == [
        f"2026-07-{day:02d}" for day in range(1, 10)
    ]
    assert store.latest_automated_logical_date("daily") == datetime(
        2026, 7, 10, tzinfo=UTC
    )


def test_scheduled_scan_passes_thousand_consecutive_backfilled_intervals(tmp_path):
    path = tmp_path / "minute.py"
    path.write_text(
        "from datetime import datetime, timezone\n"
        "from easyflow.sdk import DAG\n"
        'dag = DAG("minute", schedule="* * * * *", catchup=True, '
        "start_date=datetime(2026, 7, 1, tzinfo=timezone.utc))\n",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    store.init_db()
    start = datetime(2026, 7, 1, tzinfo=UTC)
    with sqlite3.connect(store.path) as connection:
        connection.executemany(
            "INSERT INTO dag_run(dag_id, run_id, run_type, logical_date, "
            "data_interval_start, data_interval_end, state, backfill_id, queued_at) "
            "VALUES ('minute', ?, 'backfill', ?, ?, ?, 'success', 'bulk', ?)",
            (
                (
                    f"backfill-{index}",
                    (start + timedelta(minutes=index)).isoformat(),
                    (start + timedelta(minutes=index)).isoformat(),
                    (start + timedelta(minutes=index)).isoformat(),
                    start.isoformat(),
                )
                for index in range(1000)
            ),
        )

    result = run_scheduler_once(
        dag_id="minute",
        file_path=path,
        store=store,
        now=start + timedelta(minutes=1000),
        log_root=tmp_path / "logs",
    )
    assert len(result) == 1 and result[0].state.value == "success"
    assert store.latest_automated_logical_date("minute") == start + timedelta(
        minutes=1000
    )
    assert store.count_dag_runs(dag_id="minute", run_type="scheduled") == 1


def test_clear_uses_capacity_of_the_runs_own_group(tmp_path):
    with DAG("capacity", max_active_runs=1) as dag:
        EmptyOperator(task_id="work")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    at = datetime(2026, 7, 1, tzinfo=UTC)
    manual = runner.create_manual_run(dag, file_path=None, logical_date=at)
    assert runner.run_existing(dag, run_id=manual).state.value == "success"

    def backfill_run(backfill_id: str, day: int) -> str:
        logical_date = at + timedelta(days=day)
        return runner.create_backfill_run(
            dag,
            file_path=None,
            interval=interval_for_logical_date("@daily", logical_date),
            backfill_id=backfill_id,
            max_active_runs=1,
        )

    store.create_backfill(
        backfill_id="second",
        dag_id=dag.dag_id,
        start_logical_date=at,
        end_logical_date=at + timedelta(days=4),
        reprocessing_behavior="none",
        max_active_runs=1,
    )
    completed = backfill_run("first", 1)
    assert runner.run_existing(dag, run_id=completed).state.value == "success"
    completed_same_group = backfill_run("first", 2)
    assert (
        runner.run_existing(dag, run_id=completed_same_group).state.value == "success"
    )
    queued_other_group = backfill_run("second", 3)
    assert store.get_dag_run(dag.dag_id, queued_other_group).state == "queued"

    assert store.clear_task_instances(dag_id=dag.dag_id, run_id=manual).to_clear == [
        "work"
    ]
    assert store.clear_task_instances(dag_id=dag.dag_id, run_id=completed).to_clear == [
        "work"
    ]
    blocked = store.clear_task_instances(dag_id=dag.dag_id, run_id=completed_same_group)
    assert blocked.blocked and "max_active_runs=1" in blocked.blocked[0].reason
    assert store.get_dag_run(dag.dag_id, completed_same_group).state == "success"


def test_backfill_preview_is_bounded_and_reads_states_once(
    tmp_path, monkeypatch, capsys
):
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    hourly = DAG("hourly", schedule="@hourly")
    runner = PersistentDagRunner(store)
    runner.create_manual_run(
        hourly,
        file_path=None,
        logical_date=datetime(2026, 7, 1, 1, tzinfo=UTC),
        run_id="previous",
    )
    assert runner.run_existing(hourly, run_id="previous").state.value == "success"
    runner.create_manual_run(
        hourly,
        file_path=None,
        logical_date=datetime(2026, 7, 1, 1, tzinfo=UTC),
        run_id="existing",
    )
    original_init_db = store.init_db
    init_calls = 0

    def counted_init_db():
        nonlocal init_calls
        init_calls += 1
        return original_init_db()

    monkeypatch.setattr(store, "init_db", counted_init_db)
    preview = preview_backfill(
        dag=hourly,
        store=store,
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 1),
    )
    assert len(preview) == 24
    assert init_calls == 1
    assert preview[1].latest_state.value == "queued"
    assert not preview[1].eligible and preview[2].eligible

    minute = DAG("minute", schedule="* * * * *")
    with pytest.raises(ValueError, match="最多支持 1000 个区间"):
        preview_backfill(
            dag=minute,
            store=store,
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 1),
        )
    assert init_calls == 1
    path = tmp_path / "minute.py"
    path.write_text(
        'from easyflow.sdk import DAG\ndag = DAG("minute", schedule="* * * * *")\n',
        encoding="utf-8",
    )
    assert (
        main(
            [
                "backfill",
                "create",
                "--dag-id",
                "minute",
                "--file",
                str(path),
                "--start-date",
                "2026-07-01",
                "--end-date",
                "2026-07-01",
                "--database-url",
                store.database_url,
                "--dry-run",
            ]
        )
        == 2
    )
    assert "最多支持 1000 个区间" in capsys.readouterr().err


def test_long_run_id_has_bounded_log_component_and_log_failure_is_terminal(tmp_path):
    with DAG("long_log") as dag:
        EmptyOperator(task_id="work")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = "x" * 200
    runner.create_manual_run(
        dag,
        file_path=None,
        logical_date=datetime(2026, 7, 1, tzinfo=UTC),
        run_id=run_id,
    )
    assert runner.run_existing(dag, run_id=run_id).state.value == "success"
    log_path = Path(store.latest_task_log_path(dag.dag_id, run_id, "work"))
    assert log_path.exists()
    assert log_path.parts[-4].startswith("run-hash-")
    assert len(log_path.parts[-4].encode("utf-8")) <= 255

    blocked_root = tmp_path / "blocked-logs"
    blocked_root.write_text("not a directory", encoding="utf-8")
    blocked_runner = PersistentDagRunner(store, log_root=blocked_root)
    blocked_runner.create_manual_run(
        dag,
        file_path=None,
        logical_date=datetime(2026, 7, 2, tzinfo=UTC),
        run_id="blocked",
    )
    result = blocked_runner.run_existing(dag, run_id="blocked")
    assert result.state.value == "failed"
    assert (
        store.get_task_state(dag.dag_id, "blocked", "work") == TaskInstanceState.FAILED
    )
    with sqlite3.connect(store.path) as connection:
        error = connection.execute(
            "SELECT error_message FROM task_try WHERE dag_id = ? AND run_id = ?",
            (dag.dag_id, "blocked"),
        ).fetchone()[0]
    assert error and "directory" in error.lower()


def test_long_run_id_executes_with_local_executor(tmp_path):
    path = tmp_path / "local.py"
    path.write_text(
        "from easyflow.sdk import DAG\n"
        "from easyflow.providers.standard.operators.empty import EmptyOperator\n"
        'with DAG("local_logs") as dag:\n    EmptyOperator(task_id="work")\n',
        encoding="utf-8",
    )
    dag = load_dag_from_file(path, "local_logs")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = "local-" + "x" * 200
    runner.create_manual_run(
        dag,
        file_path=path,
        logical_date=datetime(2026, 7, 1, tzinfo=UTC),
        run_id=run_id,
    )
    result = runner.run_existing(dag, run_id=run_id, file_path=path, executor="local")
    assert result.state.value == "success"
    assert Path(store.latest_task_log_path(dag.dag_id, run_id, "work")).exists()
