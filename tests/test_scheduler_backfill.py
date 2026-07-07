from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from easyflow._internal.dag_parser import load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.scheduler import create_backfill, run_scheduler_once
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.cli import main


def test_scheduler_once_creates_latest_daily_run_and_deduplicates(tmp_path) -> None:
    dag_file = tmp_path / "scheduled_dag.py"
    marker = tmp_path / "marker.txt"
    dag_file.write_text(
        f"""
from pathlib import Path
from easyflow.sdk import DAG, get_current_context, task

with DAG("daily_job", schedule="@daily") as dag:
    @task
    def write_marker():
        context = get_current_context()
        Path({str(marker)!r}).write_text(context["ds"])
        return context["ds"]

    write_marker()
""",
        encoding="utf-8",
    )
    db_url = f"sqlite:///{tmp_path / 'easyflow.db'}"
    store = SQLiteStateStore(db_url)

    first = run_scheduler_once(
        dag_id="daily_job",
        file_path=dag_file,
        store=store,
        now=__import__("datetime").datetime.fromisoformat("2026-07-08T12:00:00+00:00"),
        log_root=tmp_path / "logs",
    )
    second = run_scheduler_once(
        dag_id="daily_job",
        file_path=dag_file,
        store=store,
        now=__import__("datetime").datetime.fromisoformat("2026-07-08T12:00:00+00:00"),
        log_root=tmp_path / "logs",
    )

    assert len(first) == 1
    assert first[0].state.value == "success"
    assert second == []
    assert marker.read_text() == "2026-07-07"
    assert store.count_dag_runs(dag_id="daily_job", run_type="scheduled") == 1


def test_scheduler_respects_max_active_runs(tmp_path) -> None:
    dag_file = tmp_path / "active_limit.py"
    dag_file.write_text(
        """
from easyflow.sdk import DAG, task

with DAG("active_limit", schedule="@daily", max_active_runs=1) as dag:
    @task
    def work():
        return "ok"

    work()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'easyflow.db'}")
    dag = load_dag_from_file(dag_file, "active_limit")
    PersistentDagRunner(store, log_root=tmp_path / "logs").create_manual_run(
        dag,
        file_path=dag_file,
        logical_date=__import__("datetime").datetime.fromisoformat(
            "2026-07-06T00:00:00+00:00"
        ),
        run_id="manual__still_queued",
    )

    created = run_scheduler_once(
        dag_id="active_limit",
        file_path=dag_file,
        store=store,
        now=__import__("datetime").datetime.fromisoformat("2026-07-08T12:00:00+00:00"),
        log_root=tmp_path / "logs",
    )

    assert created == []
    assert store.count_dag_runs(dag_id="active_limit", run_type="scheduled") == 0


def test_backfill_create_runs_date_range(tmp_path) -> None:
    dag_file = tmp_path / "backfill_dag.py"
    output = tmp_path / "dates.txt"
    dag_file.write_text(
        f"""
from pathlib import Path
from easyflow.sdk import DAG, get_current_context, task

with DAG("daily_backfill", schedule="@daily") as dag:
    @task
    def append_date():
        context = get_current_context()
        path = Path({str(output)!r})
        old = path.read_text() if path.exists() else ""
        path.write_text(old + context["ds"] + "\\n")
        return context["ds"]

    append_date()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'easyflow.db'}")

    summary = create_backfill(
        dag_id="daily_backfill",
        file_path=dag_file,
        store=store,
        start_date=__import__("datetime").date(2026, 7, 1),
        end_date=__import__("datetime").date(2026, 7, 3),
        backfill_id="bf_test",
        log_root=tmp_path / "logs",
    )

    assert summary.backfill_id == "bf_test"
    assert [run.state.value for run in summary.runs] == ["success", "success", "success"]
    assert output.read_text().splitlines() == [
        "2026-07-01",
        "2026-07-02",
        "2026-07-03",
    ]
    assert store.count_dag_runs(dag_id="daily_backfill", run_type="backfill") == 3


def test_depends_on_past_blocks_after_previous_backfill_failure(tmp_path) -> None:
    dag_file = tmp_path / "depends_dag.py"
    output = tmp_path / "ran.txt"
    dag_file.write_text(
        f"""
from pathlib import Path
from easyflow.sdk import DAG, get_current_context, task

with DAG("depends_job", schedule="@daily") as dag:
    @task(depends_on_past=True)
    def fragile():
        context = get_current_context()
        if context["ds"] == "2026-07-01":
            raise RuntimeError("first day failed")
        path = Path({str(output)!r})
        old = path.read_text() if path.exists() else ""
        path.write_text(old + context["ds"] + "\\n")
        return context["ds"]

    fragile()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'easyflow.db'}")

    summary = create_backfill(
        dag_id="depends_job",
        file_path=dag_file,
        store=store,
        start_date=__import__("datetime").date(2026, 7, 1),
        end_date=__import__("datetime").date(2026, 7, 2),
        backfill_id="bf_depends",
        log_root=tmp_path / "logs",
    )

    assert [run.state.value for run in summary.runs] == ["failed", "failed"]
    with sqlite3.connect(tmp_path / "easyflow.db") as connection:
        states = connection.execute(
            """
            SELECT dr.logical_date, ti.state
            FROM dag_run dr
            JOIN task_instance ti ON ti.dag_id = dr.dag_id AND ti.run_id = dr.run_id
            ORDER BY dr.logical_date
            """
        ).fetchall()
        tries = connection.execute("SELECT task_id, try_number FROM task_try").fetchall()
    assert states[0][1] == "failed"
    assert states[1][1] == "upstream_failed"
    assert tries == [("fragile", 1)]
    assert not output.exists()


def test_depends_on_past_ignores_manual_run_failure(tmp_path) -> None:
    dag_file = tmp_path / "manual_isolated.py"
    output = tmp_path / "scheduled.txt"
    dag_file.write_text(
        f"""
from pathlib import Path
from easyflow.sdk import DAG, get_current_context, task

with DAG("manual_isolated", schedule="@daily") as dag:
    @task(depends_on_past=True)
    def work():
        context = get_current_context()
        if context["dag_run"]["conf"].get("fail"):
            raise RuntimeError("manual failed")
        Path({str(output)!r}).write_text(context["ds"])
        return context["ds"]

    work()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'easyflow.db'}")
    dag = load_dag_from_file(dag_file, "manual_isolated")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    manual_run_id = runner.create_manual_run(
        dag,
        file_path=dag_file,
        logical_date=datetime(2026, 7, 1, tzinfo=timezone.utc),
        run_id="manual__failed",
        conf={"fail": True},
    )
    assert runner.run_existing(dag, run_id=manual_run_id).state.value == "failed"

    scheduled = run_scheduler_once(
        dag_id="manual_isolated",
        file_path=dag_file,
        store=store,
        now=datetime(2026, 7, 8, 12, tzinfo=timezone.utc),
        log_root=tmp_path / "logs",
    )

    assert [run.state.value for run in scheduled] == ["success"]
    assert output.read_text() == "2026-07-07"


def test_scheduler_cli_and_backfill_cli(tmp_path, capsys) -> None:
    dag_file = tmp_path / "cli_sched.py"
    dag_file.write_text(
        """
from easyflow.sdk import DAG, task

with DAG("cli_sched", schedule="@daily") as dag:
    @task
    def work():
        print("scheduled-log")
        return "ok"

    work()
""",
        encoding="utf-8",
    )
    db_url = f"sqlite:///{tmp_path / 'easyflow.db'}"
    log_root = tmp_path / "logs"

    assert (
        main(
            [
                "scheduler",
                "--once",
                "--dag-id",
                "cli_sched",
                "--file",
                str(dag_file),
                "--now",
                "2026-07-08T12:00:00+00:00",
                "--database-url",
                db_url,
                "--log-root",
                str(log_root),
            ]
        )
        == 0
    )
    assert "scheduled__2026-07-07T00:00:00+00:00 success" in capsys.readouterr().out

    assert (
        main(
            [
                "backfill",
                "create",
                "--dag-id",
                "cli_sched",
                "--file",
                str(dag_file),
                "--start-date",
                "2026-07-01",
                "--end-date",
                "2026-07-01",
                "--backfill-id",
                "bf_cli",
                "--database-url",
                db_url,
                "--log-root",
                str(log_root),
            ]
        )
        == 0
    )
    assert "backfill_id=bf_cli" in capsys.readouterr().out
