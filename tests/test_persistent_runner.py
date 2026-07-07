from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from easyflow._internal.dag_parser import load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.cli import main
from easyflow.sdk.states import DagRunState, TaskInstanceState


def test_persistent_manual_run_taskflow_xcom_and_logs(tmp_path) -> None:
    dag_file = tmp_path / "dag_file.py"
    dag_file.write_text(
        """
from easyflow.sdk import DAG, task

with DAG("persisted", schedule="@daily") as dag:
    @task
    def extract():
        print("extract-log")
        return {"value": 41}

    @task
    def transform(data):
        print("transform-log")
        return data["value"] + 1

    transform(extract())
""",
        encoding="utf-8",
    )
    db_url = f"sqlite:///{tmp_path / 'easyflow.db'}"
    log_root = tmp_path / "logs"
    dag = load_dag_from_file(dag_file, "persisted")
    store = SQLiteStateStore(db_url)
    runner = PersistentDagRunner(store, log_root=log_root)
    run_id = runner.create_manual_run(
        dag,
        file_path=dag_file,
        logical_date=datetime(2026, 7, 6, tzinfo=timezone.utc),
        run_id="manual__test",
    )

    result = runner.run_existing(dag, run_id=run_id)

    assert result.state == DagRunState.SUCCESS
    assert result.task_states == {
        "extract": TaskInstanceState.SUCCESS,
        "transform": TaskInstanceState.SUCCESS,
    }
    assert store.pull_xcom("persisted", run_id, "transform", "return_value") == 42
    log_path = store.latest_task_log_path("persisted", run_id, "transform")
    assert log_path is not None
    assert "transform-log" in open(log_path, encoding="utf-8").read()


def test_persistent_runner_does_not_rerun_successful_existing_run(tmp_path) -> None:
    counter = tmp_path / "counter.txt"
    dag_file = tmp_path / "dag_file.py"
    dag_file.write_text(
        f"""
from pathlib import Path
from easyflow.sdk import DAG, task

with DAG("skip_success") as dag:
    @task
    def work():
        path = Path({str(counter)!r})
        value = int(path.read_text() or "0") if path.exists() else 0
        path.write_text(str(value + 1))
        return value + 1

    work()
""",
        encoding="utf-8",
    )
    db_url = f"sqlite:///{tmp_path / 'easyflow.db'}"
    dag = load_dag_from_file(dag_file, "skip_success")
    store = SQLiteStateStore(db_url)
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(
        dag,
        file_path=dag_file,
        logical_date=datetime(2026, 7, 6, tzinfo=timezone.utc),
        run_id="manual__once",
    )
    assert runner.run_existing(dag, run_id=run_id).state == DagRunState.SUCCESS
    assert runner.run_existing(dag, run_id=run_id).state == DagRunState.SUCCESS
    assert counter.read_text() == "1"


def test_persistent_runner_records_failure(tmp_path) -> None:
    dag_file = tmp_path / "bad_dag.py"
    dag_file.write_text(
        """
from easyflow.sdk import DAG, task

with DAG("bad") as dag:
    @task(retries=1)
    def fail():
        raise RuntimeError("boom")

    fail()
""",
        encoding="utf-8",
    )
    db_url = f"sqlite:///{tmp_path / 'easyflow.db'}"
    dag = load_dag_from_file(dag_file, "bad")
    store = SQLiteStateStore(db_url)
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(
        dag,
        file_path=dag_file,
        logical_date=datetime(2026, 7, 6, tzinfo=timezone.utc),
        run_id="manual__bad",
    )

    result = runner.run_existing(dag, run_id=run_id)

    assert result.state == DagRunState.FAILED
    assert result.task_states["fail"] == TaskInstanceState.FAILED
    with sqlite3.connect(tmp_path / "easyflow.db") as connection:
        rows = connection.execute(
            "SELECT try_number, state, error_message FROM task_try ORDER BY try_number"
        ).fetchall()
    assert len(rows) == 2
    assert rows[-1][1] == "failed"
    assert "boom" in rows[-1][2]


def test_cli_trigger_state_and_logs(tmp_path, capsys) -> None:
    dag_file = tmp_path / "cli_dag.py"
    dag_file.write_text(
        """
from easyflow.sdk import DAG, task

with DAG("cli_demo") as dag:
    @task
    def work():
        print("hello-log")
        return "ok"

    work()
""",
        encoding="utf-8",
    )
    db_url = f"sqlite:///{tmp_path / 'easyflow.db'}"
    log_root = tmp_path / "logs"

    exit_code = main(
        [
            "dags",
            "trigger",
            "cli_demo",
            "--file",
            str(dag_file),
            "--run-id",
            "manual__cli",
            "--logical-date",
            "2026-07-06T00:00:00+00:00",
            "--database-url",
            db_url,
            "--log-root",
            str(log_root),
        ]
    )
    assert exit_code == 0
    assert "cli_demo manual__cli success" in capsys.readouterr().out

    assert main(["dags", "state", "cli_demo", "manual__cli", "--database-url", db_url]) == 0
    assert capsys.readouterr().out.strip() == "success"
    assert (
        main(
            [
                "tasks",
                "state",
                "cli_demo",
                "manual__cli",
                "work",
                "--database-url",
                db_url,
            ]
        )
        == 0
    )
    assert capsys.readouterr().out.strip() == "success"
    assert (
        main(
            [
                "tasks",
                "logs",
                "cli_demo",
                "manual__cli",
                "work",
                "--database-url",
                db_url,
            ]
        )
        == 0
    )
    assert "hello-log" in capsys.readouterr().out
