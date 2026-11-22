from __future__ import annotations

import sqlite3
import subprocess
import sys
from datetime import datetime, timezone

from easyflow._internal.dag_parser import load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.cli import main


def _successful_windows(store, run_id):
    with sqlite3.connect(store.path) as connection:
        rows = connection.execute(
            "SELECT task_id, started_at, ended_at FROM task_try "
            "WHERE run_id = ? AND state = 'success' ORDER BY task_id",
            (run_id,),
        ).fetchall()
    assert [row[0] for row in rows] == ["a", "b"]
    return [
        (datetime.fromisoformat(started), datetime.fromisoformat(ended))
        for _, started, ended in rows
    ]


def test_local_executor_runs_independent_tasks_concurrently(tmp_path) -> None:
    dag_file = tmp_path / "parallel_dag.py"
    dag_file.write_text(
        """
import time
from easyflow.sdk import DAG, task

with DAG("parallel") as dag:
    @task
    def a():
        time.sleep(1.0)
        return "a"

    @task
    def b():
        time.sleep(1.0)
        return "b"

    a()
    b()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'easyflow.db'}")
    run_id = "manual__parallel"
    command = subprocess.run(
        [
            sys.executable,
            "-m",
            "easyflow.cli",
            "dags",
            "trigger",
            "parallel",
            "--file",
            str(dag_file),
            "--database-url",
            store.database_url,
            "--log-root",
            str(tmp_path / "logs"),
            "--run-id",
            run_id,
            "--executor",
            "local",
            "--parallelism",
            "2",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert command.returncode == 0, command.stderr
    assert store.get_dag_run("parallel", run_id).state == "success"
    (started_a, ended_a), (started_b, ended_b) = _successful_windows(store, run_id)
    assert max(started_a, started_b) < min(ended_a, ended_b)


def test_local_executor_respects_max_active_tasks(tmp_path) -> None:
    dag_file = tmp_path / "limited_dag.py"
    dag_file.write_text(
        """
import time
from easyflow.sdk import DAG, task

with DAG("limited", max_active_tasks=1) as dag:
    @task
    def a():
        time.sleep(0.7)
        return "a"

    @task
    def b():
        time.sleep(0.7)
        return "b"

    a()
    b()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'easyflow.db'}")
    dag = load_dag_from_file(dag_file, "limited")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(
        dag,
        file_path=dag_file,
        logical_date=datetime(2026, 7, 6, tzinfo=timezone.utc),
        run_id="manual__limited",
    )

    result = runner.run_existing(
        dag,
        run_id=run_id,
        file_path=dag_file,
        executor="local",
        parallelism=2,
    )
    assert result.state.value == "success"
    (started_a, ended_a), (started_b, ended_b) = _successful_windows(store, run_id)
    assert ended_a <= started_b or ended_b <= started_a


def test_local_executor_taskflow_xcom_across_processes(tmp_path) -> None:
    dag_file = tmp_path / "xcom_dag.py"
    dag_file.write_text(
        """
from easyflow.sdk import DAG, task

with DAG("xcom_local") as dag:
    @task
    def extract():
        return {"value": 41}

    @task
    def transform(data):
        return data["value"] + 1

    transform(extract())
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'easyflow.db'}")
    dag = load_dag_from_file(dag_file, "xcom_local")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(
        dag,
        file_path=dag_file,
        logical_date=datetime(2026, 7, 6, tzinfo=timezone.utc),
        run_id="manual__xcom",
    )

    result = runner.run_existing(
        dag,
        run_id=run_id,
        file_path=dag_file,
        executor="local",
        parallelism=2,
    )

    assert result.state.value == "success"
    assert store.pull_xcom("xcom_local", run_id, "transform", "return_value") == 42


def test_local_executor_failure_marks_dag_failed(tmp_path) -> None:
    dag_file = tmp_path / "fail_dag.py"
    dag_file.write_text(
        """
from easyflow.sdk import DAG, task

with DAG("fail_local") as dag:
    @task
    def fail():
        raise RuntimeError("boom")

    fail()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'easyflow.db'}")
    dag = load_dag_from_file(dag_file, "fail_local")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(
        dag,
        file_path=dag_file,
        logical_date=datetime(2026, 7, 6, tzinfo=timezone.utc),
        run_id="manual__fail",
    )

    result = runner.run_existing(
        dag,
        run_id=run_id,
        file_path=dag_file,
        executor="local",
        parallelism=2,
    )

    assert result.state.value == "failed"
    assert result.task_states["fail"].value == "failed"


def test_cli_trigger_local_executor(tmp_path, capsys) -> None:
    dag_file = tmp_path / "cli_local.py"
    dag_file.write_text(
        """
from easyflow.sdk import DAG, task

with DAG("cli_local") as dag:
    @task
    def work():
        print("local-log")
        return "ok"

    work()
""",
        encoding="utf-8",
    )
    exit_code = main(
        [
            "dags",
            "trigger",
            "cli_local",
            "--file",
            str(dag_file),
            "--run-id",
            "manual__local",
            "--logical-date",
            "2026-07-06T00:00:00+00:00",
            "--executor",
            "local",
            "--parallelism",
            "2",
            "--database-url",
            f"sqlite:///{tmp_path / 'easyflow.db'}",
            "--log-root",
            str(tmp_path / "logs"),
        ]
    )
    assert exit_code == 0
    assert "cli_local manual__local success" in capsys.readouterr().out
