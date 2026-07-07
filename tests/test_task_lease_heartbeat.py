from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from easyflow._internal.dag_parser import load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.cli import main


def test_task_lease_heartbeat_and_orphan_retry_then_resume(tmp_path) -> None:
    counter = tmp_path / "counter.txt"
    dag_file = tmp_path / "lease_task.py"
    dag_file.write_text(
        f"""
from pathlib import Path
from easyflow.sdk import DAG, task
with DAG("lease_task") as dag:
    @task(retries=1)
    def work():
        path = Path({str(counter)!r})
        value = int(path.read_text()) if path.exists() else 0
        path.write_text(str(value + 1))
        return value + 1
    work()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'easyflow.db'}")
    dag = load_dag_from_file(dag_file, "lease_task")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(
        dag,
        file_path=dag_file,
        logical_date=datetime(2026, 7, 6, tzinfo=timezone.utc),
        run_id="manual__lease",
    )
    start = datetime(2026, 7, 6, tzinfo=timezone.utc)
    try_number = store.start_task_try(
        "lease_task",
        run_id,
        "work",
        owner="worker-a",
        lease_seconds=1,
        now=start,
    )
    assert try_number == 1
    assert store.renew_task_lease(
        dag_id="lease_task",
        run_id=run_id,
        task_id="work",
        try_number=1,
        owner="worker-a",
        lease_seconds=5,
        now=start + timedelta(seconds=1),
    )
    assert (
        store.recover_orphaned_task(
            dag_id="lease_task",
            run_id=run_id,
            task_id="work",
            retries=1,
            now=start + timedelta(seconds=2),
        )
        is None
    )
    recovered = store.recover_orphaned_task(
        dag_id="lease_task",
        run_id=run_id,
        task_id="work",
        retries=1,
        now=start + timedelta(seconds=7),
    )
    assert recovered.value == "up_for_retry"
    with sqlite3.connect(tmp_path / "easyflow.db") as connection:
        heartbeat = connection.execute(
            "SELECT heartbeat_at FROM task_heartbeat WHERE task_id = 'work'"
        ).fetchone()
        task_try = connection.execute(
            "SELECT state, error_code FROM task_try WHERE task_id = 'work' AND try_number = 1"
        ).fetchone()
    assert heartbeat is not None
    assert task_try == ("failed", "TASK_PROCESS_LOST")

    result = runner.run_existing(dag, run_id=run_id, file_path=dag_file)
    assert result.state.value == "success"
    assert counter.read_text() == "1"
    with sqlite3.connect(tmp_path / "easyflow.db") as connection:
        tries = connection.execute(
            "SELECT try_number, state FROM task_try ORDER BY try_number"
        ).fetchall()
    assert tries == [(1, "failed"), (2, "success")]


def test_orphan_without_retry_becomes_failed(tmp_path) -> None:
    dag_file = tmp_path / "no_retry.py"
    dag_file.write_text(
        """
from easyflow.sdk import DAG, task
with DAG("no_retry") as dag:
    @task(retries=0)
    def work():
        return 1
    work()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'easyflow.db'}")
    dag = load_dag_from_file(dag_file, "no_retry")
    run_id = PersistentDagRunner(store, log_root=tmp_path / "logs").create_manual_run(
        dag,
        file_path=dag_file,
        logical_date=datetime(2026, 7, 6, tzinfo=timezone.utc),
        run_id="manual__no_retry",
    )
    start = datetime(2026, 7, 6, tzinfo=timezone.utc)
    store.start_task_try(
        "no_retry",
        run_id,
        "work",
        owner="worker-a",
        lease_seconds=1,
        now=start,
    )
    recovered = store.recover_orphaned_task(
        dag_id="no_retry",
        run_id=run_id,
        task_id="work",
        retries=0,
        now=start + timedelta(seconds=2),
    )
    assert recovered.value == "failed"


def test_scheduler_loop_max_loops_cli(tmp_path, capsys) -> None:
    dag_file = tmp_path / "loop_dag.py"
    dag_file.write_text(
        """
from easyflow.sdk import DAG, task
with DAG("loop_job", schedule=None) as dag:
    @task
    def work():
        return 1
    work()
""",
        encoding="utf-8",
    )
    exit_code = main(
        [
            "scheduler",
            "--dag-id",
            "loop_job",
            "--file",
            str(dag_file),
            "--max-loops",
            "2",
            "--interval-seconds",
            "0",
            "--database-url",
            f"sqlite:///{tmp_path / 'easyflow.db'}",
            "--log-root",
            str(tmp_path / "logs"),
        ]
    )
    assert exit_code == 0
    assert "No scheduled runs created." in capsys.readouterr().out
