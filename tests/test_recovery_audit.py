from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from easyflow._internal.dag_parser import load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.scheduler import run_scheduler_once
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.cli import main


def test_scheduler_lease_blocks_second_owner_until_expired(tmp_path) -> None:
    dag_file = tmp_path / "lease_dag.py"
    dag_file.write_text(
        """
from datetime import datetime, timezone
from easyflow.sdk import DAG, task
with DAG("lease_job", schedule="@daily", start_date=datetime(2026, 7, 1, tzinfo=timezone.utc)) as dag:
    @task
    def work():
        return "ok"
    work()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'easyflow.db'}")
    now = datetime(2026, 7, 8, 12, tzinfo=timezone.utc)
    assert store.acquire_scheduler_lease(owner="other", lease_seconds=30).acquired
    with pytest.raises(RuntimeError, match="scheduler lease"):
        run_scheduler_once(
            dag_id="lease_job",
            file_path=dag_file,
            store=store,
            now=now + timedelta(seconds=1),
            owner="mine",
            log_root=tmp_path / "logs",
        )

    store.release_scheduler_lease(owner="other")
    assert store.acquire_scheduler_lease(
        owner="other",
        lease_seconds=30,
        now=datetime.now(timezone.utc) - timedelta(seconds=31),
    ).acquired
    result = run_scheduler_once(
        dag_id="lease_job",
        file_path=dag_file,
        store=store,
        now=now + timedelta(seconds=31),
        owner="mine",
        log_root=tmp_path / "logs",
    )
    assert [run.state.value for run in result] == ["success"]


def test_definition_hash_change_fails_unfinished_run_without_executing_new_code(
    tmp_path,
) -> None:
    marker = tmp_path / "marker.txt"
    dag_file = tmp_path / "hash_dag.py"
    dag_file.write_text(
        f"""
from pathlib import Path
from easyflow.sdk import DAG, task
with DAG("hash_job") as dag:
    @task
    def work():
        Path({str(marker)!r}).write_text("old")
        return "old"
    work()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'easyflow.db'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    dag = load_dag_from_file(dag_file, "hash_job")
    run_id = runner.create_manual_run(
        dag,
        file_path=dag_file,
        logical_date=datetime(2026, 7, 6, tzinfo=timezone.utc),
        run_id="manual__hash",
    )
    dag_file.write_text(
        f"""
from pathlib import Path
from easyflow.sdk import DAG, task
with DAG("hash_job") as dag:
    @task
    def work():
        Path({str(marker)!r}).write_text("new")
        return "new"
    work()
""",
        encoding="utf-8",
    )
    new_dag = load_dag_from_file(dag_file, "hash_job")
    result = runner.run_existing(new_dag, run_id=run_id, file_path=dag_file)

    assert result.state.value == "failed"
    assert not marker.exists()
    assert (
        "dag_definition_changed",
        "hash_job",
        "manual__hash",
    ) in store.list_audit_actions()


def test_tasks_clear_dry_run_execute_and_rerun(tmp_path, capsys) -> None:
    counter = tmp_path / "counter.txt"
    dag_file = tmp_path / "clear_dag.py"
    dag_file.write_text(
        f"""
from pathlib import Path
from easyflow.sdk import DAG, task
with DAG("clear_job") as dag:
    @task
    def work():
        path = Path({str(counter)!r})
        value = int(path.read_text()) if path.exists() else 0
        path.write_text(str(value + 1))
        return value + 1
    work()
""",
        encoding="utf-8",
    )
    db_url = f"sqlite:///{tmp_path / 'easyflow.db'}"
    assert (
        main(
            [
                "dags",
                "trigger",
                "clear_job",
                "--file",
                str(dag_file),
                "--run-id",
                "manual__clear",
                "--logical-date",
                "2026-07-06T00:00:00+00:00",
                "--database-url",
                db_url,
                "--log-root",
                str(tmp_path / "logs"),
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert counter.read_text() == "1"

    assert (
        main(
            [
                "tasks",
                "clear",
                "clear_job",
                "--run-id",
                "manual__clear",
                "--task-id",
                "work",
                "--dry-run",
                "--database-url",
                db_url,
            ]
        )
        == 0
    )
    assert "DRY-RUN clear candidates: work" in capsys.readouterr().out
    assert (
        SQLiteStateStore(db_url)
        .get_task_state("clear_job", "manual__clear", "work")
        .value
        == "success"
    )

    assert (
        main(
            [
                "tasks",
                "clear",
                "clear_job",
                "--run-id",
                "manual__clear",
                "--task-id",
                "work",
                "--yes",
                "--reason",
                "rerun test",
                "--database-url",
                db_url,
            ]
        )
        == 0
    )
    assert "EXECUTED clear candidates: work" in capsys.readouterr().out
    store = SQLiteStateStore(db_url)
    assert store.get_task_state("clear_job", "manual__clear", "work") is None
    assert store.get_dag_run("clear_job", "manual__clear").state == "queued"
    assert ("tasks_clear", "clear_job", "manual__clear") in store.list_audit_actions()
    with sqlite3.connect(tmp_path / "easyflow.db") as connection:
        try_count = connection.execute("SELECT COUNT(*) FROM task_try").fetchone()[0]
        xcom_count = connection.execute("SELECT COUNT(*) FROM xcom").fetchone()[0]
    assert try_count == 1
    assert xcom_count == 0

    dag = load_dag_from_file(dag_file, "clear_job")
    result = PersistentDagRunner(store, log_root=tmp_path / "logs").run_existing(
        dag,
        run_id="manual__clear",
        file_path=dag_file,
    )
    assert result.state.value == "success"
    assert counter.read_text() == "2"
    with sqlite3.connect(tmp_path / "easyflow.db") as connection:
        try_count = connection.execute("SELECT COUNT(*) FROM task_try").fetchone()[0]
    assert try_count == 2


def test_tasks_clear_without_yes_refuses_to_modify(tmp_path) -> None:
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'easyflow.db'}")
    dag_file = tmp_path / "refuse.py"
    dag_file.write_text(
        """
from easyflow.sdk import DAG, task
with DAG("refuse") as dag:
    @task
    def work():
        return 1
    work()
""",
        encoding="utf-8",
    )
    dag = load_dag_from_file(dag_file, "refuse")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(
        dag,
        file_path=dag_file,
        logical_date=datetime(2026, 7, 6, tzinfo=timezone.utc),
        run_id="manual__refuse",
    )
    assert (
        runner.run_existing(dag, run_id=run_id, file_path=dag_file).state.value
        == "success"
    )

    assert (
        main(
            [
                "tasks",
                "clear",
                "refuse",
                "--run-id",
                "manual__refuse",
                "--task-id",
                "work",
                "--database-url",
                f"sqlite:///{tmp_path / 'easyflow.db'}",
            ]
        )
        == 2
    )
    assert store.get_task_state("refuse", "manual__refuse", "work").value == "success"
