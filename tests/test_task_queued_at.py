from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from easyflow._internal.dag_parser import load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.state_store import DagPaused, SQLiteStateStore, TaskCapacityFull
from easyflow.providers.standard.operators.empty import EmptyOperator
from easyflow.sdk import DAG
from easyflow.sdk.states import TaskInstanceState


def task_row(store, run_id, task_id="work"):
    with sqlite3.connect(store.path) as connection:
        connection.row_factory = sqlite3.Row
        return dict(
            connection.execute(
                "SELECT * FROM task_instance WHERE run_id = ? AND task_id = ?",
                (run_id, task_id),
            ).fetchone()
        )


def queued_run(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    with DAG("queue") as dag:
        EmptyOperator(task_id="work")
        EmptyOperator(task_id="other")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(
        dag, file_path=None, logical_date=datetime.now(UTC)
    )
    return store, runner, dag, run_id


def test_queue_survives_capacity_wait_and_preserves_time(tmp_path):
    store, _, _, run_id = queued_run(tmp_path)
    start = datetime.now(UTC)
    assert store.queue_task("queue", run_id, "work", now=start)
    assert store.queue_task("queue", run_id, "other", now=start)
    assert store.start_task_try("queue", run_id, "other", now=start) == 1
    with pytest.raises(TaskCapacityFull):
        store.start_task_try("queue", run_id, "work", now=start + timedelta(seconds=5))
    assert store.queue_task("queue", run_id, "work", now=start + timedelta(seconds=10))
    waiting = task_row(store, run_id)
    assert waiting["state"] == "queued"
    assert waiting["queued_at"] == start.isoformat()
    assert waiting["started_at"] is None
    store.commit_task_terminal(
        dag_id="queue", run_id=run_id, task_id="other", try_number=1, xcom_values={}
    )
    assert (
        store.start_task_try("queue", run_id, "work", now=start + timedelta(seconds=20))
        == 1
    )
    running = task_row(store, run_id)
    assert running["queued_at"] == waiting["queued_at"]
    assert datetime.fromisoformat(running["started_at"]) - start == timedelta(
        seconds=20
    )
    assert not store.queue_task("queue", run_id, "work")


def test_retry_requeues_only_when_due_and_clear_resets_time(tmp_path):
    store, _, _, run_id = queued_run(tmp_path)
    first = datetime.now(UTC)
    assert store.queue_task("queue", run_id, "work", now=first)
    assert store.start_task_try("queue", run_id, "work") == 1
    store.commit_failure(
        dag_id="queue",
        run_id=run_id,
        task_id="work",
        try_number=1,
        error=RuntimeError("重试验证"),
        retries=1,
        retry_delay=timedelta(minutes=2),
    )
    retrying = task_row(store, run_id)
    retry_at = datetime.fromisoformat(retrying["next_retry_at"])
    assert not store.queue_task(
        "queue", run_id, "work", now=retry_at - timedelta(seconds=1)
    )
    assert task_row(store, run_id)["queued_at"] == first.isoformat()
    assert store.queue_task("queue", run_id, "work", now=retry_at)
    queued = task_row(store, run_id)
    assert queued["queued_at"] == retry_at.isoformat()
    assert queued["started_at"] is None and queued["ended_at"] is None
    assert (
        store.start_task_try(
            "queue", run_id, "work", now=retry_at + timedelta(seconds=5)
        )
        == 2
    )
    store.commit_task_terminal(
        dag_id="queue", run_id=run_id, task_id="work", try_number=2, xcom_values={}
    )
    store.clear_task_instances(dag_id="queue", run_id=run_id, task_ids=["work"])
    assert task_row(store, run_id)["queued_at"] is None
    cleared_at = retry_at + timedelta(minutes=5)
    assert store.queue_task("queue", run_id, "work", now=cleared_at)
    assert task_row(store, run_id)["queued_at"] == cleared_at.isoformat()


def test_direct_start_has_queue_time_and_pause_prevents_queue(tmp_path):
    store, _, _, run_id = queued_run(tmp_path)
    store.set_dag_paused("queue", True)
    with pytest.raises(DagPaused):
        store.queue_task("queue", run_id, "work")
    assert task_row(store, run_id)["queued_at"] is None
    store.set_dag_paused("queue", False)
    with pytest.raises(ValueError, match="时区"):
        store.queue_task("queue", run_id, "work", now=datetime(2026, 1, 1))
    assert store.start_task_try("queue", run_id, "work") == 1
    row = task_row(store, run_id)
    assert row["queued_at"] == row["started_at"]


@pytest.mark.parametrize("executor", ["sequential", "local"])
def test_executors_queue_ready_tasks_before_capacity_and_leave_dependencies_unqueued(
    tmp_path, executor
):
    database = tmp_path / "state.db"
    path = tmp_path / "queue_demo.py"
    path.write_text(
        f"""import sqlite3
from easyflow.sdk import DAG, task
with DAG("queue_demo") as dag:
    @task
    def a():
        with sqlite3.connect({str(database)!r}) as connection:
            rows = dict(connection.execute("SELECT task_id, state FROM task_instance"))
            assert rows["b"] == "queued", rows
            assert rows["child"] is None, rows
    @task
    def b():
        return 2
    @task
    def child():
        return 3
    a() >> child()
    b()
""",
        encoding="utf-8",
    )
    dag = load_dag_from_file(path, "queue_demo")
    store = SQLiteStateStore(str(database))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(
        dag, file_path=path, logical_date=datetime.now(UTC)
    )
    result = runner.run_existing(dag, run_id=run_id, file_path=path, executor=executor)
    assert result.state.value == "success"
    assert all(
        value == TaskInstanceState.SUCCESS for value in result.task_states.values()
    )
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            "SELECT queued_at, started_at FROM task_instance"
        ).fetchall()
    assert all(queued and queued <= started for queued, started in rows)
    assert (
        task_row(store, run_id, "child")["queued_at"]
        >= task_row(store, run_id, "a")["ended_at"]
    )
