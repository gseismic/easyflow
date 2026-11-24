from __future__ import annotations

import os
import shlex
import signal
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from easyflow._internal import local_executor
from easyflow._internal.dag_parser import load_dag_from_file, load_dag_snapshot
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.cli import main
from easyflow.operators import EmptyOperator
from easyflow.sdk import DAG


@pytest.mark.skipif(os.name != "posix", reason="使用 POSIX 进程组信号")
@pytest.mark.parametrize("worker_alive", [False, True])
def test_process_group_eperm_only_ignored_after_worker_exit(monkeypatch, worker_alive):
    process = SimpleNamespace(pid=123, is_alive=lambda: worker_alive)
    worker = SimpleNamespace(process=process, ready=SimpleNamespace(value=1))

    def deny_signal(pid, sig):
        assert pid == process.pid and sig == signal.SIGKILL
        raise PermissionError("进程组当前不可用")

    monkeypatch.setattr(local_executor.os, "killpg", deny_signal)
    if worker_alive:
        with pytest.raises(PermissionError, match="进程组当前不可用"):
            local_executor._signal_worker_group(worker, signal.SIGKILL)
    else:
        assert not local_executor._signal_worker_group(worker, signal.SIGKILL)


def test_expired_local_worker_is_reaped_and_keeps_its_retry(tmp_path):
    path = tmp_path / "retry.py"
    started = tmp_path / "started"
    late = tmp_path / "late"
    path.write_text(
        f"""
from multiprocessing import parent_process
from pathlib import Path
import time
from easyflow._internal.task_heartbeat import TaskHeartbeat
from easyflow.sdk import DAG, task

if parent_process() is not None:
    TaskHeartbeat.start = lambda self: None

with DAG('retry') as dag:
    @task(retries=1)
    def work():
        marker = Path({str(started)!r})
        if not marker.exists():
            marker.touch()
            time.sleep(3)
            Path({str(late)!r}).touch()
        return '重试完成'
    work()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    dag = load_dag_from_file(path, "retry")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs", task_lease_seconds=1)
    run_id = runner.create_manual_run(dag, file_path=path, logical_date=datetime.now(UTC))
    result = runner.run_existing(dag, run_id=run_id, file_path=path, executor="local")
    assert result.state.value == "success"
    assert started.exists() and not late.exists()
    assert store.pull_xcom("retry", run_id, "work", "return_value") == "重试完成"
    with sqlite3.connect(store.path) as connection:
        assert connection.execute(
            "SELECT try_number, state, error_code FROM task_try ORDER BY try_number"
        ).fetchall() == [(1, "failed", "TASK_PROCESS_LOST"), (2, "success", None)]


@pytest.mark.skipif(os.name != "posix", reason="验证 POSIX 本地进程组")
@pytest.mark.parametrize(("inherit", "crash"), [(False, False), (True, False), (True, True)])
def test_stopped_bash_worker_cannot_write_after_its_retry(tmp_path, inherit, crash):
    path = tmp_path / "bash_retry.py"
    started, late = tmp_path / "started", tmp_path / "late"
    stop_parent = 'kill -KILL "$PPID"' if crash else ""
    command = f"""
if [ ! -e {shlex.quote(str(started))} ]; then
    trap '' TERM
    touch {shlex.quote(str(started))}
    (sleep 2.5; touch {shlex.quote(str(late))}) &
    {stop_parent}
    wait
else
    echo 'retry completed'
fi
"""
    retries = "None" if inherit else "1"
    delay = "None" if inherit else "timedelta(seconds=1)"
    maximum = "None" if inherit else "timedelta(milliseconds=200)"
    path.write_text(
        f"""from datetime import timedelta
from multiprocessing import parent_process
from pathlib import Path
from easyflow._internal.task_heartbeat import TaskHeartbeat
from easyflow.operators import BashOperator
from easyflow.sdk import DAG

if parent_process() is not None and not Path({str(started)!r}).exists():
    TaskHeartbeat.start = lambda self: None

with DAG('bash_retry', default_args={{'retries': 1, 'retry_delay': timedelta(seconds=1),
                                     'max_retry_delay': timedelta(milliseconds=200)}}) as dag:
    BashOperator(task_id='work', bash_command={command!r}, retries={retries},
                 retry_delay={delay}, max_retry_delay={maximum}, retry_exponential_backoff=True)
""",
        encoding="utf-8",
    )
    dag = load_dag_snapshot(path, "bash_retry")
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs", task_lease_seconds=1)
    run_id = runner.create_manual_run(dag, file_path=path, logical_date=datetime.now(UTC))
    result = runner.run_existing(dag, run_id=run_id, file_path=path, executor="local")
    assert result.state.value == "success"
    assert started.exists()
    assert store.pull_xcom("bash_retry", run_id, "work", "return_value") == "retry completed"
    with sqlite3.connect(store.path) as connection:
        attempts = connection.execute(
            "SELECT state, error_code, started_at, ended_at FROM task_try ORDER BY try_number"
        ).fetchall()
    assert [attempt[:2] for attempt in attempts] == [("failed", "TASK_PROCESS_LOST"), ("success", None)]
    first_failure = datetime.fromisoformat(attempts[0][3])
    second_start = datetime.fromisoformat(attempts[1][2])
    assert second_start - first_failure >= timedelta(milliseconds=200)
    assert dag.get_task("work").retry_delay == timedelta(seconds=1)
    assert dag.get_task("work").max_retry_delay == timedelta(milliseconds=200)
    time.sleep(max(0, started.stat().st_mtime + 2.8 - time.time()))
    assert not late.exists()


def test_full_local_executor_does_not_write_already_queued_tasks(tmp_path, monkeypatch):
    with DAG("full") as dag:
        for i in range(500):
            EmptyOperator(task_id=f"task_{i}")
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    now = datetime.now(UTC)
    run_id = runner.create_manual_run(dag, file_path=None, logical_date=now)
    store.start_task_try(dag.dag_id, run_id, "task_0", lease_seconds=60)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE task_instance SET state = 'queued', queued_at = ? WHERE state IS NULL",
            (now.isoformat(),),
        )
    statements = []
    connect = store._connect

    @contextmanager
    def traced_connect():
        with connect() as connection:
            connection.set_trace_callback(statements.append)
            yield connection

    monkeypatch.setattr(store, "_connect", traced_connect)
    executor = local_executor.LocalExecutor(
        store=store, dag=dag, run_id=run_id, file_path=tmp_path / "unused.py",
        log_root=tmp_path / "logs", task_lease_seconds=60, parallelism=1,
        max_active_tasks=1, depends_on_past_decision=lambda **_: "ready", run_type="manual",
        logical_date=now, conf={},
    )
    states = executor.run()
    assert list(states.values()).count("queued") == 499 and states["task_0"] == "running"
    assert not any(statement.startswith("BEGIN IMMEDIATE") for statement in statements)


@pytest.mark.skipif(os.name != "posix", reason="使用 POSIX SIGTERM 处理器")
@pytest.mark.parametrize("phase", ["startup", "shutdown"])
def test_local_worker_ignoring_sigterm_stops(tmp_path, monkeypatch, phase):
    path = tmp_path / "slow.py"
    started = tmp_path / "started"
    finished = tmp_path / "finished"
    path.write_text(
        f"""
from multiprocessing import parent_process
from pathlib import Path
import signal
import time
from easyflow.sdk import DAG, task

if parent_process() is not None:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if {phase!r} == 'startup':
        Path({str(started)!r}).touch()
        time.sleep(4)

with DAG('slow') as dag:
    @task
    def work():
        if {phase!r} == 'shutdown':
            Path({str(started)!r}).touch()
            time.sleep(4)
        Path({str(finished)!r}).touch()
    work()
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(local_executor, "PARSER_TIMEOUT_SECONDS", 2)
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    dag = load_dag_from_file(path, "slow")
    runner = PersistentDagRunner(
        store, log_root=tmp_path / "logs",
        should_continue=lambda: phase == "startup" or not started.exists(),
    )
    run_id = runner.create_manual_run(dag, file_path=path, logical_date=datetime.now(UTC))
    result = runner.run_existing(dag, run_id=run_id, file_path=path, executor="local")
    assert started.exists() and not finished.exists()
    if phase == "startup":
        assert result.state.value == "failed"
        with sqlite3.connect(store.path) as connection:
            assert connection.execute("SELECT error_code, error_message FROM task_try").fetchall() == [
                ("WORKER_STARTUP_FAILED", "Worker 在领取任务前启动超时。")
            ]
    else:
        assert result.state.value == "running"


def test_file_tasks_keep_local_annotation_modules_through_execution(tmp_path):
    helper = tmp_path / "review_annotations.py"
    helper.write_text(
        "from __future__ import annotations\nAlias = int\nclass Payload:\n    value: Alias\n",
        encoding="utf-8",
    )
    path = tmp_path / "typed.py"
    path.write_text(
        """
from typing import get_type_hints
from review_annotations import Payload
from easyflow.sdk import DAG, task

with DAG('typed') as dag:
    @task
    def work():
        assert get_type_hints(Payload) == {'value': int}
        return '解析成功'
    work()
""",
        encoding="utf-8",
    )
    assert main(["dags", "test", "typed", "--file", str(path)]) == 0
    assert "review_annotations" not in sys.modules
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    dag = load_dag_from_file(path, "typed")
    run_id = runner.create_manual_run(dag, file_path=path, logical_date=datetime.now(UTC))
    result = runner.run_existing(dag, run_id=run_id, file_path=path, executor="local")
    assert result.state.value == "success"
    assert store.pull_xcom("typed", run_id, "work", "return_value") == "解析成功"


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
