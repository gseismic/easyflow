from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone

from easyflow._internal.dag_parser import discover_dag_sources, load_dag_snapshot
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.scheduler import create_backfill, run_scheduler_once_for_sources
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.providers.standard.operators.empty import EmptyOperator
from easyflow.sdk import DAG


NOW = datetime(2026, 7, 8, tzinfo=timezone.utc)


def test_expired_task_from_removed_dag_does_not_block_global_capacity(tmp_path) -> None:
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.sqlite'}")
    store.configure_parallelism(1)
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    with DAG("removed") as old:
        EmptyOperator(task_id="abandoned")
    old_run = runner.create_manual_run(old, file_path=None, logical_date=NOW)
    store.start_task_try(
        "removed",
        old_run,
        "abandoned",
        now=datetime.now(timezone.utc) - timedelta(minutes=2),
    )
    with DAG("new") as new:
        EmptyOperator(task_id="ready")
    new_run = runner.create_manual_run(new, file_path=None, logical_date=NOW)
    assert runner.run_existing(new, run_id=new_run).state.value == "success"


def test_scheduler_runs_independent_dags_concurrently(tmp_path) -> None:
    for dag_id, other in (("a", "b"), ("b", "a")):
        (tmp_path / f"{dag_id}.py").write_text(
            f"""from datetime import datetime, timezone
from easyflow.sdk import DAG, task
with DAG({dag_id!r}, schedule="@once", start_date=datetime(2026, 7, 1, tzinfo=timezone.utc)) as dag:
    @task
    def work():
        import time
        from pathlib import Path
        Path({str(tmp_path / dag_id)!r}).touch()
        deadline = time.monotonic() + 5
        while not Path({str(tmp_path / other)!r}).exists():
            if time.monotonic() > deadline: raise RuntimeError("other DAG did not start")
            time.sleep(0.02)
    work()
""",
            encoding="utf-8",
        )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.sqlite'}")
    sources = discover_dag_sources(tmp_path).sources
    summaries, issues = run_scheduler_once_for_sources(
        sources=sources,
        store=store,
        now=NOW,
        executor="local",
        parallelism=2,
        log_root=tmp_path / "logs",
    )
    assert issues == []
    assert [item.state.value for item in summaries] == ["success", "success"]


def test_scheduler_runs_multiple_runs_of_same_dag_concurrently(tmp_path) -> None:
    dag_file = tmp_path / "same.py"
    dag_file.write_text(
        f"""from datetime import datetime, timezone
from easyflow.sdk import DAG, task
with DAG("same", schedule="@daily", catchup=True, max_active_runs=2,
         start_date=datetime(2026, 7, 7, tzinfo=timezone.utc)) as dag:
    @task
    def work(logical_date):
        import time
        from pathlib import Path
        directory = Path({str(tmp_path)!r})
        (directory / str(logical_date.day)).touch()
        other = directory / ("8" if logical_date.day == 7 else "7")
        deadline = time.monotonic() + 5
        while not other.exists():
            if time.monotonic() > deadline: raise RuntimeError("other run did not start")
            time.sleep(0.02)
    work()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.sqlite'}")
    sources = discover_dag_sources(tmp_path).sources
    summaries, issues = run_scheduler_once_for_sources(
        sources=sources,
        store=store,
        now=NOW,
        executor="local",
        parallelism=2,
        log_root=tmp_path / "logs",
    )
    assert issues == []
    assert [item.state.value for item in summaries] == ["success", "success"]


def test_backfill_runs_dates_concurrently_when_capacity_allows(tmp_path) -> None:
    dag_file = tmp_path / "backfill.py"
    dag_file.write_text(
        f"""from easyflow.sdk import DAG, task
with DAG("backfill_parallel", schedule="@daily", max_active_runs=1) as dag:
    @task
    def work(logical_date):
        import time
        from pathlib import Path
        directory = Path({str(tmp_path)!r})
        (directory / str(logical_date.day)).touch()
        other = directory / ("8" if logical_date.day == 7 else "7")
        deadline = time.monotonic() + 5
        while not other.exists():
            if time.monotonic() > deadline: raise RuntimeError("other backfill did not start")
            time.sleep(0.02)
    work()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.sqlite'}")
    summary = create_backfill(
        dag_id="backfill_parallel",
        file_path=dag_file,
        store=store,
        start_date=date(2026, 7, 7),
        end_date=date(2026, 7, 8),
        max_active_runs=2,
        executor="local",
        parallelism=2,
        log_root=tmp_path / "logs",
    )
    assert summary.complete
    assert [run.state.value for run in summary.runs] == ["success", "success"]


def test_parallelism_is_shared_by_independent_cli_processes(tmp_path) -> None:
    lock = tmp_path / "task.lock"
    commands = []
    url = f"sqlite:///{tmp_path / 'state.sqlite'}"
    SQLiteStateStore(url).init_db()
    for dag_id in ("a", "b"):
        path = tmp_path / f"{dag_id}.py"
        path.write_text(
            f"""from easyflow.sdk import DAG, task
with DAG({dag_id!r}) as dag:
    @task
    def work():
        import os, time
        fd = os.open({str(lock)!r}, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try: time.sleep(0.4)
        finally:
            os.close(fd)
            os.unlink({str(lock)!r})
    work()
""",
            encoding="utf-8",
        )
        commands.append(
            [
                sys.executable,
                "-m",
                "easyflow.cli",
                "dags",
                "trigger",
                dag_id,
                "--file",
                str(path),
                "--database-url",
                url,
                "--log-root",
                str(tmp_path / "logs"),
                "--executor",
                "local",
                "--parallelism",
                "1",
            ]
        )
    processes = [
        subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=os.environ.copy(),
        )
        for command in commands
    ]
    for process in processes:
        stdout, stderr = process.communicate(timeout=20)
        assert process.returncode == 0, (stdout, stderr)


def test_retry_wait_does_not_block_other_ready_task(tmp_path) -> None:
    events = tmp_path / "events.txt"
    dag_file = tmp_path / "retry.py"
    dag_file.write_text(
        f"""from datetime import timedelta
from easyflow.sdk import DAG, task
with DAG("retry") as dag:
    @task(retries=1, retry_delay=timedelta(seconds=1))
    def a_retry():
        from pathlib import Path
        path = Path({str(events)!r})
        old = path.read_text() if path.exists() else ""
        path.write_text(old + "retry\\n")
        if not old: raise ValueError("first attempt")
    @task
    def b_ready():
        from pathlib import Path
        path = Path({str(events)!r})
        path.write_text(path.read_text() + "ready\\n")
    a_retry()
    b_ready()
""",
        encoding="utf-8",
    )
    dag = load_dag_snapshot(dag_file, "retry")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.sqlite'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(dag, file_path=dag_file, logical_date=NOW)
    assert (
        runner.run_existing(dag, run_id=run_id, file_path=dag_file).state.value
        == "success"
    )
    assert events.read_text().splitlines() == ["retry", "ready", "retry"]


def test_daemon_discovers_new_dag_while_another_is_running(tmp_path) -> None:
    started = tmp_path / "started"
    release = tmp_path / "release"
    finished = tmp_path / "finished"
    quick = tmp_path / "quick"
    (tmp_path / "a.py").write_text(
        f"""from easyflow.sdk import DAG, task
with DAG("a", schedule="@once") as dag:
    @task
    def slow():
        import time
        from pathlib import Path
        Path({str(started)!r}).touch()
        deadline = time.monotonic() + 10
        while not Path({str(release)!r}).exists():
            if time.monotonic() > deadline: raise RuntimeError("not released")
            time.sleep(0.02)
        Path({str(finished)!r}).touch()
    slow()
""",
        encoding="utf-8",
    )
    command = [
        sys.executable,
        "-m",
        "easyflow.cli",
        "scheduler",
        "--dags-folder",
        str(tmp_path),
        "--database-url",
        f"sqlite:///{tmp_path / 'state.sqlite'}",
        "--log-root",
        str(tmp_path / "logs"),
        "--interval-seconds",
        "0.1",
        "--executor",
        "local",
        "--parallelism",
        "2",
    ]
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )
    try:
        deadline = time.monotonic() + 10
        while not started.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert started.exists(), "第一个 DAG 未启动"
        (tmp_path / "b.py").write_text(
            f"""from easyflow.sdk import DAG, task
with DAG("b", schedule="@once") as dag:
    @task
    def work():
        from pathlib import Path
        Path({str(quick)!r}).touch()
    work()
""",
            encoding="utf-8",
        )
        deadline = time.monotonic() + 8
        while not quick.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert quick.exists(), "运行中的 DAG 阻塞了新 DAG"
        release.touch()
        deadline = time.monotonic() + 5
        while not finished.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert finished.exists()
    finally:
        release.touch()
        process.terminate()
        process.communicate(timeout=10)


def test_daemon_schedules_good_dag_while_another_file_is_slow_to_parse(
    tmp_path,
) -> None:
    marker = tmp_path / "ran"
    (tmp_path / "a_slow.py").write_text(
        "import time\ntime.sleep(5)\n", encoding="utf-8"
    )
    (tmp_path / "b_good.py").write_text(
        f"""from easyflow.sdk import DAG, task
with DAG("good", schedule="@once") as dag:
    @task
    def work():
        from pathlib import Path
        Path({str(marker)!r}).touch()
    work()
""",
        encoding="utf-8",
    )
    command = [
        sys.executable,
        "-m",
        "easyflow.cli",
        "scheduler",
        "--dags-folder",
        str(tmp_path),
        "--database-url",
        f"sqlite:///{tmp_path / 'state.sqlite'}",
        "--log-root",
        str(tmp_path / "logs"),
        "--interval-seconds",
        "0.1",
        "--executor",
        "local",
    ]
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )
    try:
        deadline = time.monotonic() + 4
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert marker.exists(), "慢解析文件不应阻止其他 DAG 调度"
    finally:
        process.terminate()
        process.communicate(timeout=10)
