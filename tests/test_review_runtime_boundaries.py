from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import UTC, date, datetime
from importlib.machinery import SourceFileLoader
from pathlib import Path
from types import ModuleType

import pytest

from easyflow._internal import task_heartbeat as heartbeat_module
from easyflow._internal.dag_parser import load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.scheduler import (
    create_backfill,
    run_scheduler_loop,
    run_scheduler_once,
    run_scheduler_once_for_sources,
)
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.cli import main
from easyflow.providers.standard.operators.empty import EmptyOperator
from easyflow.providers.standard.operators.python import PythonOperator
from easyflow.sdk import DAG, chain
from easyflow.sdk.exceptions import AirflowSkipException
from easyflow.web.service import WebService

AT = datetime(2026, 7, 1, tzinfo=UTC)


def write_simple_dag(
    path, *, dag_id="demo", task_id="work", body="return 1", retries=0
):
    path.write_text(
        "from datetime import datetime, timezone\n"
        "from easyflow.sdk import DAG, task\n"
        f"with DAG({dag_id!r}, schedule='@daily', "
        "start_date=datetime(2026, 7, 1, tzinfo=timezone.utc)) as dag:\n"
        f"    @task(task_id={task_id!r}, retries={retries})\n    def work():\n"
        + "\n".join("        " + line for line in body.splitlines())
        + "\n    work()\n",
        encoding="utf-8",
    )
    return load_dag_from_file(path, dag_id)


def database_snapshot(store):
    with sqlite3.connect(store.path) as db:
        return {
            table: db.execute(f'SELECT * FROM "{table}"').fetchall()
            for table in store.list_tables()
        }


@pytest.mark.parametrize("executor", ["sequential", "local"])
def test_native_gil_holder_keeps_lease_and_worker_identity(tmp_path, executor):
    marker = tmp_path / "entered"
    (tmp_path / "dags").mkdir()
    dag_file = tmp_path / "dags" / "native.py"
    dag = write_simple_dag(
        dag_file,
        body=(
            "import ctypes, os\nfrom pathlib import Path\n"
            f"Path({str(marker)!r}).touch()\n"
            "sleep = ctypes.PyDLL(None).sleep\n"
            "sleep.argtypes = [ctypes.c_uint]\nsleep.restype = ctypes.c_uint\n"
            "sleep(2)\nreturn os.getpid()"
        ),
    )
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    runner.create_manual_run(dag, file_path=dag_file, logical_date=AT, run_id="native")
    driver = tmp_path / "driver.py"
    driver.write_text(
        "from easyflow._internal.dag_parser import load_dag_from_file\n"
        "from easyflow._internal.persistent_runner import PersistentDagRunner\n"
        "from easyflow._internal.state_store import SQLiteStateStore\n"
        "if __name__ == '__main__':\n"
        f"    dag = load_dag_from_file({str(dag_file)!r}, 'demo')\n"
        f"    store = SQLiteStateStore({store.database_url!r})\n"
        f"    runner = PersistentDagRunner(store, log_root={str(tmp_path / 'logs')!r}, task_lease_seconds=1)\n"
        f"    result = runner.run_existing(dag, run_id='native', file_path={str(dag_file)!r}, executor={executor!r})\n"
        "    assert result.state.value == 'success', result\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(sys.path)
    process = subprocess.Popen(
        [sys.executable, str(driver)],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    heartbeats = set()
    recovered = {}
    try:
        deadline = time.monotonic() + 15
        while process.poll() is None and time.monotonic() < deadline:
            recovered.update(
                store.recover_orphaned_tasks_for_run(dag=dag, run_id="native")
            )
            if marker.exists():
                with sqlite3.connect(store.path) as db:
                    row = db.execute(
                        "SELECT heartbeat_at FROM task_heartbeat"
                    ).fetchone()
                if row:
                    heartbeats.add(row[0])
            time.sleep(0.05)
        output, errors = process.communicate(timeout=2)
        assert process.returncode == 0, (output.decode(), errors.decode())
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    assert marker.exists() and not recovered
    assert len(heartbeats) >= 3
    worker_pid = store.pull_xcom("demo", "native", "work", "return_value")
    with sqlite3.connect(store.path) as db:
        assert db.execute("SELECT worker_pid FROM task_heartbeat").fetchone() == (
            worker_pid,
        )
        assert db.execute("SELECT state, error_code FROM task_try").fetchall() == [
            ("success", None)
        ]
    assert not store.list_audit_actions()


def test_loading_local_sources_preserves_entry_modules(tmp_path, monkeypatch):
    entry = tmp_path / "driver.py"
    entry.write_text("# 正在执行的入口不应作为普通依赖清除。\n")
    module = ModuleType("__main__")
    module.__loader__ = SourceFileLoader("__main__", str(entry))
    monkeypatch.setitem(sys.modules, "__main__", module)
    monkeypatch.setitem(sys.modules, "__mp_main__", module)
    write_simple_dag(tmp_path / "demo.py")
    assert sys.modules.get("__main__") is module
    assert sys.modules.get("__mp_main__") is module


def test_real_worker_crash_stops_renewal_and_allows_retry(tmp_path):
    marker = tmp_path / "crashed"
    path = tmp_path / "demo.py"
    write_simple_dag(
        path,
        retries=1,
        body=(
            "import os\nfrom pathlib import Path\n"
            f"path = Path({str(marker)!r})\n"
            "if not path.exists():\n    path.touch()\n    os._exit(9)\nreturn 42"
        ),
    )
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    driver = tmp_path / "driver.py"
    driver.write_text(
        "import time\n"
        "from datetime import datetime, timezone\n"
        "from easyflow._internal.dag_parser import load_dag_from_file\n"
        "from easyflow._internal.persistent_runner import PersistentDagRunner\n"
        "from easyflow._internal.state_store import SQLiteStateStore\n"
        "if __name__ == '__main__':\n"
        f"    dag = load_dag_from_file({str(path)!r}, 'demo')\n"
        f"    runner = PersistentDagRunner(SQLiteStateStore({store.database_url!r}), log_root={str(tmp_path / 'logs')!r}, task_lease_seconds=1)\n"
        f"    runner.create_manual_run(dag, file_path={str(path)!r}, logical_date=datetime(2026, 7, 1, tzinfo=timezone.utc), run_id='crash')\n"
        f"    result = runner.run_existing(dag, run_id='crash', file_path={str(path)!r}, executor='local')\n"
        "    deadline = time.monotonic() + 8\n"
        "    while result.state.value == 'running' and time.monotonic() < deadline:\n"
        "        time.sleep(0.1)\n"
        f"        result = runner.run_existing(dag, run_id='crash', file_path={str(path)!r}, executor='local')\n"
        "    assert result.state.value == 'success', result\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(sys.path)
    result = subprocess.run(
        [sys.executable, str(driver)],
        env=environment,
        capture_output=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode()
    assert marker.exists()
    assert store.pull_xcom("demo", "crash", "work", "return_value") == 42
    with sqlite3.connect(store.path) as db:
        assert db.execute(
            "SELECT state, error_code FROM task_try ORDER BY try_number"
        ).fetchall() == [("failed", "TASK_PROCESS_LOST"), ("success", None)]


@pytest.mark.parametrize("outcome", ["success", "failure", "skip"])
def test_lease_covers_terminal_commit_and_heartbeat_is_reaped(
    tmp_path, monkeypatch, outcome
):
    def execute():
        if outcome == "failure":
            raise RuntimeError("expected failure")
        if outcome == "skip":
            raise AirflowSkipException("expected skip")
        return 42

    with DAG("commit") as dag:
        PythonOperator(task_id="work", python_callable=execute)
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(
        store, log_root=tmp_path / "logs", task_lease_seconds=1
    )
    runner.create_manual_run(dag, file_path=None, logical_date=AT, run_id="commit")
    method = "commit_failure" if outcome == "failure" else "commit_task_terminal"
    original_commit = getattr(store, method)

    def delayed_commit(**kwargs):
        time.sleep(1.5)
        return original_commit(**kwargs)

    monkeypatch.setattr(store, method, delayed_commit)
    processes = []
    original_popen = heartbeat_module.subprocess.Popen

    def capture_process(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(heartbeat_module.subprocess, "Popen", capture_process)
    stopped = threading.Event()
    recovered = {}

    def scan():
        while not stopped.wait(0.05):
            recovered.update(
                store.recover_orphaned_tasks_for_run(dag=dag, run_id="commit")
            )

    scanner = threading.Thread(target=scan, daemon=True)
    scanner.start()
    try:
        result = runner.run_existing(dag, run_id="commit")
    finally:
        stopped.set()
        scanner.join(5)
    assert not scanner.is_alive() and not recovered
    assert (
        result.task_states["work"].value
        == {"success": "success", "failure": "failed", "skip": "skipped"}[outcome]
    )
    assert processes and all(process.poll() is not None for process in processes)
    assert all(process.stdin.closed and process.stdout.closed for process in processes)


def test_heartbeat_start_failure_finishes_attempt_without_executing(
    tmp_path, monkeypatch
):
    calls = []
    with DAG("startup") as dag:
        PythonOperator(task_id="work", python_callable=lambda: calls.append(1))
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    runner.create_manual_run(dag, file_path=None, logical_date=AT, run_id="startup")

    def fail_start(*args, **kwargs):
        raise OSError("heartbeat start denied")

    monkeypatch.setattr(heartbeat_module.subprocess, "Popen", fail_start)
    assert runner.run_existing(dag, run_id="startup").state.value == "failed"
    assert not calls
    with sqlite3.connect(store.path) as db:
        assert db.execute("SELECT state, error_code FROM task_try").fetchone() == (
            "failed",
            "OSError",
        )
    assert (
        "heartbeat start denied"
        in Path(store.latest_task_log_path("startup", "startup", "work")).read_text()
    )


def test_renewal_cannot_replace_worker_or_renew_an_old_attempt(tmp_path):
    with DAG("identity") as dag:
        EmptyOperator(task_id="work")
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    PersistentDagRunner(store).create_manual_run(
        dag, file_path=None, logical_date=AT, run_id="identity"
    )
    store.start_task_try(
        "identity", "identity", "work", owner="owner", now=AT, lease_seconds=1
    )
    before = database_snapshot(store)
    common = {
        "dag_id": "identity",
        "run_id": "identity",
        "task_id": "work",
        "try_number": 1,
    }
    assert not store.renew_task_lease(**common, owner="other")
    assert not store.renew_task_lease(
        **common, owner="owner", worker_pid=os.getpid() + 1
    )
    assert database_snapshot(store) == before
    store.recover_orphaned_tasks_for_run(dag=dag, run_id="identity")
    assert store.clear_task_instances(
        dag_id="identity", run_id="identity"
    ).to_clear == ["work"]
    assert store.start_task_try("identity", "identity", "work", owner="owner") == 2
    assert not store.renew_task_lease(**common, owner="owner")


@pytest.mark.parametrize("database_url", [None, "sqlite:///state.db", "state.db"])
def test_database_is_fixed_before_initialization(tmp_path, monkeypatch, database_url):
    original = tmp_path / "original"
    elsewhere = tmp_path / "elsewhere"
    original.mkdir()
    elsewhere.mkdir()
    monkeypatch.chdir(original)
    store = (
        SQLiteStateStore() if database_url is None else SQLiteStateStore(database_url)
    )
    expected = original / (
        ".easyflow/easyflow.db" if database_url is None else "state.db"
    )
    monkeypatch.chdir(elsewhere)
    assert store.init_db() == expected
    assert SQLiteStateStore(store.database_url).path == expected
    assert expected.is_file()
    assert not list(elsewhere.rglob("*.db"))


@pytest.mark.parametrize("executor", ["sequential", "local"])
def test_task_chdir_does_not_move_default_database(tmp_path, monkeypatch, executor):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(tmp_path)
    dag_file = tmp_path / "demo.py"
    dag = write_simple_dag(
        dag_file, body=f"import os\nos.chdir({str(elsewhere)!r})\nreturn 42"
    )
    store = SQLiteStateStore()
    runner = PersistentDagRunner(store)
    runner.create_manual_run(dag, file_path=dag_file, logical_date=AT, run_id="chdir")
    try:
        result = runner.run_existing(
            dag, run_id="chdir", file_path=dag_file, executor=executor
        )
        assert result.state.value == "success"
        assert store.pull_xcom("demo", "chdir", "work", "return_value") == 42
    finally:
        monkeypatch.chdir(tmp_path)
    assert not list(elsewhere.rglob("*.db"))
    assert SQLiteStateStore().get_dag_run("demo", "chdir").state == "success"


@pytest.mark.parametrize("entry", ["trigger", "backfill", "once", "loop"])
def test_cli_rejects_executor_before_any_persistence(tmp_path, entry, capsys):
    path = tmp_path / "demo.py"
    write_simple_dag(path)
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    before = database_snapshot(store)
    args = {
        "trigger": ["dags", "trigger", "demo"],
        "backfill": [
            "backfill",
            "create",
            "--dag-id",
            "demo",
            "--start-date",
            "2026-07-01",
            "--end-date",
            "2026-07-01",
        ],
        "once": ["scheduler", "--once", "--dag-id", "demo"],
        "loop": ["scheduler", "--max-loops", "1", "--dag-id", "demo"],
    }[entry]
    args += [
        "--file",
        str(path),
        "--database-url",
        store.database_url,
        "--executor",
        "typo",
    ]
    assert main(args) == 2
    assert "typo" in capsys.readouterr().err
    assert database_snapshot(store) == before


@pytest.mark.parametrize("entry", ["runner", "once", "sources", "loop", "backfill"])
def test_internal_entries_reject_executor_before_writes(tmp_path, entry):
    path = tmp_path / "demo.py"
    dag = write_simple_dag(path)
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    runner.create_manual_run(dag, file_path=path, logical_date=AT, run_id="queued")
    before = database_snapshot(store)
    common = {"store": store, "executor": "typo"}
    with pytest.raises(ValueError, match="executor"):
        if entry == "runner":
            runner.run_existing(dag, run_id="queued", executor="typo")
        elif entry == "once":
            run_scheduler_once(dag_id="demo", file_path=path, now=AT, **common)
        elif entry == "sources":
            run_scheduler_once_for_sources(sources=[], now=AT, **common)
        elif entry == "loop":
            run_scheduler_loop(dag_id="demo", file_path=path, max_loops=1, **common)
        else:
            create_backfill(
                dag_id="demo",
                file_path=path,
                start_date=date(2026, 7, 1),
                end_date=date(2026, 7, 1),
                **common,
            )
    assert database_snapshot(store) == before


@pytest.mark.parametrize("executor", ["sequential", "local"])
@pytest.mark.parametrize(
    "kind", ["absolute", "traversal", "slash", "dots", "unicode", "long"]
)
def test_log_identifiers_stay_inside_root_and_are_readable(
    tmp_path, executor, kind, capsys
):
    dag_id, task_id = {
        "absolute": ("dag", str(tmp_path / "escaped-task")),
        "traversal": ("..", "../../escaped"),
        "slash": ("dag", "c/d"),
        "dots": (".", ".."),
        "unicode": ("dag", "task%?中文"),
        "long": ("a" * 600, "长" * 300),
    }[kind]
    path = tmp_path / "demo.py"
    dag = write_simple_dag(
        path, dag_id=dag_id, task_id=task_id, body="print('safe-log')"
    )
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    log_root = tmp_path / "logs"
    runner = PersistentDagRunner(store, log_root=log_root)
    runner.create_manual_run(dag, file_path=path, logical_date=AT, run_id="run/../中文")
    result = runner.run_existing(
        dag, run_id="run/../中文", file_path=path, executor=executor
    )
    assert result.state.value == "success"
    saved = Path(store.latest_task_log_path(dag_id, "run/../中文", task_id)).resolve()
    assert saved.is_relative_to(log_root) and saved.is_file()
    assert all(len(part.encode()) <= 255 for part in saved.relative_to(log_root).parts)
    service = WebService(str(tmp_path), store.database_url, str(log_root))
    assert (
        "safe-log" in service.task_log(dag_id, "run/../中文", task_id, None)["content"]
    )
    assert (
        main(
            [
                "tasks",
                "logs",
                dag_id,
                "run/../中文",
                task_id,
                "--database-url",
                store.database_url,
            ]
        )
        == 0
    )
    assert "safe-log" in capsys.readouterr().out


@pytest.mark.parametrize("task_ids", [("same", "a/../same"), ("aaA", "aa[")])
def test_distinct_task_ids_do_not_share_log_files(tmp_path, task_ids):
    with DAG("logs") as dag:
        PythonOperator(task_id=task_ids[0], python_callable=lambda: print("first-log"))
        PythonOperator(task_id=task_ids[1], python_callable=lambda: print("second-log"))
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    runner.create_manual_run(dag, file_path=None, logical_date=AT, run_id="logs")
    assert runner.run_existing(dag, run_id="logs").state.value == "success"
    first = Path(store.latest_task_log_path("logs", "logs", task_ids[0]))
    second = Path(store.latest_task_log_path("logs", "logs", task_ids[1]))
    assert (
        first.resolve().as_posix().casefold() != second.resolve().as_posix().casefold()
    )
    assert first.read_text().strip() == "first-log"
    assert second.read_text().strip() == "second-log"


@pytest.mark.parametrize("executor", ["test", "sequential", "local"])
def test_xcom_push_and_each_pull_are_json_snapshots(tmp_path, executor):
    path = tmp_path / "snapshots.py"
    path.write_text(
        "from easyflow.sdk import DAG, task, get_current_context\n"
        "with DAG('snapshots') as dag:\n"
        "    @task\n    def push():\n"
        "        ti = get_current_context()['ti']\n"
        "        value = {'value': [1], 1: (1, 2)}\n"
        "        ti.xcom_push(key='snapshot', value=value)\n"
        "        value['value'][0] = 2\n"
        "        expected = {'value': [1], '1': [1, 2]}\n"
        "        first = ti.xcom_pull(task_ids='push', key='snapshot')\n"
        "        assert first == expected\n"
        "        first['value'][0] = 3\n"
        "        assert ti.xcom_pull(task_ids='push', key='snapshot') == expected\n"
        "    @task\n    def read():\n"
        "        ti = get_current_context()['ti']\n"
        "        value = ti.xcom_pull(task_ids='push', key='snapshot')\n"
        "        value['value'][0] = 4\n"
        "        return ti.xcom_pull(task_ids='push', key='snapshot')\n"
        "    push() >> read()\n",
        encoding="utf-8",
    )
    dag = load_dag_from_file(path, "snapshots")
    expected = {"value": [1], "1": [1, 2]}
    if executor == "test":
        result = dag.test(AT)
        assert result.success
        assert result.task_results["read"] == expected
    else:
        store = SQLiteStateStore(str(tmp_path / "state.db"))
        runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
        runner.create_manual_run(
            dag, file_path=path, logical_date=AT, run_id="snapshots"
        )
        result = runner.run_existing(
            dag, run_id="snapshots", file_path=path, executor=executor
        )
        assert result.state.value == "success"
        assert store.pull_xcom("snapshots", "snapshots", "push", "snapshot") == expected
        assert (
            store.pull_xcom("snapshots", "snapshots", "read", "return_value")
            == expected
        )


@pytest.mark.parametrize("executor", ["test", "sequential", "local"])
@pytest.mark.parametrize("kind", ["invalid", "oversize", "unicode"])
def test_xcom_rejects_bad_push_immediately_without_overwriting(
    tmp_path, executor, kind
):
    path = tmp_path / "demo.py"
    bad_value = {
        "invalid": "object()",
        "oversize": "'中' * 400_000",
        "unicode": ascii(chr(0xD800)),
    }[kind]
    dag = write_simple_dag(
        path,
        body=(
            "from easyflow.sdk import get_current_context\n"
            "from easyflow.sdk.exceptions import XComSerializationError, XComValueTooLargeError\n"
            "ti = get_current_context()['ti']\nti.xcom_push(key='key', value={'ok': 1})\n"
            f"try:\n    ti.xcom_push(key='key', value={bad_value})\n"
            "except (XComSerializationError, XComValueTooLargeError):\n    pass\n"
            "else:\n    raise AssertionError('push must reject immediately')\n"
            "assert ti.xcom_pull(task_ids='work', key='key') == {'ok': 1}\nreturn 42"
        ),
    )
    if executor == "test":
        assert dag.test(AT).success
    else:
        store = SQLiteStateStore(str(tmp_path / "state.db"))
        runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
        runner.create_manual_run(
            dag, file_path=path, logical_date=AT, run_id="validation"
        )
        result = runner.run_existing(
            dag, run_id="validation", file_path=path, executor=executor
        )
        assert result.state.value == "success"
        assert store.pull_xcom("demo", "validation", "work", "key") == {"ok": 1}


def test_chain_pairs_lists_and_keeps_other_branch_independent(tmp_path):
    def fail():
        raise RuntimeError("expected")

    with DAG("chain") as dag:
        a = PythonOperator(task_id="a", python_callable=fail)
        b = EmptyOperator(task_id="b")
        c = EmptyOperator(task_id="c")
        d = EmptyOperator(task_id="d")
        chain([a, b], [c, d])
    assert c.upstream_task_ids == {"a"}
    assert d.upstream_task_ids == {"b"}
    test_result = dag.test(AT)
    assert test_result.task_states["c"].value == "upstream_failed"
    assert test_result.task_states["d"].value == "success"
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    runner.create_manual_run(dag, file_path=None, logical_date=AT, run_id="chain")
    result = runner.run_existing(dag, run_id="chain")
    assert result.task_states == test_result.task_states


@pytest.mark.parametrize("sizes", [(2, 1), (1, 2)])
def test_chain_rejects_mismatched_lengths_before_changing_graph(sizes):
    with DAG("lengths"):
        start = EmptyOperator(task_id="start")
        left = [EmptyOperator(task_id=f"left{i}") for i in range(sizes[0])]
        right = [EmptyOperator(task_id=f"right{i}") for i in range(sizes[1])]
        with pytest.raises(ValueError, match="长度"):
            chain(start, left, right)
    assert not start.downstream_task_ids
    assert all(
        not item.upstream_task_ids and not item.downstream_task_ids
        for item in left + right
    )


def test_chain_keeps_fan_in_fan_out_xcom_args_and_middle_iterator():
    with DAG("mixed_chain") as dag:
        start, a, b, c, d, end = [
            EmptyOperator(task_id=name) for name in ("start", "a", "b", "c", "d", "end")
        ]
        chain(start, (item.output for item in [a, b]), (c, d), end)
    assert start.downstream_task_ids == {"a", "b"}
    assert c.upstream_task_ids == {"a"}
    assert d.upstream_task_ids == {"b"}
    assert end.upstream_task_ids == {"c", "d"}
    assert dag.test(AT).success
