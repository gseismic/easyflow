from __future__ import annotations

import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from easyflow._internal.dag_parser import load_dag_from_file, load_dags_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.state_store import SQLiteStateStore, task_retry_settings
from easyflow.cli import main
from easyflow.providers.standard.operators.bash import BashOperator
from easyflow.providers.standard.operators.empty import EmptyOperator
from easyflow.providers.standard.operators.python import PythonOperator
from easyflow.sdk import DAG, TriggerRule, task
from easyflow.sdk.states import DagRunState, TaskInstanceState

UTC = timezone.utc
NOW = datetime(2026, 7, 6, tzinfo=UTC)


def test_retry_delay_backoff_and_clear_resets_budget(tmp_path):
    def fail():
        raise RuntimeError("failed")

    with DAG("retry") as dag:
        PythonOperator(
            task_id="fail",
            python_callable=fail,
            retries=2,
            retry_delay=timedelta(seconds=0.12),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(seconds=0.18),
        )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run = runner.create_manual_run(dag, file_path=None, logical_date=NOW)
    assert runner.run_existing(dag, run_id=run).state.value == "failed"
    with sqlite3.connect(store.path) as db:
        starts = [
            datetime.fromisoformat(row[0])
            for row in db.execute("SELECT started_at FROM task_try ORDER BY try_number")
        ]
    assert len(starts) == 3
    assert (
        "RuntimeError: failed"
        in Path(store.latest_task_log_path("retry", run, "fail")).read_text()
    )
    assert (starts[1] - starts[0]).total_seconds() >= 0.10
    assert (starts[2] - starts[1]).total_seconds() >= 0.16
    assert store.clear_task_instances(dag_id="retry", run_id=run).to_clear == ["fail"]
    assert runner.run_existing(dag, run_id=run).state.value == "failed"
    with sqlite3.connect(store.path) as db:
        assert [
            row[0]
            for row in db.execute("SELECT try_number FROM task_try ORDER BY try_number")
        ] == list(range(1, 7))


def test_retry_at_is_persisted_and_enforced(tmp_path):
    with DAG("retry_at") as dag:
        EmptyOperator(task_id="work")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    run = PersistentDagRunner(store).create_manual_run(
        dag, file_path=None, logical_date=NOW
    )
    assert store.start_task_try("retry_at", run, "work") == 1
    store.commit_failure(
        dag_id="retry_at",
        run_id=run,
        task_id="work",
        try_number=1,
        error=ValueError("retry"),
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    assert store.task_retry_at("retry_at", run, "work") > datetime.now(UTC)
    assert store.start_task_try("retry_at", run, "work") is None


def test_explicit_no_backoff_is_not_overridden_by_default_args():
    with DAG("retry_options", default_args={"retry_exponential_backoff": True}) as dag:
        operator = EmptyOperator(task_id="work", retry_exponential_backoff=False)
    assert task_retry_settings(dag, operator)[1] == 1.0


def test_clear_rechecks_under_write_lock_and_preserves_xcom(tmp_path):
    with DAG("clear") as dag:
        EmptyOperator(task_id="work")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    run = PersistentDagRunner(store).create_manual_run(
        dag, file_path=None, logical_date=NOW
    )
    store.start_task_try("clear", run, "work")
    store.commit_failure(
        dag_id="clear",
        run_id=run,
        task_id="work",
        try_number=1,
        error=ValueError(),
        retries=1,
    )
    entered = threading.Event()
    result = []
    with sqlite3.connect(store.path) as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            "INSERT INTO xcom(dag_id,run_id,task_id,map_index,key,value_json,created_at) VALUES ('clear', ?, 'work', -1, 'k', '1', ?)",
            (run, datetime.now(UTC).isoformat()),
        )
        db.execute(
            "UPDATE task_instance SET state='running' WHERE dag_id='clear' AND run_id=?",
            (run,),
        )

        def clear():
            entered.set()
            result.append(store.clear_task_instances(dag_id="clear", run_id=run))

        thread = threading.Thread(target=clear)
        thread.start()
        assert entered.wait(1)
        time.sleep(0.1)
    thread.join(5)
    assert result[0].blocked[0].state == "running"
    assert store.pull_xcom("clear", run, "work", "k") == 1
    assert store.get_task_state("clear", run, "work") == TaskInstanceState.RUNNING


def test_leaf_outcome_and_xcom_disabled_match_test_runner(tmp_path):
    def fail():
        raise RuntimeError("expected")

    with DAG("leaves") as dag:
        upstream = PythonOperator(task_id="fail", python_callable=fail)
        cleanup = PythonOperator(
            task_id="cleanup",
            python_callable=lambda: object(),
            trigger_rule=TriggerRule.ALL_DONE,
            do_xcom_push=False,
        )
        skipped = EmptyOperator(task_id="skipped", trigger_rule=TriggerRule.ONE_FAILED)
        success = EmptyOperator(task_id="success")
        upstream >> cleanup
        success >> skipped
    test_result = dag.test(NOW)
    assert (
        test_result.success
        and test_result.task_states["skipped"] == TaskInstanceState.SKIPPED
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run = runner.create_manual_run(dag, file_path=None, logical_date=NOW)
    result = runner.run_existing(dag, run_id=run)
    assert result.state.value == "success"
    with pytest.raises(KeyError):
        store.pull_xcom("leaves", run, "cleanup", "return_value")


def test_xcom_objects_are_isolated_and_sets_resolve_in_both_runners(tmp_path):
    with DAG("xcom") as dag:

        @task
        def source():
            return {"v": 0}

        @task
        def mutate(value):
            value["v"] = 42

        @task
        def read(value):
            return value["v"]

        @task
        def number():
            return 7

        @task
        def sum_set(values):
            return sum(values)

        value = source()
        changed = mutate(value)
        reader = read(value)
        changed >> reader
        sum_set({number()})
    expected = dag.test(NOW)
    assert expected.task_results["read"] == 0 and expected.task_results["sum_set"] == 7
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run = runner.create_manual_run(dag, file_path=None, logical_date=NOW)
    assert runner.run_existing(dag, run_id=run).state.value == "success"
    assert store.pull_xcom("xcom", run, "read", "return_value") == 0
    assert store.pull_xcom("xcom", run, "sum_set", "return_value") == 7


def test_error_and_template_traceback_in_logs_and_run_id_paths_unique(tmp_path, capsys):
    with DAG("logs") as dag:
        BashOperator(task_id="bad", bash_command="{{ missing_variable }}")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    paths = []
    for run_id in ("x:y", "x_y"):
        runner.create_manual_run(dag, file_path=None, logical_date=NOW, run_id=run_id)
        assert runner.run_existing(dag, run_id=run_id).state.value == "failed"
        paths.append(store.latest_task_log_path("logs", run_id, "bad"))
        assert (
            main(
                [
                    "tasks",
                    "logs",
                    "logs",
                    run_id,
                    "bad",
                    "--database-url",
                    store.database_url,
                ]
            )
            == 0
        )
        assert "missing_variable" in capsys.readouterr().out
    assert paths[0] != paths[1]


def test_bash_explicit_empty_env_and_bash_syntax(monkeypatch):
    monkeypatch.setenv("EASYFLOW_SECRET_FOR_TEST", "secret")
    with DAG("bash") as dag:
        BashOperator(
            task_id="empty",
            env={},
            append_env=False,
            bash_command='a=(one two); [[ ${a[1]} == two ]] && printf "%s" "${EASYFLOW_SECRET_FOR_TEST-unset}"',
        )
        BashOperator(
            task_id="inherited", bash_command='printf "%s" "$EASYFLOW_SECRET_FOR_TEST"'
        )
    result = dag.test(NOW)
    assert result.success
    assert result.task_results["empty"] == "unset"
    assert result.task_results["inherited"] == "secret"


def test_explicit_dag_must_match_context():
    other = DAG("other")
    with DAG("outer"):
        with pytest.raises(ValueError, match="context"):
            EmptyOperator(task_id="wrong", dag=other)
    assert other.tasks == []


def test_file_with_invalid_dag_keeps_valid_one(tmp_path, capsys):
    path = tmp_path / "mixed.py"
    path.write_text("""
from easyflow.sdk import DAG, task
with DAG("good", schedule="@daily") as good:
    @task
    def work(): return "ok"
    work()
bad = DAG("bad", schedule="bogus")
""")
    assert load_dag_from_file(path, "good").dag_id == "good"
    assert [source.dag_id for source in load_dags_from_file(path)] == ["good"]
    assert main(["dags", "list", "--file", str(path)]) == 0
    output = capsys.readouterr()
    assert "good" in output.out and "bad" in output.err
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    dag = load_dag_from_file(path, "good")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run = runner.create_manual_run(dag, file_path=path, logical_date=NOW)
    assert (
        runner.run_existing(
            dag, run_id=run, file_path=path, executor="local"
        ).state.value
        == "success"
    )


def test_local_worker_startup_error_does_not_restart_forever(tmp_path):
    path = tmp_path / "startup.py"
    path.write_text(f"""
import os
if os.getpid() != {os.getpid()}: raise RuntimeError("worker import failed")
from easyflow.sdk import DAG, task
with DAG("startup") as dag:
    @task
    def work(): return 1
    work()
""")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    dag = load_dag_from_file(path, "startup")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run = runner.create_manual_run(dag, file_path=path, logical_date=NOW)
    result = runner.run_existing(dag, run_id=run, file_path=path, executor="local")
    assert result.state.value == "failed"
    with sqlite3.connect(store.path) as db:
        assert db.execute("SELECT error_code FROM task_try").fetchall() == [
            ("WORKER_STARTUP_FAILED",)
        ]
    assert (
        "exitcode"
        in Path(store.latest_task_log_path("startup", run, "work")).read_text()
    )


def test_local_worker_rejects_changed_source_between_tasks(tmp_path):
    ready, output = tmp_path / "ready", tmp_path / "output"
    path = tmp_path / "changed.py"
    source = f"""
from pathlib import Path
import time
from easyflow.sdk import DAG, task
with DAG("changed") as dag:
    @task
    def first():
        Path({str(ready)!r}).write_text("ready")
        time.sleep(0.8)
    @task
    def last(): Path({str(output)!r}).write_text("OLD")
    first() >> last()
"""
    path.write_text(source)
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    dag = load_dag_from_file(path, "changed")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run = runner.create_manual_run(dag, file_path=path, logical_date=NOW)
    results = []
    thread = threading.Thread(
        target=lambda: results.append(
            runner.run_existing(dag, run_id=run, file_path=path, executor="local")
        )
    )
    thread.start()
    deadline = time.monotonic() + 5
    while not ready.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert ready.exists()
    path.write_text(source.replace('"OLD"', '"NEW"'))
    thread.join(8)
    assert not thread.is_alive() and results[0].state.value == "failed"
    assert not output.exists()
    assert ("dag_definition_changed", "changed", run) in store.list_audit_actions()


def test_clear_reactivation_respects_capacity_and_finish_rechecks_state(tmp_path):
    with DAG("clear_capacity") as dag:
        EmptyOperator(task_id="work")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    first = runner.create_manual_run(
        dag, file_path=None, logical_date=NOW, run_id="first"
    )
    assert runner.run_existing(dag, run_id=first).state.value == "success"
    second = runner.create_manual_run(
        dag, file_path=None, logical_date=NOW, run_id="second"
    )
    blocked = store.clear_task_instances(dag_id=dag.dag_id, run_id=first)
    assert blocked.blocked and "max_active_runs" in blocked.blocked[0].reason
    assert store.get_dag_run(dag.dag_id, first).state == "success"
    store.set_dag_run_state(dag.dag_id, second, DagRunState.RUNNING)
    store.start_task_try(dag.dag_id, second, "work")
    store.commit_task_terminal(
        dag_id=dag.dag_id, run_id=second, task_id="work", try_number=1, xcom_values={}
    )
    assert store.clear_task_instances(dag_id=dag.dag_id, run_id=second).to_clear == [
        "work"
    ]
    assert store.finish_dag_run(dag, second).value == "running"
    assert store.get_task_state(dag.dag_id, second, "work") is None


def test_existing_database_upgrades_with_utc_fallback(tmp_path, capsys):
    from easyflow._internal.state_store import SCHEMA

    path = tmp_path / "legacy.db"
    old_log = tmp_path / "legacy-run" / "1.log"
    old_log.parent.mkdir()
    old_log.write_text("旧日志内容\n")
    with sqlite3.connect(path) as db:
        db.executescript(SCHEMA)
        db.execute(
            "INSERT INTO dag_definition(dag_id, definition_hash, parsed_at) VALUES ('legacy', 'old', ?)",
            (NOW.isoformat(),),
        )
        db.execute(
            "INSERT INTO dag_run(dag_id,run_id,run_type,logical_date,data_interval_start,data_interval_end,state,queued_at) VALUES ('legacy','r','manual',?,?,?,'queued',?)",
            (NOW.isoformat(), NOW.isoformat(), NOW.isoformat(), NOW.isoformat()),
        )
        db.execute(
            "INSERT INTO task_instance(dag_id,run_id,task_id,map_index,state,try_number) VALUES ('legacy','r','work',-1,NULL,1)"
        )
        db.execute(
            "INSERT INTO task_try(dag_id,run_id,task_id,map_index,try_number,state,log_path) VALUES ('legacy','r','work',-1,1,'failed',?)",
            (str(old_log),),
        )
    with DAG("legacy") as dag:
        EmptyOperator(task_id="work")
    store = SQLiteStateStore(f"sqlite:///{path}")
    assert store.get_dag_run("legacy", "r").logical_timezone is None
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT retry_base FROM task_instance").fetchone()[0] == 1
    assert (
        main(
            [
                "tasks",
                "logs",
                "legacy",
                "r",
                "work",
                "--database-url",
                store.database_url,
            ]
        )
        == 0
    )
    assert "旧日志内容" in capsys.readouterr().out
    assert (
        PersistentDagRunner(store, log_root=tmp_path / "logs")
        .run_existing(dag, run_id="r")
        .state.value
        == "success"
    )
    with sqlite3.connect(path) as db:
        assert "retry_base" in {
            row[1] for row in db.execute("PRAGMA table_info(task_instance)")
        }
        assert "max_active_runs" in {
            row[1] for row in db.execute("PRAGMA table_info(dag_definition)")
        }


def test_upgrade_recovers_retry_baseline_from_clear_audit(tmp_path):
    import json

    from easyflow._internal.state_store import SCHEMA

    path = tmp_path / "legacy_retry.db"
    with sqlite3.connect(path) as db:
        db.executescript(SCHEMA)
        db.execute(
            "INSERT INTO dag_definition(dag_id,definition_hash,parsed_at) VALUES ('old_retry','old',?)",
            (NOW.isoformat(),),
        )
        db.execute(
            "INSERT INTO dag_run(dag_id,run_id,run_type,logical_date,data_interval_start,data_interval_end,state,queued_at) VALUES ('old_retry','r','manual',?,?,?,'running',?)",
            (NOW.isoformat(), NOW.isoformat(), NOW.isoformat(), NOW.isoformat()),
        )
        db.execute(
            "INSERT INTO task_instance(dag_id,run_id,task_id,map_index,state,try_number) VALUES ('old_retry','r','work',-1,'up_for_retry',3)"
        )
        for number, minutes in ((1, 0), (2, 1), (3, 4)):
            db.execute(
                "INSERT INTO task_try(dag_id,run_id,task_id,map_index,try_number,state,started_at) VALUES ('old_retry','r','work',-1,?,'failed',?)",
                (number, (NOW + timedelta(minutes=minutes)).isoformat()),
            )
        db.execute(
            "INSERT INTO audit_log(action,dag_id,run_id,payload_json,created_at) VALUES ('tasks_clear','old_retry','r',?,?)",
            (
                json.dumps({"task_ids": ["work"]}),
                (NOW + timedelta(minutes=3)).isoformat(),
            ),
        )

    def fail():
        raise RuntimeError("failed")

    with DAG("old_retry") as dag:
        PythonOperator(task_id="work", python_callable=fail, retries=2)
    store = SQLiteStateStore(f"sqlite:///{path}")
    store.init_db()
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT retry_base FROM task_instance").fetchone()[0] == 2
    assert (
        PersistentDagRunner(store, log_root=tmp_path / "logs")
        .run_existing(dag, run_id="r")
        .state.value
        == "failed"
    )
    with sqlite3.connect(path) as db:
        assert [
            row[0]
            for row in db.execute("SELECT try_number FROM task_try ORDER BY try_number")
        ] == [1, 2, 3, 4, 5]


def test_windows_run_lock_branch_handles_contention_and_release(tmp_path, monkeypatch):
    import errno

    import easyflow._internal.persistent_runner as runner_module

    class FakeWindowsLock:
        LK_NBLCK = 1
        LK_UNLCK = 2
        held = False

        def locking(self, fd, operation, length):
            assert length == 1 and os.fstat(fd).st_size >= 1
            if operation == self.LK_NBLCK:
                if self.held:
                    raise OSError(errno.EACCES, "locked")
                self.held = True
            else:
                self.held = False

    fake = FakeWindowsLock()
    monkeypatch.setattr(runner_module, "fcntl", None)
    monkeypatch.setattr(runner_module, "msvcrt", fake, raising=False)
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    runner = PersistentDagRunner(store)
    with runner._run_lock("d", "r") as acquired:
        assert acquired
        with runner._run_lock("d", "r") as concurrent:
            assert not concurrent
    assert not fake.held
    with runner._run_lock("d", "r") as acquired:
        assert acquired
