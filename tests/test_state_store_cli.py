from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

import pytest

from easyflow import cli
from easyflow._internal.dag_parser import load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.scheduler import ScheduledRunSummary
from easyflow._internal.state_store import SQLiteStateStore, path_from_database_url
from easyflow.cli import main
from easyflow.sdk.states import DagRunState


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        ([], ("scheduler", "dags")),
        (["dags"], ("list", "trigger")),
        (["tasks", "clear"], ("--run-id", "--dry-run")),
    ],
)
def test_cli_help_shows_commands_and_options(capsys, args, expected):
    assert main([*args, "--help"]) == 0
    output = capsys.readouterr().out
    assert "Usage: easyflow" in output
    for entry in expected:
        assert entry in output


@pytest.mark.parametrize("option", ["--dry-run", "--yes"])
def test_cli_clear_rejects_nonexistent_run_without_task_id(tmp_path, capsys, option):
    status = main([
        "tasks", "clear", "demo", "--run-id", "missing", option,
        "--database-url", str(tmp_path / "state.db"),
    ])
    captured = capsys.readouterr()
    assert status == 1 and "DagRun 不存在" in captured.err
    assert "EXECUTED" not in captured.out


@pytest.mark.parametrize("change", ["main", "dependency", "during_clear"])
def test_cli_clear_checks_definition_before_deleting_xcom(tmp_path, capsys, monkeypatch, change):
    path = tmp_path / "workflow.py"
    helper = tmp_path / "cli_clear_values.py"
    helper.write_text("VALUE = 'original'\n", encoding="utf-8")
    path.write_text(
        """from easyflow.sdk import DAG, task
from cli_clear_values import VALUE
with DAG('demo') as dag:
    @task
    def work():
        return VALUE
    work()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    dag = load_dag_from_file(path, "demo")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(dag, file_path=path, logical_date=datetime.now(UTC))
    assert runner.run_existing(dag, run_id=run_id, file_path=path).state == DagRunState.SUCCESS
    args = ["tasks", "clear", "demo", "--run-id", run_id, "--database-url", store.database_url]
    target = path if change == "main" else helper
    original = target.read_text(encoding="utf-8")
    modified = original + "\n# 修改定义\n"
    if change == "during_clear":
        assert main([*args, "--dry-run"]) == 0
        capsys.readouterr()
        check_clear = SQLiteStateStore._check_clear

        def change_inside_transaction(self, connection, *args, dry_run, **kwargs):
            if not dry_run:
                assert connection.in_transaction
                target.write_text(modified, encoding="utf-8")
            return check_clear(self, connection, *args, dry_run=dry_run, **kwargs)

        with monkeypatch.context() as context:
            context.setattr(SQLiteStateStore, "_check_clear", change_inside_transaction)
            assert main([*args, "--yes"]) == 1
            assert "不能清理后重跑" in capsys.readouterr().err
    else:
        target.write_text(modified, encoding="utf-8")
        for option in ("--dry-run", "--yes"):
            assert main([*args, option]) == 1
            captured = capsys.readouterr()
            assert "不能清理后重跑" in captured.err and "clear candidates" not in captured.out
    assert store.pull_xcom("demo", run_id, "work", "return_value") == "original"
    assert store.get_dag_run("demo", run_id).state == "success"
    assert store.get_task_state("demo", run_id, "work") == "success"
    assert not store.list_audit_actions()

    target.write_text(original, encoding="utf-8")
    assert main([*args, "--yes"]) == 0
    assert runner.run_existing(dag, run_id=run_id, file_path=path).state == DagRunState.SUCCESS
    assert store.pull_xcom("demo", run_id, "work", "return_value") == "original"


@pytest.mark.parametrize("other_dag_failed", [False, True])
def test_finite_scheduler_exit_uses_last_state_per_run(tmp_path, monkeypatch, capsys, other_dag_failed):
    summaries = [
        ScheduledRunSummary("shared", DagRunState.RUNNING, "a"),
        ScheduledRunSummary("shared", DagRunState.SUCCESS, "a"),
    ]
    if other_dag_failed:
        summaries.insert(1, ScheduledRunSummary("shared", DagRunState.FAILED, "b"))
    monkeypatch.setattr(cli, "_run_scheduler_loop_for_sources", lambda args: (summaries, []))
    code = main(["scheduler", "--max-loops", "2", "--database-url", str(tmp_path / "state.db")])
    assert code == int(other_dag_failed)
    assert capsys.readouterr().out.splitlines()[-1] == "a shared success"


def test_sqlite_state_store_init_creates_core_tables(tmp_path) -> None:
    db_path = tmp_path / "easyflow.db"
    store = SQLiteStateStore(f"sqlite:///{db_path}")
    store.init_db()
    assert {
        "dag_definition",
        "dag_run",
        "task_instance",
        "task_try",
        "xcom",
        "scheduler_lease",
        "task_heartbeat",
        "backfill",
        "audit_log",
    }.issubset(set(store.list_tables()))

    with sqlite3.connect(db_path) as connection:
        indexes = {
            row[1]
            for row in connection.execute("PRAGMA index_list('dag_run')").fetchall()
        }
    assert "uq_scheduled_dag_run" in indexes
    assert "uq_backfill_dag_run" in indexes


def test_sqlite_url_relative_and_absolute_paths(tmp_path) -> None:
    assert path_from_database_url("sqlite:///./.easyflow/easyflow.db").as_posix() == (
        ".easyflow/easyflow.db"
    )
    absolute = tmp_path / "absolute.db"
    assert path_from_database_url(f"sqlite:///{absolute}") == absolute


def test_db_init_cli(tmp_path, capsys) -> None:
    db_path = tmp_path / "cli.db"
    exit_code = main(["db", "init", "--database-url", f"sqlite:///{db_path}"])
    assert exit_code == 0
    assert db_path.exists()
    assert "Initialized easyflow SQLite database" in capsys.readouterr().out
