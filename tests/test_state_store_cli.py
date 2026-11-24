from __future__ import annotations

import sqlite3

import pytest

from easyflow import cli
from easyflow._internal.scheduler import ScheduledRunSummary
from easyflow._internal.state_store import SQLiteStateStore, path_from_database_url
from easyflow.cli import main
from easyflow.sdk.states import DagRunState


@pytest.mark.parametrize("option", ["--dry-run", "--yes"])
def test_cli_clear_rejects_nonexistent_run_without_task_id(tmp_path, capsys, option):
    status = main([
        "tasks", "clear", "demo", "--run-id", "missing", option,
        "--database-url", str(tmp_path / "state.db"),
    ])
    captured = capsys.readouterr()
    assert status == 1 and "DagRun 不存在" in captured.err
    assert "EXECUTED" not in captured.out


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
