from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from easyflow._internal.dag_parser import load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.scheduler import create_backfill, preview_backfill
from easyflow._internal.schedules import backfill_intervals, scheduler_intervals
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.cli import main
from easyflow.providers.standard.operators.empty import EmptyOperator
from easyflow.sdk import DAG
from easyflow.web.app import create_app

AT = datetime(2026, 7, 1, tzinfo=UTC)


def make_run(tmp_path, *, task_id="work", run_id="manual", log_root=None):
    with DAG("demo") as dag:
        EmptyOperator(task_id=task_id)
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    runner = PersistentDagRunner(store, **({"log_root": log_root} if log_root else {}))
    runner.create_manual_run(dag, file_path=None, logical_date=AT, run_id=run_id)
    return dag, store, runner


@pytest.mark.parametrize("live_lease", [False, True])
def test_definition_change_retires_expired_attempt_and_allows_clear(
    tmp_path, live_lease
):
    path = tmp_path / "demo.py"
    original = (
        "from easyflow.sdk import DAG\n"
        "from easyflow.providers.standard.operators.empty import EmptyOperator\n"
        'with DAG("demo") as dag:\n    EmptyOperator(task_id="removed")\n'
    )
    path.write_text(original)
    dag = load_dag_from_file(path, "demo")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    runner.create_manual_run(dag, file_path=path, logical_date=AT, run_id="old")
    store.start_task_try(
        "demo",
        "old",
        "removed",
        lease_seconds=3600,
        now=datetime.now(UTC) if live_lease else AT,
    )
    path.write_text('from easyflow.sdk import DAG\ndag = DAG("demo")\n')
    changed = load_dag_from_file(path, "demo")
    result = runner.run_existing(changed, file_path=path, run_id="old")
    if live_lease:
        assert result.state.value in {"queued", "running"}
        assert store.get_task_state("demo", "old", "removed").value == "running"
        with sqlite3.connect(store.path) as db:
            db.execute(
                "UPDATE task_instance SET lease_expires_at = ?", (AT.isoformat(),)
            )
        result = runner.run_existing(changed, file_path=path, run_id="old")
    assert result.state.value == "failed"
    assert result.task_states["removed"].value == "failed"
    with sqlite3.connect(store.path) as db:
        state, ended, error = db.execute(
            "SELECT state, ended_at, error_code FROM task_try"
        ).fetchone()
        assert state == "failed" and ended and error == "DAG_DEFINITION_CHANGED"
        assert db.execute(
            "SELECT lease_expires_at, executor_id, worker_pid FROM task_instance"
        ).fetchone() == (None, None, None)
    assert ("dag_definition_changed", "demo", "old") in store.list_audit_actions()
    path.write_text(original)
    assert store.clear_task_instances(dag_id="demo", run_id="old").to_clear == [
        "removed"
    ]
    restored = load_dag_from_file(path, "demo")
    assert (
        runner.run_existing(restored, file_path=path, run_id="old").state.value
        == "success"
    )
    with sqlite3.connect(store.path) as db:
        assert db.execute(
            "SELECT state FROM task_try ORDER BY try_number"
        ).fetchall() == [
            ("failed",),
            ("success",),
        ]


def test_retry_clears_instance_end_but_preserves_attempt_history(tmp_path):
    _dag, store, _ = make_run(tmp_path)
    assert store.start_task_try("demo", "manual", "work") == 1
    store.commit_failure(
        dag_id="demo",
        run_id="manual",
        task_id="work",
        try_number=1,
        error=RuntimeError("retry"),
        retries=1,
    )
    with sqlite3.connect(store.path) as db:
        previous_end = db.execute("SELECT ended_at FROM task_try").fetchone()[0]
    assert previous_end
    assert store.start_task_try("demo", "manual", "work") == 2
    with sqlite3.connect(store.path) as db:
        state, started, ended = db.execute(
            "SELECT state, started_at, ended_at FROM task_instance"
        ).fetchone()
        assert state == "running" and started and ended is None
        assert db.execute(
            "SELECT ended_at FROM task_try ORDER BY try_number"
        ).fetchall() == [
            (previous_end,),
            (None,),
        ]


@pytest.mark.parametrize("zone", [UTC, ZoneInfo("Asia/Shanghai")])
def test_weekly_runs_on_sunday_in_dag_timezone(zone):
    sunday = datetime(2026, 7, 5, tzinfo=zone)
    dag = DAG(
        "weekly",
        schedule="@weekly",
        start_date=sunday - timedelta(days=1),
        catchup=True,
    )
    assert scheduler_intervals(dag, sunday - timedelta(microseconds=1)) == []
    assert [i.logical_date for i in scheduler_intervals(dag, sunday)] == [sunday]
    assert [
        i.logical_date
        for i in scheduler_intervals(
            dag,
            sunday + timedelta(days=7),
            last_automated_date=sunday,
        )
    ] == [sunday + timedelta(days=7)]
    assert [
        i.logical_date
        for i in backfill_intervals(
            "@weekly",
            date(2026, 7, 5),
            date(2026, 7, 6),
            zone,
        )
    ] == [sunday]


@pytest.mark.parametrize("executor", ["sequential", "local"])
@pytest.mark.parametrize("log_root", [None, "relative-logs"])
def test_logs_survive_working_directory_change(
    tmp_path, monkeypatch, capsys, executor, log_root
):
    producer = tmp_path / "producer"
    consumer = tmp_path / "consumer"
    producer.mkdir()
    consumer.mkdir()
    path = producer / "demo.py"
    path.write_text(
        'from easyflow.sdk import DAG, task\nwith DAG("demo") as dag:\n'
        '    @task\n    def work():\n        print("cross-directory-log")\n    work()\n'
    )
    monkeypatch.chdir(producer)
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    runner = PersistentDagRunner(store, **({"log_root": log_root} if log_root else {}))
    expected_root = producer / (log_root or ".easyflow/logs")
    assert runner.log_root == expected_root
    dag = load_dag_from_file(path, "demo")
    runner.create_manual_run(dag, file_path=path, logical_date=AT, run_id="log")
    monkeypatch.chdir(consumer)
    assert (
        runner.run_existing(
            dag, file_path=path, run_id="log", executor=executor
        ).state.value
        == "success"
    )
    saved = Path(store.latest_task_log_path("demo", "log", "work"))
    assert (
        saved.is_absolute() and saved.is_relative_to(expected_root) and saved.is_file()
    )
    assert (
        main(
            [
                "tasks",
                "logs",
                "demo",
                "log",
                "work",
                "--database-url",
                store.database_url,
            ]
        )
        == 0
    )
    assert "cross-directory-log" in capsys.readouterr().out
    app = create_app(
        dags_folder=str(producer),
        database_url=store.database_url,
        log_root=str(expected_root),
    )
    with TestClient(app, base_url="http://127.0.0.1") as client:
        login_admin(app, client)
        response = client.get(
            "/api/history/task-log",
            params={"dag_id": "demo", "run_id": "log", "task_id": "work"},
        )
        assert response.status_code == 200
        assert "cross-directory-log" in response.json()["content"]


def login_admin(app, client):
    app.state.auth.create_user("admin", "administrator-password", "admin")
    response = client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "administrator-password"},
    )
    assert response.status_code == 200
    client.headers["X-CSRF-Token"] = response.json()["csrf_token"]


def test_slash_ids_support_details_logs_clear_and_authorization(tmp_path):
    run_id, task_id = "custom/run +%?#中文", "group/task +%?#中文"
    dag, store, runner = make_run(
        tmp_path, task_id=task_id, run_id=run_id, log_root=tmp_path / "logs"
    )
    assert runner.run_existing(dag, run_id=run_id).state.value == "success"
    app = create_app(
        dags_folder=str(tmp_path / "dags"),
        database_url=store.database_url,
        log_root=str(tmp_path / "logs"),
    )
    identity = {"dag_id": "demo", "run_id": run_id}
    body = {"task_ids": [task_id]}
    with TestClient(app, base_url="http://127.0.0.1") as client:
        assert (
            client.post("/api/runs/clear", params=identity, json=body).status_code
            == 401
        )
        login_admin(app, client)
        detail = client.get("/api/history/run", params=identity)
        assert (
            detail.status_code == 200 and detail.json()["tasks"][task_id] == "success"
        )
        logs = client.get(
            "/api/history/task-log",
            params={**identity, "task_id": task_id, "attempt": 1},
        )
        assert logs.status_code == 200 and logs.json()["selected"] == 1
        assert logs.json()["content"] is not None
        for endpoint in ("clear-preview", "clear"):
            assert (
                client.post(
                    f"/api/runs/{endpoint}",
                    params={**identity, "run_id": "missing"},
                    json=body,
                ).status_code
                == 404
            )
            assert (
                client.post(
                    f"/api/runs/{endpoint}",
                    params=identity,
                    json=body,
                    headers={"X-CSRF-Token": "wrong"},
                ).status_code
                == 403
            )
        preview = client.post("/api/runs/clear-preview", params=identity, json=body)
        assert preview.status_code == 200 and preview.json() == {
            "to_clear": [task_id],
            "blocked": [],
        }
        cleared = client.post("/api/runs/clear", params=identity, json=body)
        assert cleared.status_code == 200 and cleared.json()["to_clear"] == [task_id]
        assert store.start_task_try("demo", run_id, task_id) == 2
        assert (
            client.post("/api/runs/clear", params=identity, json=body).status_code
            == 409
        )
        app.state.auth.create_user("reader", "readonly-password", "viewer")
        response = client.post(
            "/api/auth/login",
            json={"username": "reader", "password": "readonly-password"},
        )
        client.headers["X-CSRF-Token"] = response.json()["csrf_token"]
        for endpoint in ("clear-preview", "clear"):
            assert (
                client.post(
                    f"/api/runs/{endpoint}", params=identity, json=body
                ).status_code
                == 403
            )


@pytest.mark.parametrize("zone", [UTC, ZoneInfo("Asia/Shanghai")])
def test_backfill_limits_apply_after_dag_bounds(tmp_path, zone):
    start = datetime(2026, 7, 1, tzinfo=zone)
    dag = DAG(
        "minute",
        schedule="* * * * *",
        start_date=start,
        end_date=start + timedelta(minutes=2),
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    preview = preview_backfill(
        dag=dag, store=store, start_date=date(2026, 6, 30), end_date=date(2026, 7, 2)
    )
    assert [i.logical_date for i in preview] == [
        start + timedelta(minutes=i) for i in range(3)
    ]
    assert (
        preview_backfill(
            dag=dag,
            store=store,
            start_date=date(2026, 6, 29),
            end_date=date(2026, 6, 30),
        )
        == []
    )
    assert (
        preview_backfill(
            dag=dag, store=store, start_date=date(2026, 7, 2), end_date=date(2026, 7, 3)
        )
        == []
    )
    with pytest.raises(ValueError, match="end-date"):
        preview_backfill(
            dag=dag,
            store=store,
            start_date=date(2026, 7, 3),
            end_date=date(2026, 6, 29),
        )
    dag.end_date = None
    assert (
        len(
            preview_backfill(
                dag=dag,
                store=store,
                start_date=date(2026, 6, 30),
                end_date=start + timedelta(minutes=2),
            )
        )
        == 3
    )
    with pytest.raises(ValueError, match="1000"):
        preview_backfill(
            dag=dag,
            store=store,
            start_date=start,
            end_date=start + timedelta(minutes=1000),
        )


def test_backfill_creation_enumerates_only_intersecting_range(tmp_path, monkeypatch):
    from easyflow._internal import schedules

    path = tmp_path / "minute.py"
    path.write_text(
        "from datetime import datetime, timezone\nfrom easyflow.sdk import DAG\n"
        'dag = DAG("minute", schedule="* * * * *", start_date=datetime(2026, 7, 1, tzinfo=timezone.utc), '
        "end_date=datetime(2026, 7, 1, 0, 2, tzinfo=timezone.utc))\n"
    )
    original = schedules.next_logical_date
    calls = 0

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        assert calls <= 3, "不应枚举 DAG 边界之外的区间"
        return original(*args, **kwargs)

    monkeypatch.setattr(schedules, "next_logical_date", counted)
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    result = create_backfill(
        dag_id="minute",
        file_path=path,
        store=store,
        start_date=date(2026, 6, 30),
        end_date=date(2026, 7, 2),
        log_root=tmp_path / "logs",
    )
    assert result.complete and len(result.runs) == 3 and calls == 2


def test_clipping_preserves_delta_anchor_and_dst_instant(tmp_path):
    zone = ZoneInfo("America/New_York")
    start = datetime(2026, 11, 1, 1, 0, tzinfo=zone, fold=1)
    dag = DAG(
        "fold",
        schedule=timedelta(minutes=30),
        start_date=start,
        end_date=start + timedelta(hours=1),
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    preview = preview_backfill(
        dag=dag,
        store=store,
        start_date=datetime(2026, 11, 1, 1, 45, tzinfo=zone, fold=0),
        end_date=date(2026, 11, 2),
    )
    assert [i.logical_date.astimezone(UTC) for i in preview] == [
        start.astimezone(UTC) + timedelta(minutes=30 * i) for i in range(3)
    ]
    dag = DAG(
        "delta",
        schedule=timedelta(minutes=7),
        start_date=AT + timedelta(minutes=1),
        end_date=AT + timedelta(minutes=20),
    )
    preview = preview_backfill(
        dag=dag,
        store=store,
        start_date=AT + timedelta(minutes=2),
        end_date=date(2026, 7, 2),
    )
    assert [i.logical_date for i in preview] == [
        AT + timedelta(minutes=8),
        AT + timedelta(minutes=15),
    ]
