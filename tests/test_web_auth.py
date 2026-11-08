from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from easyflow.cli import app as cli
from easyflow.web.app import create_app


def client_for(tmp_path: Path):
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "demo.py").write_text(
        "from easyflow.sdk import DAG, task\n"
        'with DAG("demo") as dag:\n'
        "    @task\n"
        "    def example():\n"
        "        return 1\n"
        "    example()\n",
        encoding="utf-8",
    )
    application = create_app(
        dags_folder=str(dags),
        database_url=f"sqlite:///{tmp_path / 'state.db'}",
        log_root=str(tmp_path / "logs"),
    )
    application.state.auth.create_user("admin", "administrator-password", "admin")
    application.state.auth.create_user("reader", "readonly-password", "viewer")
    return application, TestClient(application, base_url="http://127.0.0.1:8790")


def login(client, username, password):
    response = client.post(
        "/api/auth/login", json={"username": username, "password": password}
    )
    assert response.status_code == 200
    client.headers["X-CSRF-Token"] = response.json()["csrf_token"]
    return response


def test_authentication_authorization_csrf_and_logout(tmp_path):
    app, client = client_for(tmp_path)
    assert client.get("/").status_code == 200
    assert client.get("/api/dags").status_code == 401
    assert client.get("/api/dags/demo/source").status_code == 401
    assert client.post("/api/dags/demo/trigger", json={}).status_code == 401
    assert (
        client.post(
            "/api/auth/login", json={"username": "reader", "password": "wrong password"}
        ).status_code
        == 401
    )

    response = login(client, "reader", "readonly-password")
    assert "HttpOnly" in response.headers["set-cookie"]
    assert "SameSite=strict" in response.headers["set-cookie"]
    assert client.get("/api/auth/session").json()["role"] == "viewer"
    assert client.get("/api/dags").status_code == 200
    assert client.get("/api/dags/demo/source").status_code == 403
    assert client.post("/api/dags/demo/trigger", json={}).status_code == 403
    assert client.post("/api/dags/demo/pause", json={"paused": True}).status_code == 403
    assert client.post("/api/auth/logout").status_code == 200
    assert client.get("/api/dags").status_code == 401

    login(client, "admin", "administrator-password")
    source = client.get("/api/dags/demo/source")
    assert source.status_code == 200 and "def example" in source.json()["content"]
    csrf = client.headers.pop("X-CSRF-Token")
    assert client.post("/api/dags/demo/pause", json={"paused": True}).status_code == 403
    client.headers["X-CSRF-Token"] = csrf
    assert (
        client.post(
            "/api/dags/demo/pause",
            json={"paused": True},
            headers={"Origin": "https://elsewhere.example"},
        ).status_code
        == 403
    )
    assert client.post("/api/dags/demo/pause", json={"paused": True}).status_code == 200

    token = client.cookies.get("easyflow_session")
    with sqlite3.connect(app.state.service.store.path) as connection:
        assert (
            connection.execute("SELECT token_hash FROM web_session").fetchone()[0]
            != token
        )
        assert (
            "administrator-password"
            not in connection.execute(
                "SELECT password_hash FROM web_user WHERE username = 'admin'"
            ).fetchone()[0]
        )
    app.state.auth.set_password("admin", "new-administrator-password")
    assert client.get("/api/dags").status_code == 401


def test_source_path_limit_and_expired_session(tmp_path, monkeypatch):
    app, client = client_for(tmp_path)
    login(client, "admin", "administrator-password")
    outside = tmp_path / "outside.py"
    outside.write_text("secret", encoding="utf-8")
    monkeypatch.setattr(
        app.state.service, "source", lambda _: SimpleNamespace(file_path=outside)
    )
    assert client.get("/api/dags/demo/source").status_code == 400
    with sqlite3.connect(app.state.service.store.path) as connection:
        connection.execute("UPDATE web_session SET expires_at = 0")
    assert client.get("/api/dags").status_code == 401


def test_account_cli_initialization_and_reset(tmp_path):
    runner = CliRunner()
    db = f"sqlite:///{tmp_path / 'state.db'}"
    result = runner.invoke(
        cli,
        ["users", "create", "alice", "--role", "admin", "--database-url", db],
        input="long-initial-password\nlong-initial-password\n",
    )
    assert result.exit_code == 0, result.output
    result = runner.invoke(cli, ["users", "list", "--database-url", db])
    assert result.exit_code == 0 and "alice\tadmin" in result.output
    result = runner.invoke(
        cli,
        ["users", "passwd", "alice", "--database-url", db],
        input="long-changed-password\nlong-changed-password\n",
    )
    assert result.exit_code == 0, result.output
    result = runner.invoke(
        cli, ["users", "remove", "alice", "--yes", "--database-url", db]
    )
    assert result.exit_code != 0 and "最后一个管理员" in result.output


def test_login_rate_limit_and_password_change_race(tmp_path):
    app, client = client_for(tmp_path)
    verified = app.state.auth.verify("reader", "readonly-password")
    app.state.auth.set_password("reader", "new-readonly-password")
    with pytest.raises(ValueError, match="重新登录"):
        app.state.auth.create_session(verified)
    for _ in range(6):
        assert (
            client.post(
                "/api/auth/login",
                json={"username": "reader", "password": "incorrect-password"},
            ).status_code
            == 401
        )
    assert (
        client.post(
            "/api/auth/login",
            json={"username": "reader", "password": "new-readonly-password"},
        ).status_code
        == 429
    )


def test_runtime_metadata_source_version_and_old_database(tmp_path):
    # 先建立不含账户表的旧状态库和运行，随后才初始化 Web 认证。
    from easyflow._internal.dag_parser import load_dag_snapshot
    from easyflow._internal.persistent_runner import PersistentDagRunner
    from easyflow._internal.state_store import SQLiteStateStore

    dags = tmp_path / "dags"
    dags.mkdir()
    path = dags / "demo.py"
    original = (
        "from easyflow.sdk import DAG, task\n"
        'with DAG("demo") as dag:\n'
        "    @task\n"
        "    def first():\n"
        "        return 1\n"
        "    @task\n"
        "    def second(value):\n"
        "        return value\n"
        "    second(first())\n"
    )
    path.write_text(original, encoding="utf-8")
    db = f"sqlite:///{tmp_path / 'state.db'}"
    store = SQLiteStateStore(db)
    store.init_db()
    dag = load_dag_snapshot(path, "demo")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(
        dag, file_path=path, logical_date=datetime.now(timezone.utc)
    )

    app = create_app(
        dags_folder=str(dags), database_url=db, log_root=str(tmp_path / "logs")
    )
    app.state.auth.create_user("admin", "administrator-password", "admin")
    client = TestClient(app, base_url="http://127.0.0.1:8790")
    login(client, "admin", "administrator-password")
    run = client.get(f"/api/dags/demo/runs/{run_id}").json()
    assert run["run_type"] == "manual" and run["queued_at"] and not run["start_date"]
    assert (
        run["logical_date"] and run["data_interval_start"] and run["data_interval_end"]
    )
    assert set(run["task_details"]) == {"first", "second"}
    assert run["definition_matches"] is True
    info = client.get("/api/dags/demo").json()
    first = next(task for task in info["tasks"] if task["task_id"] == "first")
    second = next(task for task in info["tasks"] if task["task_id"] == "second")
    assert first["downstream"] == ["second"] and second["upstream"] == ["first"]
    assert (
        client.get("/api/dags/demo/source", params={"run_id": run_id}).json()[
            "run_matches"
        ]
        is True
    )

    path.write_text(original + "\n# 文件已更新\n", encoding="utf-8")
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        response = client.get("/api/dags/demo/source", params={"run_id": run_id})
        if response.status_code == 200 and response.json()["run_matches"] is False:
            break
        time.sleep(0.05)
    assert response.status_code == 200 and response.json()["run_matches"] is False
    assert store.get_dag_run("demo", run_id) is not None
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE task_instance SET state = 'running', last_heartbeat_at = ?, "
            "lease_expires_at = ? WHERE dag_id = 'demo' AND run_id = ? AND task_id = 'first'",
            ("2026-11-08T08:00:00+00:00", "2026-11-08T08:00:30+00:00", run_id),
        )
        connection.execute(
            "UPDATE dag_run SET state = 'running' WHERE dag_id = 'demo' AND run_id = ?",
            (run_id,),
        )
        connection.execute(
            "INSERT INTO dag_run (dag_id, run_id, run_type, logical_date, "
            "data_interval_start, data_interval_end, state, queued_at) "
            "SELECT dag_id, 'newer', run_type, logical_date, data_interval_start, "
            "data_interval_end, 'success', ? FROM dag_run WHERE dag_id = 'demo' AND run_id = ?",
            ((datetime.now(timezone.utc) + timedelta(seconds=5)).isoformat(), run_id),
        )
    overview = client.get("/api/dags").json()["dags"][0]
    assert overview["latest_run"]["state"] == "success"
    assert overview["running_count"] == 1
    task = client.get(f"/api/dags/demo/runs/{run_id}").json()["task_details"]["first"]
    assert task["state"] == "running"
    assert task["last_heartbeat_at"] == "2026-11-08T08:00:00+00:00"
    assert task["lease_expires_at"] == "2026-11-08T08:00:30+00:00"


def test_next_schedule_respects_pause_start_end_and_existing_history(tmp_path):
    app, _ = client_for(tmp_path)
    service = app.state.service
    now = datetime.now(timezone.utc)
    dag = SimpleNamespace(
        dag_id="absent", schedule=None, start_date=None, end_date=None, catchup=False
    )
    assert service.next_schedule(dag, False)["status"] == "manual"
    dag.schedule = "@daily"
    assert service.next_schedule(dag, True)["status"] == "paused"
    dag.start_date = (now + timedelta(days=2)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    candidate = service.next_schedule(dag, False)
    assert candidate["status"] == "planned"
    assert datetime.fromisoformat(candidate["at"]) == dag.start_date
    dag.end_date = dag.start_date - timedelta(seconds=1)
    assert service.next_schedule(dag, False)["status"] == "completed"
    dag.start_date = (now - timedelta(days=20)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    dag.end_date = now - timedelta(days=10)
    assert service.next_schedule(dag, False)["status"] == "completed"
    dag.end_date = None
    dag.start_date = (now - timedelta(days=2)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    assert service.next_schedule(dag, False)["status"] == "due"
    from easyflow._internal.schedules import scheduler_intervals

    dag.catchup = True
    all_due = scheduler_intervals(dag, now)
    first_due = scheduler_intervals(dag, now, max_intervals=1)
    assert len(all_due) > 1 and first_due == all_due[:1]
