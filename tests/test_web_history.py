from __future__ import annotations

import base64
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from easyflow.web.app import create_app


def history_client(tmp_path: Path):
    dags = tmp_path / "dags"
    dags.mkdir()
    app = create_app(
        dags_folder=str(dags),
        database_url=f"sqlite:///{tmp_path / 'state.db'}",
        log_root=str(tmp_path / "logs"),
    )
    app.state.auth.create_user("reader", "readonly-password", "viewer")
    client = TestClient(app, base_url="http://127.0.0.1:8790")
    return app, client


def add_run(
    connection, dag_id, run_id, state="success", queued_at="2026-01-01T00:00:00+00:00"
):
    connection.execute(
        "INSERT INTO dag_run (dag_id, run_id, run_type, logical_date, "
        "data_interval_start, data_interval_end, state, queued_at) "
        "VALUES (?, ?, 'manual', ?, ?, ?, ?, ?)",
        (dag_id, run_id, queued_at, queued_at, queued_at, state, queued_at),
    )


def test_history_pagination_auth_filter_and_new_runs(tmp_path):
    app, client = history_client(tmp_path)
    assert client.get("/api/history/runs").status_code == 401
    assert (
        client.get(
            "/api/history/run", params={"dag_id": "x", "run_id": "y"}
        ).status_code
        == 401
    )
    expected = set()
    with sqlite3.connect(app.state.service.store.path) as connection:
        for i in range(153):
            dag_id = "old_workflow" if i % 2 else "other"
            run_id = f"run-{i:03d}"
            add_run(connection, dag_id, run_id, "failed" if i % 3 == 0 else "success")
            expected.add((dag_id, run_id))
        # 旧状态库原先没有历史索引；重新初始化后不应改变其运行记录。
        connection.execute("DROP INDEX ix_dag_run_history")
    app.state.service.store.init_db()
    with sqlite3.connect(app.state.service.store.path) as connection:
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'ix_dag_run_history'"
        ).fetchone()
        assert connection.execute("SELECT COUNT(*) FROM dag_run").fetchone()[0] == 153

    assert (
        client.post(
            "/api/auth/login",
            json={"username": "reader", "password": "readonly-password"},
        ).status_code
        == 200
    )
    first = client.get("/api/history/runs", params={"limit": 25}).json()
    seen = [(row["dag_id"], row["run_id"]) for row in first["runs"]]
    assert len(seen) == 25 and first["next_cursor"]
    with sqlite3.connect(app.state.service.store.path) as connection:
        add_run(connection, "latest", "inserted", queued_at="2026-01-02T00:00:00+00:00")
    cursor = first["next_cursor"]
    while cursor:
        page = client.get("/api/history/runs", params={"limit": 25, "cursor": cursor})
        assert page.status_code == 200
        data = page.json()
        seen.extend((row["dag_id"], row["run_id"]) for row in data["runs"])
        cursor = data["next_cursor"]
    assert len(seen) == len(expected) == len(set(seen))
    assert set(seen) == expected
    assert seen == sorted(seen, reverse=True)
    assert client.get("/api/history/runs").json()["runs"][0]["run_id"] == "inserted"

    filtered = client.get(
        "/api/history/runs", params={"dag": "OLD_WORK", "state": "failed", "limit": 100}
    ).json()["runs"]
    assert filtered and all(
        row["dag_id"] == "old_workflow" and row["state"] == "failed" for row in filtered
    )
    assert client.get("/api/history/runs", params={"dag": "%"}).json()["runs"] == []
    assert (
        client.get("/api/history/runs", params={"cursor": "not-valid!"}).status_code
        == 400
    )
    nested = base64.urlsafe_b64encode(("[" * 1100 + "0" + "]" * 1100).encode()).decode()
    assert client.get("/api/history/runs", params={"cursor": nested}).status_code == 400
    assert client.get("/api/history/runs", params={"limit": 101}).status_code == 422
    assert client.get("/api/history/runs", params={"state": "bogus"}).status_code == 422


def test_historical_task_and_logs_without_current_dag(tmp_path):
    app, client = history_client(tmp_path)
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / "try.log"
    log_file.write_text("历史任务日志", encoding="utf-8")
    dag_id, run_id, task_id = "removed_dag", "custom/run", "task/one"
    with sqlite3.connect(app.state.service.store.path) as connection:
        add_run(connection, dag_id, run_id, "failed")
        connection.execute(
            "INSERT INTO task_instance (dag_id, run_id, task_id, state, try_number) VALUES (?, ?, ?, 'failed', 1)",
            (dag_id, run_id, task_id),
        )
        connection.execute(
            "INSERT INTO task_try (dag_id, run_id, task_id, try_number, state, log_path) VALUES (?, ?, ?, 1, 'failed', ?)",
            (dag_id, run_id, task_id, str(log_file)),
        )
    client.post(
        "/api/auth/login", json={"username": "reader", "password": "readonly-password"}
    )
    ids = {"dag_id": dag_id, "run_id": run_id}
    assert client.get(f"/api/dags/{dag_id}").status_code == 404
    detail = client.get("/api/history/run", params=ids)
    assert detail.status_code == 200
    assert detail.json()["tasks"] == {task_id: "failed"}
    assert detail.json()["definition_matches"] is None
    logs = client.get("/api/history/task-log", params={**ids, "task_id": task_id})
    assert logs.status_code == 200 and logs.json()["content"] == "历史任务日志"
    assert (
        client.get(
            "/api/history/task-log", params={**ids, "task_id": "missing"}
        ).status_code
        == 404
    )
    assert (
        client.get(
            "/api/history/run", params={"dag_id": dag_id, "run_id": "missing"}
        ).status_code
        == 404
    )
    assert (
        client.get(
            "/api/history/task-log",
            params={"dag_id": dag_id, "run_id": "missing", "task_id": task_id},
        ).status_code
        == 404
    )
