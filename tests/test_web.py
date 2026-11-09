from __future__ import annotations

from fastapi.testclient import TestClient

from easyflow._internal.dag_parser import load_dag_snapshot
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow.web.app import create_app


def test_web_dag_run_logs_pause_and_clear(tmp_path):
    dags = tmp_path / "dags"
    dags.mkdir()
    path = dags / "demo.py"
    path.write_text(
        """from easyflow.sdk import DAG, task
with DAG("demo") as dag:
    @task
    def hello():
        print("hello from task")
    hello()
""",
        encoding="utf-8",
    )
    app = create_app(
        dags_folder=str(dags),
        database_url=f"sqlite:///{tmp_path / 'db.sqlite'}",
        log_root=str(tmp_path / "logs"),
    )
    client = TestClient(app, base_url="http://127.0.0.1:8790")
    app.state.auth.create_user("admin", "a sufficiently long password", "admin")
    login = client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "a sufficiently long password"},
    )
    assert login.status_code == 200
    client.headers["X-CSRF-Token"] = login.json()["csrf_token"]
    overview = client.get("/api/dags").json()
    assert [dag["dag_id"] for dag in overview["dags"]] == ["demo"]
    assert not overview["scheduler_running"]
    assert client.get("/").status_code == 200

    assert client.post("/api/dags/demo/pause", json={"paused": True}).json()["paused"]
    triggered = client.post("/api/dags/demo/trigger", json={"conf": {"name": "test"}})
    assert triggered.status_code == 200 and triggered.json()["paused"]
    run_id = triggered.json()["run_id"]
    base = f"/api/dags/demo/runs/{run_id}"
    assert client.get(base).json()["state"] == "queued"
    assert (
        client.post("/api/dags/demo/pause", json={"paused": False}).status_code == 200
    )

    dag = load_dag_snapshot(path, "demo")
    runner = PersistentDagRunner(app.state.service.store, log_root=tmp_path / "logs")
    assert (
        runner.run_existing(dag, run_id=run_id, file_path=path).state.value == "success"
    )
    assert "hello from task" in client.get(f"{base}/tasks/hello/logs").json()["content"]
    assert client.post(f"{base}/clear-preview", json={"task_ids": []}).json() == {
        "to_clear": [],
        "blocked": [],
    }
    assert client.post(f"{base}/clear", json={"task_ids": []}).json() == {
        "to_clear": []
    }
    assert client.get(base).json()["state"] == "success"
    assert not any(
        action == "tasks_clear"
        for action, _, _ in app.state.service.store.list_audit_actions()
    )
    preview = client.post(f"{base}/clear-preview", json={"task_ids": ["hello"]}).json()
    assert preview == {"to_clear": ["hello"], "blocked": []}
    assert client.post(f"{base}/clear", json={"task_ids": ["hello"]}).json()[
        "to_clear"
    ] == ["hello"]
    assert client.get(base).json()["state"] == "queued"

    response = client.post(
        "/api/dags/demo/pause",
        json={"paused": True},
        headers={"Origin": "https://example.com"},
    )
    assert response.status_code == 403
    assert client.get("/api/dags/missing").status_code == 404


def test_web_backfill_preview_rejects_oversized_range(tmp_path):
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "minute.py").write_text(
        'from easyflow.sdk import DAG\ndag = DAG("minute", schedule="* * * * *")\n',
        encoding="utf-8",
    )
    app = create_app(
        dags_folder=str(dags),
        database_url=f"sqlite:///{tmp_path / 'db.sqlite'}",
        log_root=str(tmp_path / "logs"),
    )
    client = TestClient(app, base_url="http://127.0.0.1:8790")
    app.state.auth.create_user("admin", "a sufficiently long password", "admin")
    login = client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "a sufficiently long password"},
    )
    client.headers["X-CSRF-Token"] = login.json()["csrf_token"]
    assert [dag["dag_id"] for dag in client.get("/api/dags").json()["dags"]] == [
        "minute"
    ]
    response = client.post(
        "/api/dags/minute/backfill-preview",
        json={"start_date": "2026-07-01", "end_date": "2026-07-01"},
    )
    assert response.status_code == 400
    assert "最多支持 1000 个区间" in response.json()["detail"]
    response = client.post(
        "/api/dags/minute/backfill-preview",
        json={
            "start_date": "2026-07-01T00:00:00+08:00",
            "end_date": "2026-07-01T00:09:00+08:00",
        },
    )
    assert response.status_code == 200
    assert len(response.json()) == 10
    assert response.json()[0]["logical_date"] == "2026-06-30T16:00:00+00:00"
    response = client.post(
        "/api/dags/minute/backfill-preview",
        json={"start_date": "2026-07-01T00:00:00", "end_date": "2026-07-01"},
    )
    assert response.status_code == 422
