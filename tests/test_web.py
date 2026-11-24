from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from easyflow._internal.dag_parser import load_dag_snapshot
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow.sdk.states import DagRunState
from easyflow.web.app import create_app


@pytest.fixture
def review_workflow(tmp_path):
    dags = tmp_path / "dags"
    dags.mkdir()
    helper = dags / "review_settings.py"
    helper.write_text("VALUE = 'v1'\n", encoding="utf-8")
    path = dags / "demo.py"
    path.write_text(
        """from review_settings import VALUE
from easyflow.sdk import DAG, task
with DAG('demo', max_active_runs=2) as dag:
    @task
    def first():
        return VALUE
    @task
    def second(value):
        return value
    second(first())
""",
        encoding="utf-8",
    )
    app = create_app(
        dags_folder=str(dags),
        database_url=f"sqlite:///{tmp_path / 'state.db'}",
        log_root=str(tmp_path / "logs"),
    )
    app.state.auth.create_user("admin", "a sufficiently long password", "admin")
    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        login = client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "a sufficiently long password"},
        )
        client.headers["X-CSRF-Token"] = login.json()["csrf_token"]
        service = app.state.service
        runner = PersistentDagRunner(service.store, log_root=tmp_path / "logs")
        yield service, client, runner, path, helper


@pytest.mark.parametrize("change", ["main", "dependency", "after_preview"])
def test_web_clear_requires_current_definition_and_preserves_xcom(
    review_workflow, monkeypatch, change
):
    service, client, runner, path, helper = review_workflow
    source = service.source("demo")
    run_id = runner.create_manual_run(source.dag, file_path=path, logical_date=datetime.now(UTC))
    assert runner.run_existing(source.dag, run_id=run_id, file_path=path).state.value == "success"
    params = {"dag_id": "demo", "run_id": run_id}
    body = {"task_ids": ["first"]}
    def issue(endpoint):
        return client.post(f"/api/runs/{endpoint}", params=params, json=body)

    old = client.get("/api/history/run", params=params).json()
    assert old["definition_matches"] is True
    target = path if change == "main" else helper
    original = target.read_text(encoding="utf-8")
    modified = original + "\n# 变更定义\n" if change == "main" else "VALUE = 'v2'\n"
    if change == "after_preview":
        assert issue("clear-preview").status_code == 200
        original_clear = service.store.clear_task_instances
        def edit_before_mutation(**kwargs):
            target.write_text(modified, encoding="utf-8")
            return original_clear(**kwargs)
        with monkeypatch.context() as context:
            context.setattr(service.store, "clear_task_instances", edit_before_mutation)
            assert issue("clear").status_code == 409
    else:
        target.write_text(modified, encoding="utf-8")
        assert issue("clear-preview").status_code == 409
        assert issue("clear").status_code == 409

    history = client.get("/api/history/run", params=params).json()
    definition = client.get("/api/workflows/detail", params={"dag_id": "demo"}).json()
    assert history["definition_matches"] is False
    assert history["definition_hash"] == old["definition_hash"] != definition["definition_hash"]
    assert history["state"] == history["tasks"]["first"] == "success"
    assert service.store.pull_xcom("demo", run_id, "first", "return_value") == "v1"
    assert not any(action == "tasks_clear" for action, _, _ in service.store.list_audit_actions())

    target.write_text(original, encoding="utf-8")
    assert issue("clear-preview").json() == {"to_clear": ["first"], "blocked": []}
    assert issue("clear").json() == {"to_clear": ["first"]}
    assert runner.run_existing(source.dag, run_id=run_id, file_path=path).state.value == "success"
    assert service.store.pull_xcom("demo", run_id, "first", "return_value") == "v1"


def test_web_trigger_uses_refreshed_local_dependency(review_workflow):
    service, client, runner, path, helper = review_workflow
    source = service.source("demo")
    helper.write_text("VALUE = 'v2'\n", encoding="utf-8")
    response = client.post("/api/workflows/trigger", params={"dag_id": "demo"}, json={})
    assert response.status_code == 200, response.text
    current = service.source("demo")
    assert current.dag._easyflow_dependency_hashes != source.dag._easyflow_dependency_hashes
    run_id = response.json()["run_id"]
    assert runner.run_existing(current.dag, run_id=run_id, file_path=path).state.value == "success"
    assert service.store.pull_xcom("demo", run_id, "second", "return_value") == "v2"


def test_definition_failed_run_can_clear_a_successful_task_with_stale_queue(review_workflow):
    service, client, runner, path, helper = review_workflow
    store = service.store
    dag = service.source("demo").dag
    run_id = runner.create_manual_run(dag, file_path=path, logical_date=datetime.now(UTC))
    store.set_dag_run_state("demo", run_id, DagRunState.RUNNING)
    attempt = store.start_task_try("demo", run_id, "first")
    store.commit_task_terminal(
        dag_id="demo", run_id=run_id, task_id="first", try_number=attempt,
        xcom_values={"return_value": "之前的值"},
    )
    assert store.queue_task("demo", run_id, "second")
    params = {"dag_id": "demo", "run_id": run_id}
    body = {"task_ids": ["first"]}
    preview = client.post("/api/runs/clear-preview", params=params, json=body)
    assert preview.json()["blocked"]
    helper.write_text("VALUE = 'v2'\n", encoding="utf-8")
    changed = service.source("demo").dag
    assert runner.run_existing(changed, run_id=run_id, file_path=path).state.value == "failed"
    assert store.get_task_state("demo", run_id, "second").value == "queued"
    helper.write_text("VALUE = 'v1'\n", encoding="utf-8")
    assert client.post("/api/runs/clear-preview", params=params, json=body).json() == {
        "to_clear": ["first"], "blocked": [],
    }
    assert client.post("/api/runs/clear", params=params, json=body).status_code == 200
    assert runner.run_existing(dag, run_id=run_id, file_path=path).state.value == "success"
    assert store.pull_xcom("demo", run_id, "second", "return_value") == "v1"


def loaded_dags(client):
    deadline = time.monotonic() + 5
    while True:
        response = client.get("/api/dags")
        assert response.status_code == 200
        overview = response.json()
        if not overview.get("loading", False):
            return overview
        assert time.monotonic() < deadline, overview
        time.sleep(0.05)


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
    overview = loaded_dags(client)
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
    task_detail = client.get(base).json()["task_details"]["hello"]
    assert task_detail["queued_at"]
    assert task_detail["queued_at"] <= task_detail["started_at"] <= task_detail["ended_at"]
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
    assert client.get(base).json()["task_details"]["hello"]["queued_at"] is None

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
    assert [dag["dag_id"] for dag in loaded_dags(client)["dags"]] == ["minute"]
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
