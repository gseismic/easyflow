from __future__ import annotations

import json
import time
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow.web.app import create_app


@pytest.fixture
def workflow_client(tmp_path, request):
    dag_id = getattr(request, "param", "demo")
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "workflow.py").write_text(
        "from easyflow.sdk import DAG\n"
        "from easyflow.providers.standard.operators.empty import EmptyOperator\n"
        f"with DAG({dag_id!r}, schedule='@daily', max_active_runs=2) as dag:\n"
        "    EmptyOperator(task_id='example')\n",
        encoding="utf-8",
    )
    application = create_app(
        dags_folder=str(dags),
        database_url=f"sqlite:///{tmp_path / 'state.db'}",
        log_root=str(tmp_path / "logs"),
    )
    application.state.auth.create_user("admin", "administrator-password", "admin")
    application.state.auth.create_user("reader", "readonly-password", "viewer")
    with TestClient(application, base_url="http://127.0.0.1:8790") as client:
        yield application, client, dag_id


def login(client, username="admin", password="administrator-password"):
    response = client.post(
        "/api/auth/login", json={"username": username, "password": password}
    )
    assert response.status_code == 200
    client.headers["X-CSRF-Token"] = response.json()["csrf_token"]


def wait_for_discovery(client):
    deadline = time.monotonic() + 5
    while True:
        response = client.get("/api/dags")
        assert response.status_code == 200
        if not response.json().get("loading"):
            assert response.json()["issues"] == []
            return response.json()
        assert time.monotonic() < deadline
        time.sleep(0.05)


@pytest.mark.parametrize("workflow_client", [".", "..", "detail", "demo"], indirect=True)
def test_workflow_query_routes_preserve_ids_and_legacy_routes(workflow_client):
    application, client, dag_id = workflow_client
    login(client)
    assert [dag["dag_id"] for dag in wait_for_discovery(client)["dags"]] == [dag_id]
    params = {"dag_id": dag_id}
    detail = client.get("/api/workflows/detail", params=params)
    assert detail.status_code == 200
    assert detail.json()["dag_id"] == dag_id
    assert detail.json()["tasks"][0]["task_id"] == "example"
    source = client.get("/api/workflows/source", params=params)
    assert source.status_code == 200
    assert f"DAG({dag_id!r}" in source.json()["content"]

    pause = client.post("/api/workflows/pause", params=params, json={"paused": True})
    assert pause.status_code == 200 and pause.json()["paused"]
    triggered = client.post(
        "/api/workflows/trigger", params=params, json={"conf": {"test": dag_id}}
    )
    assert triggered.status_code == 200 and triggered.json()["paused"]
    run_id = triggered.json()["run_id"]
    service = application.state.service
    assert json.loads(service.store.get_dag_run(dag_id, run_id).conf_json) == {"test": dag_id}
    assert service.store.list_task_states(dag_id, run_id) == {"example": None}
    assert (
        client.get("/api/workflows/source", params={**params, "run_id": run_id})
        .json()["run_matches"]
        is True
    )
    special_run = "custom/run +%?#中文"
    dag_source = service.source(dag_id)
    PersistentDagRunner(service.store, log_root=service.log_root).create_manual_run(
        dag_source.dag,
        file_path=dag_source.file_path,
        logical_date=datetime(2026, 11, 1, tzinfo=UTC),
        run_id=special_run,
    )
    assert (
        client.get("/api/workflows/source", params={**params, "run_id": special_run})
        .json()["run_matches"]
        is True
    )
    assert client.post("/api/workflows/trigger", params=params, json={}).status_code == 409
    preview = client.post(
        "/api/workflows/backfill-preview",
        params=params,
        json={"start_date": "2026-07-01", "end_date": "2026-07-02"},
    )
    assert preview.status_code == 200
    assert len(preview.json()) == 2 and all(item["eligible"] for item in preview.json())
    assert service.store.count_dag_runs(dag_id=dag_id, run_type="backfill") == 0
    clear_params = {**params, "run_id": run_id}
    clear_body = {"task_ids": ["example", "missing/task"]}
    clear_preview = client.post(
        "/api/runs/clear-preview", params=clear_params, json=clear_body
    )
    assert clear_preview.status_code == 200
    assert clear_preview.json()["blocked"][0]["task_id"] == "missing/task"
    clear = client.post("/api/runs/clear", params=clear_params, json=clear_body)
    assert clear.status_code == 409 and "missing/task" in clear.json()["detail"]
    assert client.get("/api/workflows/detail", params=params).json()["paused"]
    assert client.get("/api/workflows/detail").status_code == 422
    assert client.get("/api/workflows/detail", params={"dag_id": "missing"}).status_code == 404

    if dag_id not in {".", ".."}:
        assert client.get(f"/api/dags/{dag_id}").json() == client.get(
            "/api/workflows/detail", params=params
        ).json()
        assert client.get(f"/api/dags/{dag_id}/source").json() == source.json()
        assert client.post(f"/api/dags/{dag_id}/pause", json={"paused": False}).status_code == 200


def test_workflow_query_routes_keep_auth_role_and_csrf_guards(workflow_client):
    application, client, dag_id = workflow_client
    params = {"dag_id": dag_id}
    reads = ["detail", "source"]
    writes = [("pause", {"paused": True}), ("trigger", {}), ("backfill-preview", {
        "start_date": "2026-07-01", "end_date": "2026-07-02",
    })]
    for action in reads:
        assert client.get(f"/api/workflows/{action}", params=params).status_code == 401
    for action, body in writes:
        assert client.post(f"/api/workflows/{action}", params=params, json=body).status_code == 401

    login(client, "reader", "readonly-password")
    wait_for_discovery(client)
    assert client.get("/api/workflows/detail", params=params).status_code == 200
    assert client.get("/api/workflows/source", params=params).status_code == 403
    for action, body in writes:
        assert client.post(f"/api/workflows/{action}", params=params, json=body).status_code == 403

    login(client)
    csrf = client.headers.pop("X-CSRF-Token")
    for action, body in writes:
        assert client.post(f"/api/workflows/{action}", params=params, json=body).status_code == 403
    client.headers["X-CSRF-Token"] = csrf
    for action, body in writes:
        assert client.post(
            f"/api/workflows/{action}", params=params, json=body,
            headers={"Origin": "https://elsewhere.example"},
        ).status_code == 403
    assert application.state.service.store.count_dag_runs(dag_id=dag_id) == 0
    assert not application.state.service.store.is_dag_paused(dag_id)
