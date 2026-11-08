from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from easyflow._internal.dag_parser import DagDiscoveryResult, DagSource
from easyflow.providers.standard.operators.empty import EmptyOperator
from easyflow.sdk import DAG
from easyflow.web.app import create_app


def setup_catalog(tmp_path: Path, monkeypatch):
    folder = tmp_path / "dags"
    folder.mkdir()
    app = create_app(
        dags_folder=str(folder),
        database_url=f"sqlite:///{tmp_path / 'state.db'}",
        log_root=str(tmp_path / "logs"),
    )
    sources = []

    def add(dag_id, tags, description=""):
        dag = DAG(dag_id, tags=tags, description=description, auto_register=False)
        sources.append(DagSource(dag_id, folder / f"{dag_id}.py", dag))

    add("quant_prices", ["project:quant", "daily", "prices"], "行情采集")
    add("quant_report", ["project:quant", "daily", "report"], "日报汇总")
    add("analytics_report", ["project:analytics", "daily", "report"], "数据报告")
    add("legacy", ["legacy"], "历史数据")
    add("bad_project", ["project:one", "project:two", "daily"])
    add("bad_empty", ["project: ", "daily"])
    add("unicode", ["project:Straße", "日报"], "中文描述")
    monkeypatch.setattr(
        app.state.service,
        "discover",
        lambda: DagDiscoveryResult(list(sources), [], False),
    )
    app.state.auth.create_user("admin", "administrator-password", "admin")
    app.state.auth.create_user("reader", "readonly-password", "viewer")
    return app, TestClient(app, base_url="http://127.0.0.1:8790"), sources, add


def login(client, username="reader", password="readonly-password"):
    response = client.post(
        "/api/auth/login", json={"username": username, "password": password}
    )
    assert response.status_code == 200
    client.headers["X-CSRF-Token"] = response.json()["csrf_token"]


def insert_run(
    connection, dag_id, run_id, state="success", queued="2026-01-01T00:00:00+00:00"
):
    connection.execute(
        "INSERT INTO dag_run (dag_id, run_id, run_type, logical_date, "
        "data_interval_start, data_interval_end, state, queued_at) "
        "VALUES (?, ?, 'manual', ?, ?, ?, ?, ?)",
        (dag_id, run_id, queued, queued, queued, state, queued),
    )


def test_catalog_filters_status_and_permissions(tmp_path, monkeypatch):
    app, client, _, _ = setup_catalog(tmp_path, monkeypatch)
    assert client.get("/api/catalog").status_code == 401
    assert client.get("/api/workflows").status_code == 401
    assert client.get("/api/workflows/facets").status_code == 401
    assert client.get("/api/history/runs/facets").status_code == 401
    with sqlite3.connect(app.state.service.store.path) as connection:
        insert_run(connection, "quant_prices", "success", "success")
        insert_run(
            connection,
            "quant_prices",
            "new-failure",
            "failed",
            "2026-01-02T00:00:00+00:00",
        )
        insert_run(connection, "quant_report", "run", "running")
        connection.execute("INSERT INTO dag_control VALUES ('analytics_report', 1)")
    login(client)
    catalog = client.get("/api/catalog").json()
    assert catalog["projects"] == ["analytics", "quant", "Straße"]
    assert {"daily", "prices", "report", "日报"}.issubset(catalog["tags"])
    assert not any(tag.startswith("project:") for tag in catalog["tags"])

    response = client.get(
        "/api/workflows",
        params=[
            ("project_kind", "project"),
            ("project", "quant"),
            ("tag", "daily"),
            ("tag", "prices"),
            ("q", "QUANT 行情"),
        ],
    )
    assert response.status_code == 200
    body = response.json()
    assert [row["dag_id"] for row in body["dags"]] == ["quant_prices"]
    assert body["dags"][0]["project"] == "quant"
    assert body["dags"][0]["tags"] == ["daily", "prices"]
    assert body["summary"] == {"total": 1, "running": 0, "failed": 1}

    running = client.get("/api/workflows", params={"state": "running"}).json()["dags"]
    assert [row["dag_id"] for row in running] == ["quant_report"]
    paused = client.get("/api/workflows", params={"state": "paused"}).json()["dags"]
    assert [row["dag_id"] for row in paused] == ["analytics_report"]
    unassigned = client.get(
        "/api/workflows", params={"project_kind": "unassigned"}
    ).json()["dags"]
    assert [row["dag_id"] for row in unassigned] == ["legacy"]
    conflict = client.get("/api/workflows", params={"project_kind": "conflict"}).json()[
        "dags"
    ]
    assert {row["dag_id"] for row in conflict} == {"bad_project", "bad_empty"}
    assert all(row["metadata_issue"] for row in conflict)
    assert [
        row["dag_id"]
        for row in client.get("/api/workflows", params={"q": "STRASSE 中文"}).json()[
            "dags"
        ]
    ] == ["unicode"]
    assert (
        client.get("/api/workflows", params={"project_kind": "project"}).status_code
        == 400
    )
    assert client.get("/api/workflows", params={"state": "invalid"}).status_code == 422
    assert (
        client.post("/api/dags/legacy/pause", json={"paused": True}).status_code == 403
    )

    login(client, "admin", "administrator-password")
    assert client.get("/api/catalog").status_code == 200
    assert client.get("/api/workflows").status_code == 200


def test_workflow_search_and_pagination_cover_all_candidates(tmp_path, monkeypatch):
    app, client, sources, add = setup_catalog(tmp_path, monkeypatch)
    for index in range(180):
        add(f"z_bulk_{index:03d}", ["project:bulk", "daily", "archive"])
    login(client)
    params = [
        ("project_kind", "project"),
        ("project", "bulk"),
        ("tag", "daily"),
        ("q", "bulk archive"),
        ("limit", "25"),
    ]
    seen = []
    cursor = None
    while True:
        response = client.get(
            "/api/workflows", params=params + ([("cursor", cursor)] if cursor else [])
        )
        assert response.status_code == 200
        body = response.json()
        assert body["summary"]["total"] == 180
        seen.extend(row["dag_id"] for row in body["dags"])
        cursor = body["next_cursor"]
        if not cursor:
            break
    assert seen == [f"z_bulk_{index:03d}" for index in range(180)]
    # 新增一个排序靠前的 DAG 后，继续按游标访问不会漏掉原有后续记录。
    first = client.get("/api/workflows", params=params).json()
    add("a_new", ["project:bulk", "daily", "archive"])
    second = client.get(
        "/api/workflows", params=params + [("cursor", first["next_cursor"])]
    ).json()
    assert second["dags"][0]["dag_id"] == "z_bulk_025"
    assert len(sources) == 188


def test_task_search_uses_current_workflows_and_saved_run_tasks(tmp_path, monkeypatch):
    app, client, sources, _ = setup_catalog(tmp_path, monkeypatch)
    quant = next(source.dag for source in sources if source.dag_id == "quant_prices")
    EmptyOperator(task_id="preview_prices", dag=quant)
    with sqlite3.connect(app.state.service.store.path) as connection:
        insert_run(connection, "quant_prices", "older", "failed")
        insert_run(connection, "quant_prices", "newer", "success")
        insert_run(connection, "removed_workflow", "removed", "failed")
        connection.executemany(
            "INSERT INTO task_instance (dag_id, run_id, task_id, state) VALUES (?, ?, ?, ?)",
            [
                ("quant_prices", "older", "historic_prices", "failed"),
                ("quant_prices", "newer", "unrelated", "success"),
                ("removed_workflow", "removed", "historic_prices", "failed"),
            ],
        )
    login(client)

    workflow = client.get("/api/workflows", params={"q": "QUANT preview"}).json()
    assert [row["dag_id"] for row in workflow["dags"]] == ["quant_prices"]
    assert workflow["dags"][0]["matched_task_ids"] == ["preview_prices"]
    facets = client.get("/api/workflows/facets", params={"q": "preview"}).json()
    assert facet_group(facets, "project", "quant")["matched"] == 1

    # 历史只匹配对应运行中已保存的任务，不能借用工作流当前定义。
    assert (
        client.get("/api/history/runs", params={"q": "preview_prices"}).json()["runs"]
        == []
    )
    history = client.get("/api/history/runs", params={"q": "historic_prices"}).json()
    assert {(row["dag_id"], row["run_id"]) for row in history["runs"]} == {
        ("quant_prices", "older"),
        ("removed_workflow", "removed"),
    }
    assert all(row["matched_task_ids"] == ["historic_prices"] for row in history["runs"])
    quant_history = next(row for row in history["runs"] if row["dag_id"] == "quant_prices")
    assert quant_history["description"] == "行情采集"
    first_page = client.get(
        "/api/history/runs", params={"q": "historic_prices", "limit": 1}
    ).json()
    second_page = client.get(
        "/api/history/runs",
        params={"q": "historic_prices", "limit": 1, "cursor": first_page["next_cursor"]},
    ).json()
    assert {first_page["runs"][0]["run_id"], second_page["runs"][0]["run_id"]} == {
        "older", "removed"
    }
    filtered = client.get(
        "/api/history/runs",
        params={
            "q": "quant historic",
            "state": "failed",
            "project_kind": "project",
            "project": "quant",
            "tag": "daily",
        },
    ).json()
    assert [(row["dag_id"], row["run_id"]) for row in filtered["runs"]] == [
        ("quant_prices", "older")
    ]
    facets = client.get("/api/history/runs/facets", params={"q": "historic_prices"}).json()
    assert facet_group(facets, "project", "quant")["matched"] == 1
    assert facet_group(facets, "unknown")["matched"] == 1


def test_workflow_task_changes_invalidate_facets_and_many_search_terms(tmp_path, monkeypatch):
    _, client, sources, _ = setup_catalog(tmp_path, monkeypatch)
    login(client)
    first = client.get("/api/workflows/facets", params={"q": "new_task"}).json()
    assert facet_group(first, "project", "quant")["matched"] == 0
    quant = next(source.dag for source in sources if source.dag_id == "quant_prices")
    EmptyOperator(task_id="new_task", dag=quant)
    second = client.get("/api/workflows/facets", params={"q": "new_task"}).json()
    assert facet_group(second, "project", "quant")["matched"] == 1

    # 长查询不会因重复词而突破 SQLite 的绑定参数上限。
    long_query = "a " * 499
    assert client.get("/api/history/runs", params={"q": long_query}).status_code == 200
    assert (
        client.get("/api/history/runs/facets", params={"q": long_query}).status_code == 200
    )


def test_history_filters_before_pagination_and_keeps_unknown(tmp_path, monkeypatch):
    app, client, sources, _ = setup_catalog(tmp_path, monkeypatch)
    with sqlite3.connect(app.state.service.store.path) as connection:
        for index in range(140):
            insert_run(
                connection,
                "quant_prices",
                f"history-{index:03d}",
                "failed" if index % 2 else "success",
            )
        insert_run(connection, "analytics_report", "special/report", "failed")
        insert_run(connection, "removed_workflow", "old-run", "failed")
        insert_run(connection, "bad_project", "bad-run", "success")
    login(client)
    params = [
        ("project_kind", "project"),
        ("project", "quant"),
        ("tag", "daily"),
        ("tag", "prices"),
        ("q", "QUANT 行情"),
        ("state", "failed"),
        ("limit", "17"),
    ]
    seen = []
    cursor = None
    while True:
        response = client.get(
            "/api/history/runs",
            params=params + ([("cursor", cursor)] if cursor else []),
        )
        assert response.status_code == 200
        body = response.json()
        seen.extend(row["run_id"] for row in body["runs"])
        assert all(
            row["project"] == "quant" and row["project_kind"] == "project"
            for row in body["runs"]
        )
        cursor = body["next_cursor"]
        if not cursor:
            break
    assert len(seen) == 70 and len(set(seen)) == 70
    assert (
        client.get("/api/history/runs", params={"q": "special/report 数据"}).json()[
            "runs"
        ][0]["dag_id"]
        == "analytics_report"
    )
    unknown = client.get(
        "/api/history/runs", params={"project_kind": "unknown"}
    ).json()["runs"]
    assert [(row["dag_id"], row["project_kind"]) for row in unknown] == [
        ("removed_workflow", "unknown")
    ]
    assert (
        client.get("/api/history/runs", params={"project_kind": "project"}).status_code
        == 400
    )
    assert (
        client.get("/api/history/runs", params={"project_kind": "conflict"}).json()[
            "runs"
        ][0]["dag_id"]
        == "bad_project"
    )

    # 工作流定义消失后仍保留所有历史，但不能继续冒充当前项目。
    sources[:] = [source for source in sources if source.dag_id != "quant_prices"]
    assert (
        client.get(
            "/api/history/runs", params={"project_kind": "project", "project": "quant"}
        ).json()["runs"]
        == []
    )
    unknown_page = client.get(
        "/api/history/runs", params={"project_kind": "unknown", "limit": 100}
    ).json()
    assert unknown_page["next_cursor"] is not None
    assert any(row["dag_id"] == "quant_prices" for row in unknown_page["runs"])


def test_history_tracks_current_project_and_reports_discovery_failure(
    tmp_path, monkeypatch
):
    app, client, sources, _ = setup_catalog(tmp_path, monkeypatch)
    with sqlite3.connect(app.state.service.store.path) as connection:
        insert_run(connection, "quant_prices", "old-run")
    login(client)
    query = {"project_kind": "project", "project": "quant"}
    assert len(client.get("/api/history/runs", params=query).json()["runs"]) == 1

    source = next(item for item in sources if item.dag_id == "quant_prices")
    source.dag.tags = ("project:analytics", "moved")
    assert client.get("/api/history/runs", params=query).json()["runs"] == []
    moved = client.get(
        "/api/history/runs",
        params={"project_kind": "project", "project": "analytics", "tag": "moved"},
    ).json()["runs"]
    assert [(row["run_id"], row["project"]) for row in moved] == [
        ("old-run", "analytics")
    ]

    def failed_discovery():
        raise ValueError("DAG 目录不可用")

    monkeypatch.setattr(app.state.service, "discover", failed_discovery)
    all_history = client.get("/api/history/runs").json()
    assert all_history["metadata_error"] == "DAG 目录不可用"
    assert [(row["run_id"], row["project_kind"]) for row in all_history["runs"]] == [
        ("old-run", "unknown")
    ]
    assert len(client.get("/api/catalog").json()["issues"]) == 1
    facets = client.get("/api/history/runs/facets").json()
    assert facets["complete"] is False and facets["pending"] is False
    assert facet_group(facets, "unknown")["total"] == 1


def test_all_discovered_projects_are_selectable_and_search_is_literal(
    tmp_path, monkeypatch
):
    app, client, _, add = setup_catalog(tmp_path, monkeypatch)
    project = "p" * 1001
    add("long_project", [f"project:{project}", "special%_tag"])
    with sqlite3.connect(app.state.service.store.path) as connection:
        insert_run(connection, "long_project", "special%_run")
    login(client)
    params = {"project_kind": "project", "project": project, "q": "%_"}
    assert project in client.get("/api/catalog").json()["projects"]
    workflows = client.get("/api/workflows", params=params)
    assert workflows.status_code == 200
    assert [item["dag_id"] for item in workflows.json()["dags"]] == ["long_project"]
    history = client.get("/api/history/runs", params=params)
    assert history.status_code == 200
    assert [item["run_id"] for item in history.json()["runs"]] == ["special%_run"]


def test_workflow_page_reads_state_without_reinitializing_database(
    tmp_path, monkeypatch
):
    app, client, sources, _ = setup_catalog(tmp_path, monkeypatch)
    dag = DAG(
        "once_flow",
        schedule="@once",
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        auto_register=False,
    )
    sources.append(DagSource(dag.dag_id, tmp_path / "once.py", dag))
    with sqlite3.connect(app.state.service.store.path) as connection:
        insert_run(connection, "once_flow", "scheduled", "success")
        connection.execute(
            "UPDATE dag_run SET run_type = 'scheduled' "
            "WHERE dag_id = 'once_flow' AND run_id = 'scheduled'"
        )
        insert_run(connection, "once_flow", "running", "running")
        insert_run(connection, "once_flow", "queued", "queued")
        insert_run(connection, "once_flow", "last", "failed")
    login(client)

    def unexpected_init():
        raise AssertionError("只读列表不应重复初始化状态库")

    monkeypatch.setattr(app.state.service.store, "init_db", unexpected_init)
    body = client.get("/api/workflows", params={"q": "once_flow"}).json()
    row = body["dags"][0]
    assert row["latest_run"]["run_id"] == "last"
    assert row["running_count"] == 1
    assert row["queued_count"] == 1
    assert row["next_schedule"]["status"] == "completed"
    assert body["summary"] == {"total": 1, "running": 1, "failed": 1}


def facet_group(body, kind, project=None):
    return next(
        row
        for row in body["groups"]
        if row["project_kind"] == kind and row["project"] == project
    )


def test_tag_union_and_workflow_facets_cover_every_page(tmp_path, monkeypatch):
    app, client, _, add = setup_catalog(tmp_path, monkeypatch)
    add("without_tag", ["project:analytics"])
    with sqlite3.connect(app.state.service.store.path) as connection:
        insert_run(connection, "quant_report", "failed", "failed")
        insert_run(connection, "analytics_report", "failed", "failed")
    login(client)
    params = [("tag", "prices"), ("tag", "report")]
    listing = client.get("/api/workflows", params=params + [("limit", "1")]).json()
    assert len(listing["dags"]) == 1 and listing["next_cursor"]
    facets = client.get("/api/workflows/facets", params=params).json()
    assert facets["complete"] is True and facets["calculated_at"]
    assert facet_group(facets, "project", "quant")["total"] == 2
    assert facet_group(facets, "project", "quant")["matched"] == 2
    assert facet_group(facets, "project", "analytics")["total"] == 2
    assert facet_group(facets, "project", "analytics")["matched"] == 1
    failed = client.get(
        "/api/workflows/facets", params=params + [("state", "failed")]
    ).json()
    assert facet_group(failed, "project", "quant")["matched"] == 1
    assert facet_group(failed, "project", "analytics")["matched"] == 1
    assert facet_group(failed, "unassigned")["matched"] == 0
    # 无标签选择表示不限制，项目中没有普通标签的 DAG 仍可见。
    assert any(
        row["dag_id"] == "without_tag"
        for row in client.get("/api/workflows").json()["dags"]
    )
    assert not any(
        row["dag_id"] == "without_tag"
        for row in client.get("/api/workflows", params=params).json()["dags"]
    )


def test_history_facets_and_exact_dag_id_include_all_runs(tmp_path, monkeypatch):
    app, client, _, add = setup_catalog(tmp_path, monkeypatch)
    add("quant_prices_extra", ["project:analytics", "report"])
    with sqlite3.connect(app.state.service.store.path) as connection:
        for index in range(80):
            insert_run(
                connection,
                "quant_prices",
                f"quant-{index:03d}",
                "failed" if index % 2 else "success",
            )
        insert_run(connection, "quant_prices_extra", "other", "failed")
        insert_run(connection, "removed", "old", "failed")
    login(client)
    params = [("dag_id", "quant_prices"), ("tag", "prices"), ("tag", "report")]
    first = client.get("/api/history/runs", params=params + [("limit", "7")]).json()
    assert len(first["runs"]) == 7 and first["next_cursor"]
    assert all(row["dag_id"] == "quant_prices" for row in first["runs"])
    facets = client.get(
        "/api/history/runs/facets", params=params + [("state", "failed")]
    ).json()
    assert facet_group(facets, "project", "quant") == {
        "project_kind": "project",
        "project": "quant",
        "total": 80,
        "matched": 40,
    }
    assert facet_group(facets, "project", "analytics")["total"] == 1
    assert facet_group(facets, "project", "analytics")["matched"] == 0
    assert facet_group(facets, "project", "Straße")["total"] == 0
    assert facet_group(facets, "unknown")["total"] == 1
    assert facet_group(facets, "unknown")["matched"] == 0
    # 匹配任一标签：两个不同标签的工作流都应出现。
    union = client.get(
        "/api/history/runs",
        params=[("tag", "prices"), ("tag", "report"), ("limit", "100")],
    ).json()["runs"]
    assert {row["dag_id"] for row in union} == {"quant_prices", "quant_prices_extra"}


def test_facet_cache_separates_filters_and_metadata_and_expires(tmp_path, monkeypatch):
    import easyflow.web.service as service_module

    app, client, sources, _ = setup_catalog(tmp_path, monkeypatch)
    login(client)
    clock = [100.0]
    monkeypatch.setattr(
        service_module, "time", SimpleNamespace(monotonic=lambda: clock[0])
    )
    calls = []
    original = app.state.service.workflow_states

    def counted(ids):
        calls.append(tuple(ids))
        return original(ids)

    monkeypatch.setattr(app.state.service, "workflow_states", counted)
    first = client.get("/api/workflows/facets", params={"q": "quant"}).json()
    second = client.get("/api/workflows/facets", params={"q": "quant"}).json()
    assert first == second and len(calls) == 1
    client.get("/api/workflows/facets", params={"q": "analytics"})
    assert len(calls) == 2
    source = next(item for item in sources if item.dag_id == "quant_prices")
    source.dag.tags = ("project:changed", "prices")
    changed = client.get("/api/workflows/facets", params={"q": "quant"}).json()
    assert facet_group(changed, "project", "changed")["total"] == 1
    assert len(calls) == 3
    clock[0] += 6
    client.get("/api/workflows/facets", params={"q": "quant"})
    assert len(calls) == 4
    monkeypatch.setattr(
        app.state.service,
        "discover",
        lambda: DagDiscoveryResult(list(sources), [], True),
    )
    assert client.get("/api/workflows/facets").json()["complete"] is False


def test_concurrent_facet_requests_share_calculation(tmp_path, monkeypatch):
    app, _, _, _ = setup_catalog(tmp_path, monkeypatch)
    service = app.state.service
    started = threading.Event()
    release = threading.Event()
    calls = []

    def calculate():
        calls.append(1)
        started.set()
        assert release.wait(5)
        return {"matched": 1}

    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs = [
            pool.submit(service._cached_facets, ("same",), calculate) for _ in range(8)
        ]
        assert started.wait(5)
        release.set()
        assert all(job.result(timeout=5) == {"matched": 1} for job in jobs)
    assert calls == [1]


def test_many_selected_tags_do_not_exceed_sql_parameter_limits(tmp_path, monkeypatch):
    app, client, _, _ = setup_catalog(tmp_path, monkeypatch)
    with sqlite3.connect(app.state.service.store.path) as connection:
        insert_run(connection, "quant_prices", "only-run")
    login(client)
    tags = [("tag", f"extra-{index:04d}") for index in range(1100)] + [
        ("tag", "prices")
    ]
    response = client.get("/api/history/runs", params=tags)
    assert response.status_code == 200
    assert [row["run_id"] for row in response.json()["runs"]] == ["only-run"]
    facets = client.get("/api/history/runs/facets", params=tags)
    assert facets.status_code == 200
    assert facet_group(facets.json(), "project", "quant")["matched"] == 1
