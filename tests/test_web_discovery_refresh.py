from __future__ import annotations

from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from easyflow._internal import dag_parser
from easyflow._internal.dag_parser import (
    AsyncDagDiscovery,
    DagDiscoveryResult,
    DagParseIssue,
    DagSource,
)
from easyflow.sdk import DAG
from easyflow.web.app import create_app


@pytest.fixture
def parser_control(monkeypatch):
    """独立控制时间和解析完成顺序，不用真实等待触发刷新。"""
    control = SimpleNamespace(now=100.0, jobs={}, submitted=[], wait=Mock())

    def submit(function, path):
        future = Future()
        future.set_running_or_notify_cancel()
        control.jobs[path] = future
        control.submitted.append(path)
        return future

    pool = SimpleNamespace(submit=submit, shutdown=lambda **options: None)
    monkeypatch.setattr(dag_parser, "ThreadPoolExecutor", lambda **options: pool)
    monkeypatch.setattr(
        dag_parser, "time", SimpleNamespace(monotonic=lambda: control.now)
    )
    monkeypatch.setattr(dag_parser, "wait", control.wait)
    return control


def parsed(path, dag_id="demo"):
    dag = DAG(dag_id, tags=["project:refresh"], auto_register=False)
    return DagDiscoveryResult([DagSource(dag_id, path, dag)], [])


@pytest.mark.parametrize("wait_seconds", [0, 0.5])
def test_unchanged_snapshot_stays_complete_during_refresh(
    tmp_path, parser_control, wait_seconds
):
    path = tmp_path / "demo.py"
    path.touch()
    discovery = AsyncDagDiscovery(tmp_path)
    assert discovery.scan().pending
    parser_control.jobs[path].set_result(parsed(path))
    assert not discovery.scan().pending

    parser_control.now += 6
    refreshing = discovery.scan(wait_seconds=wait_seconds)
    assert [source.dag_id for source in refreshing.sources] == ["demo"]
    assert refreshing.pending is False
    assert not parser_control.jobs[path].done()
    assert parser_control.submitted == [path, path]
    parser_control.wait.assert_not_called()

    parser_control.jobs[path].set_result(parsed(path, "refreshed"))
    finished = discovery.scan()
    assert not finished.pending
    assert [source.dag_id for source in finished.sources] == ["refreshed"]


def test_changed_file_rejects_old_inflight_snapshot(tmp_path, parser_control):
    path = tmp_path / "demo.py"
    path.touch()
    discovery = AsyncDagDiscovery(tmp_path)
    discovery.scan()
    parser_control.jobs[path].set_result(parsed(path))
    discovery.scan()

    parser_control.now += 6
    discovery.scan()
    obsolete = parser_control.jobs[path]
    path.write_text("# 文件在后台刷新期间发生变化\n", encoding="utf-8")
    changed = discovery.scan(wait_seconds=0.5)
    assert changed.pending and not changed.sources
    parser_control.wait.assert_called_once()

    obsolete.set_result(parsed(path, "obsolete"))
    still_loading = discovery.scan()
    assert still_loading.pending and not still_loading.sources
    assert parser_control.jobs[path] is not obsolete
    parser_control.jobs[path].set_result(parsed(path, "updated"))
    finished = discovery.scan()
    assert not finished.pending
    assert [source.dag_id for source in finished.sources] == ["updated"]


def test_new_and_deleted_files_during_background_refresh(tmp_path, parser_control):
    path = tmp_path / "demo.py"
    path.touch()
    discovery = AsyncDagDiscovery(tmp_path)
    discovery.scan()
    parser_control.jobs[path].set_result(parsed(path))
    discovery.scan()
    parser_control.now += 6
    discovery.scan()

    added = tmp_path / "added.py"
    added.touch()
    incomplete = discovery.scan()
    assert incomplete.pending
    assert [source.dag_id for source in incomplete.sources] == ["demo"]

    added.unlink()
    complete = discovery.scan()
    assert not complete.pending
    assert [source.dag_id for source in complete.sources] == ["demo"]
    path.unlink()
    empty = discovery.scan()
    assert not empty.pending and not empty.sources and not empty.issues


def test_successful_file_without_dags_is_complete(tmp_path, parser_control):
    path = tmp_path / "helper.py"
    path.touch()
    discovery = AsyncDagDiscovery(tmp_path)
    assert discovery.scan().pending
    parser_control.jobs[path].set_result(DagDiscoveryResult([], []))
    assert not discovery.scan().pending
    parser_control.now += 6
    refreshing = discovery.scan()
    assert not refreshing.pending and not refreshing.sources
    assert not parser_control.jobs[path].done()


@pytest.mark.parametrize("raises", [False, True])
def test_background_errors_remain_visible_until_success(
    tmp_path, parser_control, raises
):
    path = tmp_path / "demo.py"
    path.touch()
    discovery = AsyncDagDiscovery(tmp_path)
    discovery.scan()
    parser_control.jobs[path].set_result(parsed(path))
    discovery.scan()
    parser_control.now += 6
    discovery.scan()
    if raises:
        parser_control.jobs[path].set_exception(RuntimeError("解析失败"))
    else:
        parser_control.jobs[path].set_result(
            DagDiscoveryResult([], [DagParseIssue(path, None, "解析失败")])
        )
    failed = discovery.scan()
    assert not failed.pending and not failed.sources
    assert [issue.message for issue in failed.issues] == ["解析失败"]

    parser_control.now += 6
    retrying = discovery.scan()
    assert not retrying.pending and retrying.issues == failed.issues
    parser_control.jobs[path].set_result(parsed(path))
    recovered = discovery.scan()
    assert not recovered.pending and not recovered.issues
    assert [source.dag_id for source in recovered.sources] == ["demo"]


def test_web_loading_and_facets_follow_snapshot_availability(
    tmp_path, parser_control
):
    folder = tmp_path / "dags"
    folder.mkdir()
    path = folder / "demo.py"
    path.touch()
    app = create_app(
        dags_folder=str(folder),
        database_url=str(tmp_path / "state.db"),
        log_root=str(tmp_path / "logs"),
    )
    app.state.auth.create_user("reader", "readonly-password", "viewer")
    endpoints = {
        "/api/catalog": "loading",
        "/api/workflows": "loading",
        "/api/history/runs": "metadata_loading",
        "/api/workflows/facets": "pending",
        "/api/history/runs/facets": "pending",
    }

    def assert_status(client, *, pending, complete):
        for endpoint, loading_key in endpoints.items():
            response = client.get(endpoint)
            assert response.status_code == 200, response.text
            payload = response.json()
            assert payload[loading_key] is pending, (endpoint, payload)
            if "complete" in payload:
                assert payload["complete"] is complete, (endpoint, payload)

    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        assert client.post(
            "/api/auth/login",
            json={"username": "reader", "password": "readonly-password"},
        ).status_code == 200
        assert_status(client, pending=True, complete=False)
        parser_control.jobs[path].set_result(parsed(path))
        assert_status(client, pending=False, complete=True)

        parser_control.now += 6
        parser_control.wait.reset_mock()
        assert_status(client, pending=False, complete=True)
        assert not parser_control.jobs[path].done()
        parser_control.wait.assert_not_called()
        assert len(client.get("/api/workflows").json()["dags"]) == 1

        path.write_text("# 修改后须重新解析\n", encoding="utf-8")
        assert_status(client, pending=True, complete=False)
        parser_control.jobs[path].set_result(parsed(path))
        assert_status(client, pending=True, complete=False)
        parser_control.jobs[path].set_result(
            DagDiscoveryResult([], [DagParseIssue(path, None, "真实解析错误")])
        )
        assert_status(client, pending=False, complete=False)
        assert "真实解析错误" in client.get("/api/catalog").json()["issues"][0]

        parser_control.now += 6
        assert_status(client, pending=False, complete=False)
        parser_control.jobs[path].set_result(parsed(path))
        assert_status(client, pending=False, complete=True)
