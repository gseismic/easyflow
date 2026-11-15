from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from easyflow._internal.state_store import SQLiteStateStore
from easyflow.web import uptime
from easyflow.web.app import create_app
from easyflow.web.uptime import workflow_failures, workflow_uptime

START = datetime(2026, 1, 2, tzinfo=UTC)


def add_run(
    connection, run_id, *, dag_id="demo", state="success", start=None, end=None
):
    connection.execute(
        "INSERT INTO dag_run (dag_id, run_id, run_type, logical_date, data_interval_start, "
        "data_interval_end, state, queued_at, start_date, end_date) "
        "VALUES (?, ?, 'backfill', ?, ?, ?, ?, ?, ?, ?)",
        (
            dag_id,
            run_id,
            "2020-01-01T00:00:00+00:00",
            "2020-01-01T00:00:00+00:00",
            "2020-01-01T00:00:00+00:00",
            state,
            START.isoformat(),
            start,
            end,
        ),
    )


def add_try(
    connection,
    run_id,
    *,
    start,
    end,
    state="success",
    task_id="work",
    attempt=1,
    dag_id="demo",
):
    connection.execute(
        "INSERT OR IGNORE INTO task_instance (dag_id, run_id, task_id) VALUES (?, ?, ?)",
        (dag_id, run_id, task_id),
    )
    connection.execute(
        "INSERT INTO task_try (dag_id, run_id, task_id, try_number, state, started_at, ended_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (dag_id, run_id, task_id, attempt, state, start, end),
    )


def test_failure_details_preserve_recovered_attempts_and_paginate_all_failures(
    tmp_path,
):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    with sqlite3.connect(store.path) as connection:
        add_run(connection, "recovered", end="2026-01-02T00:25:00+00:00")
        for attempt, minute, state in [
            (1, 10, "failed"),
            (2, 20, "failed"),
            (3, 25, "success"),
        ]:
            add_try(
                connection,
                "recovered",
                task_id="publish",
                attempt=attempt,
                start="2026-01-02T00:01:00+00:00",
                end=f"2026-01-02T00:{minute}:00+00:00",
                state=state,
            )
        add_try(
            connection,
            "recovered",
            task_id="validate",
            state="failed",
            start="2026-01-02T00:01:00+00:00",
            end="2026-01-02T00:10:00+00:00",
        )
        connection.execute(
            "UPDATE task_try SET error_code = 'TimeoutError', error_message = ? "
            "WHERE run_id = 'recovered' AND task_id = 'publish' AND try_number = 1",
            ("数据源连接超时" * 400,),
        )
        add_run(connection, "failed", state="failed", end="2026-01-02T00:12:00+00:00")
        add_try(
            connection,
            "failed",
            state="failed",
            start=None,
            end="2026-01-02T00:12:00+00:00",
        )
        add_run(connection, "legacy", state="failed", end="2026-01-02T00:22:00+00:00")
    params = {
        "dag_id": "demo",
        "start_at": START,
        "end_at": START + timedelta(minutes=30),
        "now": START + timedelta(hours=1),
    }
    pages = [
        workflow_failures(store.path, **params, limit=2, offset=i) for i in [0, 2, 4]
    ]
    assert all(page["total"] == 5 and page["failed_run_count"] == 3 for page in pages)
    assert [page["next_offset"] for page in pages] == [2, 4, None]
    items = [item for page in pages for item in page["items"]]
    assert [
        (item["run_id"], item["task_id"], item["try_number"]) for item in items
    ] == [
        ("legacy", None, None),
        ("recovered", "publish", 2),
        ("failed", "work", 1),
        ("recovered", "publish", 1),
        ("recovered", "validate", 1),
    ]
    assert items[3]["run_state"] == "success"
    assert items[3]["error_code"] == "TimeoutError"
    assert (
        len(items[3]["error_message"]) == 2000 and items[3]["error_message_truncated"]
    )
    assert items[4]["error_message"] is None and not items[4]["error_message_truncated"]
    assert items[0]["started_at"] is None and items[0]["run_state"] == "failed"
    assert "traceback" not in items[3] and "log_path" not in items[3]
    slot = workflow_uptime(
        store.path,
        dag_ids=["demo"],
        **{k: v for k, v in params.items() if k != "dag_id"},
    )["workflows"]["demo"][0]
    assert slot["failed_run_count"] == pages[0]["failed_run_count"]


def test_failure_details_use_actual_microsecond_boundaries_and_exclude_future(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    with sqlite3.connect(store.path) as connection:
        for run_id, ended in [
            ("before", "2026-01-01T23:59:59.999999+00:00"),
            ("left", "2026-01-02T08:00:00+08:00"),
            ("last", "2026-01-02T00:29:59.999999+00:00"),
            ("right", "2026-01-02T00:30:00+00:00"),
            ("future", "2026-01-02T00:40:00+00:00"),
        ]:
            add_run(connection, run_id)
            add_try(connection, run_id, start=None, end=ended, state="failed")
        add_run(connection, "other", dag_id="other")
        add_try(
            connection,
            "other",
            dag_id="other",
            start=None,
            end=START.isoformat(),
            state="failed",
        )
        # 任务失败与运行失败发生在不同格时，保留各自所在格的原有标红语义。
        add_run(connection, "cross", state="failed", end="2026-01-02T00:05:00+00:00")
        add_try(
            connection,
            "cross",
            start=None,
            end="2026-01-01T23:59:00+00:00",
            state="failed",
        )
    data = workflow_failures(
        store.path,
        dag_id="demo",
        start_at=START,
        end_at=START + timedelta(minutes=30),
        now=START + timedelta(hours=1),
    )
    assert [item["run_id"] for item in data["items"]] == ["last", "cross", "left"]
    assert data["items"][1]["task_id"] is None
    current = workflow_failures(
        store.path,
        dag_id="demo",
        start_at=START,
        end_at=START + timedelta(hours=1),
        now=START + timedelta(minutes=30),
    )
    assert "right" in {item["run_id"] for item in current["items"]}
    assert "future" not in {item["run_id"] for item in current["items"]}
    empty = workflow_failures(
        store.path,
        dag_id="removed",
        start_at=START,
        end_at=START + timedelta(hours=25),
    )
    assert empty["items"] == [] and empty["total"] == 0 and empty["next_offset"] is None


def test_failure_api_auth_validation_special_ids_and_exact_attempt_log(tmp_path):
    dags = tmp_path / "dags"
    dags.mkdir()
    logs = tmp_path / "logs"
    logs.mkdir()
    app = create_app(
        dags_folder=str(dags),
        database_url=str(tmp_path / "state.db"),
        log_root=str(logs),
    )
    app.state.auth.create_user("reader", "readonly-password", "viewer")
    dag_id, run_id, task_id = "flow/中文?+%", "run/失败?+%", "task/发布?+%"
    with sqlite3.connect(app.state.service.store.path) as connection:
        add_run(connection, run_id, dag_id=dag_id)
        for attempt, state in [(1, "failed"), (2, "success")]:
            add_try(
                connection,
                run_id,
                dag_id=dag_id,
                task_id=task_id,
                attempt=attempt,
                state=state,
                start=START.isoformat(),
                end=(START + timedelta(minutes=attempt)).isoformat(),
            )
            log = logs / f"attempt-{attempt}.log"
            log.write_text(f"第 {attempt} 次尝试：{state}", encoding="utf-8")
            connection.execute(
                "UPDATE task_try SET log_path = ? WHERE try_number = ?",
                (str(log), attempt),
            )
    params = {
        "dag_id": dag_id,
        "start_at": START.isoformat(),
        "end_at": (START + timedelta(minutes=30)).isoformat(),
    }
    endpoint = "/api/workflows/uptime/failures"
    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        assert client.get(endpoint, params=params).status_code == 401
        assert (
            client.post(
                "/api/auth/login",
                json={"username": "reader", "password": "readonly-password"},
            ).status_code
            == 200
        )
        response = client.get(endpoint, params=params)
        assert (
            response.status_code == 200
            and response.headers["cache-control"] == "no-store"
        )
        item = response.json()["items"][0]
        assert (
            item["run_id"],
            item["task_id"],
            item["try_number"],
            item["run_state"],
        ) == (run_id, task_id, 1, "success")
        log = client.get(
            "/api/history/task-log",
            params={
                "dag_id": dag_id,
                "run_id": item["run_id"],
                "task_id": item["task_id"],
                "attempt": item["try_number"],
            },
        )
        assert log.status_code == 200 and log.json()["content"] == "第 1 次尝试：failed"
        for patch in [
            {"dag_id": ""},
            {"start_at": "2026-01-02T00:00:00"},
            {"end_at": params["start_at"]},
            {"end_at": (START + timedelta(hours=25, microseconds=1)).isoformat()},
        ]:
            assert client.get(endpoint, params={**params, **patch}).status_code == 400
        for patch in [
            {"limit": 0},
            {"limit": 101},
            {"offset": -1},
            {"offset": 2**63},
            {"start_at": "invalid"},
        ]:
            assert client.get(endpoint, params={**params, **patch}).status_code == 422


def test_uptime_crosses_slots_keeps_retry_errors_and_uses_actual_time(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    with sqlite3.connect(store.path) as connection:
        add_run(
            connection,
            "retry",
            start="2026-01-01T23:45:00+00:00",
            end="2026-01-02T01:31:00+00:00",
        )
        add_try(
            connection,
            "retry",
            start="2026-01-01T23:45:00+00:00",
            end="2026-01-02T01:00:00+00:00",
            state="failed",
        )
        add_try(
            connection,
            "retry",
            start="2026-01-02T01:30:00+00:00",
            end="2026-01-02T01:31:00+00:00",
            attempt=2,
        )
        add_run(connection, "live", state="running", start="2026-01-02T02:45:00+00:00")
        add_try(
            connection,
            "live",
            start="2026-01-02T02:45:00+00:00",
            end=None,
            state="running",
        )
        add_run(connection, "queued", state="queued")
        add_run(
            connection,
            "legacy",
            start="2026-01-02T00:05:00+00:00",
            end="2026-01-02T00:06:00+00:00",
        )
        add_run(
            connection, "startup-error", state="failed", end="2026-01-02T02:10:00+00:00"
        )
    data = workflow_uptime(
        store.path,
        dag_ids=["demo", "empty"],
        start_at=START,
        end_at=START + timedelta(days=1),
        now=START + timedelta(hours=3, minutes=10),
    )
    slots = data["workflows"]["demo"]
    assert len(slots) == 48
    assert [slot["state"] for slot in slots[:8]] == [
        "active",
        "active",
        "failed",
        "active",
        "failed",
        "active",
        "running",
        "future",
    ]
    assert slots[0]["run_count"] == 2
    assert slots[2]["run_count"] == 0
    assert slots[2]["failed_run_count"] == 1
    assert slots[2]["sample_run_id"] == "retry"
    assert slots[2]["sample_task_id"] == "work"
    assert slots[2]["sample_try_number"] == 1
    assert slots[4]["sample_run_id"] == "startup-error"
    assert slots[4]["sample_task_id"] is None
    assert data["workflows"]["empty"][0]["state"] == "idle"
    assert all(
        (
            datetime.fromisoformat(s["end_at"]) - datetime.fromisoformat(s["start_at"])
        ).total_seconds()
        == 1800
        for s in slots
    )


def test_uptime_clear_does_not_fill_gap_and_failure_is_not_hidden_by_success(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    with sqlite3.connect(store.path) as connection:
        add_run(connection, "rerun", state="running", start="2026-01-02T00:05:00+00:00")
        add_try(
            connection,
            "rerun",
            start="2026-01-02T00:05:00+00:00",
            end="2026-01-02T00:06:00+00:00",
            state="failed",
        )
        add_try(
            connection,
            "rerun",
            start="2026-01-02T12:05:00+00:00",
            end=None,
            state="running",
            attempt=2,
        )
        add_run(
            connection,
            "same-slot-success",
            start="2026-01-02T00:10:00+00:00",
            end="2026-01-02T00:11:00+00:00",
        )
    slots = workflow_uptime(
        store.path,
        dag_ids=["demo"],
        start_at=START,
        end_at=START + timedelta(days=1),
        now=START + timedelta(hours=12, minutes=10),
    )["workflows"]["demo"]
    assert slots[0]["state"] == "failed" and slots[0]["run_count"] == 2
    assert slots[0]["sample_run_id"] == "rerun"
    assert all(slot["state"] == "idle" for slot in slots[1:24])
    assert slots[24]["state"] == "running"


def test_uptime_counts_all_high_frequency_runs_and_preserves_microsecond_boundaries(
    tmp_path,
):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    with sqlite3.connect(store.path) as connection:
        for index in range(1205):
            add_run(
                connection,
                f"run-{index}",
                start="2026-01-02T00:05:00+00:00",
                end="2026-01-02T00:06:00+00:00",
            )
        add_run(connection, "edge")
        add_try(
            connection,
            "edge",
            start="2026-01-02T00:29:59.999999+00:00",
            end="2026-01-02T00:29:59.999999+00:00",
            state="failed",
        )
        add_run(
            connection,
            "instant",
            start="2026-01-02T00:30:00+00:00",
            end="2026-01-02T00:30:00+00:00",
        )
        add_run(connection, "outside", state="failed", end="2026-01-03T00:00:00+00:00")
    slots = workflow_uptime(
        store.path,
        dag_ids=["demo"],
        start_at=START,
        end_at=START + timedelta(days=1),
        now=START + timedelta(days=2),
    )["workflows"]["demo"]
    assert slots[0]["run_count"] == 1206
    assert slots[0]["failed_run_count"] == 1 and slots[0]["state"] == "failed"
    assert slots[1]["run_count"] == 1 and slots[1]["failed_run_count"] == 0
    assert slots[-1]["failed_run_count"] == 0


def test_uptime_api_auth_viewer_timezone_and_range_validation(tmp_path):
    dags = tmp_path / "dags"
    dags.mkdir()
    app = create_app(
        dags_folder=str(dags),
        database_url=str(tmp_path / "state.db"),
        log_root=str(tmp_path / "logs"),
    )
    app.state.auth.create_user("reader", "readonly-password", "viewer")
    params = {
        "dag_id": ["a/中文?+", "demo"],
        "start_at": "2026-01-02T00:00:00+08:00",
        "end_at": "2026-01-03T00:00:00+08:00",
    }
    with TestClient(app, base_url="http://127.0.0.1:8790") as client:
        assert client.get("/api/workflows/uptime", params=params).status_code == 401
        assert (
            client.post(
                "/api/auth/login",
                json={"username": "reader", "password": "readonly-password"},
            ).status_code
            == 200
        )
        result = client.get("/api/workflows/uptime", params=params)
        assert result.status_code == 200
        assert result.headers["cache-control"] == "no-store"
        data = result.json()
        assert data["start_at"] == "2026-01-01T16:00:00+00:00"
        assert len(data["workflows"]["a/中文?+"]) == 48
        for patch in (
            {"start_at": "2026-01-02T00:00:00"},
            {"end_at": params["start_at"]},
            {"end_at": "2026-01-03T01:30:00+08:00"},
            {"end_at": "2026-01-02T00:31:00+08:00"},
            {"dag_id": ""},
        ):
            assert (
                client.get(
                    "/api/workflows/uptime", params={**params, **patch}
                ).status_code
                == 400
            )
        assert (
            client.get(
                "/api/workflows/uptime",
                params={**params, "dag_id": [f"dag-{i}" for i in range(101)]},
            ).status_code
            == 422
        )
        daily = {
            **params,
            "granularity": "day",
            "timezone": "Asia/Shanghai",
            "end_at": "2026-01-09T00:00:00+08:00",
        }
        result = client.get("/api/workflows/uptime", params=daily)
        assert (
            result.status_code == 200 and len(result.json()["workflows"]["demo"]) == 7
        )
        for patch in (
            {"timezone": "invalid/timezone"},
            {"start_at": "2026-01-02T00:01:00+08:00"},
            {"end_at": "2026-02-03T00:00:00+08:00"},
            {"end_at": params["start_at"]},
        ):
            assert (
                client.get(
                    "/api/workflows/uptime", params={**daily, **patch}
                ).status_code
                == 400
            )
        assert (
            client.get(
                "/api/workflows/uptime", params={**daily, "granularity": "week"}
            ).status_code
            == 422
        )


@pytest.mark.parametrize("hours", [23, 25])
def test_uptime_supports_dst_days_with_fixed_half_hour_slots(tmp_path, hours):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    data = workflow_uptime(
        store.path,
        dag_ids=["demo"],
        start_at=START,
        end_at=START + timedelta(hours=hours),
        now=START + timedelta(days=2),
    )
    assert len(data["workflows"]["demo"]) == hours * 2


def test_current_waiting_is_not_execution_and_failure_link_prefers_task(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    with sqlite3.connect(store.path) as connection:
        add_run(connection, "waiting", state="queued")
        add_run(
            connection,
            "failure",
            state="failed",
            start="2026-01-02T00:05:00+00:00",
            end="2026-01-02T00:10:00+00:00",
        )
        add_try(
            connection,
            "failure",
            task_id="publish",
            start="2026-01-02T00:05:00+00:00",
            end="2026-01-02T00:10:00+00:00",
            state="failed",
        )
    slots = workflow_uptime(
        store.path,
        dag_ids=["demo"],
        start_at=START,
        end_at=START + timedelta(days=1),
        now=START + timedelta(hours=2),
    )["workflows"]["demo"]
    assert slots[0]["state"] == "failed"
    assert (
        slots[0]["sample_task_id"] == "publish" and slots[0]["sample_try_number"] == 1
    )
    assert slots[4]["state"] == "waiting" and slots[4]["waiting_run_count"] == 1
    assert slots[4]["run_count"] == 0
    assert slots[3]["state"] == "idle" and slots[5]["state"] == "future"


def test_daily_aggregation_uses_local_midnight_and_deduplicates_runs(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    with sqlite3.connect(store.path) as connection:
        add_run(
            connection,
            "span",
            start="2026-01-01T23:50:00+08:00",
            end="2026-01-02T01:10:00+08:00",
        )
        add_try(
            connection,
            "span",
            start="2026-01-01T23:50:00+08:00",
            end="2026-01-02T00:00:00+08:00",
            state="failed",
        )
        add_try(
            connection,
            "span",
            start="2026-01-02T00:10:00+08:00",
            end="2026-01-02T01:10:00+08:00",
            attempt=2,
        )
        add_try(
            connection,
            "span",
            start="2026-01-02T00:20:00+08:00",
            end="2026-01-02T00:30:00+08:00",
            task_id="other",
        )
        add_run(connection, "waiting", state="queued")
    data = workflow_uptime(
        store.path,
        dag_ids=["demo"],
        start_at=datetime.fromisoformat("2026-01-01T00:00:00+08:00"),
        end_at=datetime.fromisoformat("2026-01-08T00:00:00+08:00"),
        granularity="day",
        timezone_name="Asia/Shanghai",
        now=datetime.fromisoformat("2026-01-03T03:00:00+08:00"),
    )
    slots = data["workflows"]["demo"]
    assert len(slots) == 7
    assert data["granularity"] == "day" and data["slot_minutes"] is None
    assert data["timezone"] == "Asia/Shanghai"
    assert slots[0]["start_at"] == "2025-12-31T16:00:00+00:00"
    assert [slot["state"] for slot in slots[:4]] == [
        "active",
        "failed",
        "waiting",
        "future",
    ]
    assert slots[0]["run_count"] == 1 and slots[0]["failed_run_count"] == 0
    assert slots[1]["run_count"] == 1 and slots[1]["failed_run_count"] == 1
    assert slots[1]["sample_task_id"] == "work" and slots[1]["sample_try_number"] == 1


@pytest.mark.parametrize(
    "month,days", [("2026-02", 28), ("2024-02", 29), ("2026-04", 30), ("2026-01", 31)]
)
def test_daily_aggregation_covers_the_whole_month(tmp_path, month, days):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    start = datetime.fromisoformat(f"{month}-01T00:00:00+00:00")
    data = workflow_uptime(
        store.path,
        dag_ids=["demo"],
        start_at=start,
        end_at=start + timedelta(days=days),
        granularity="day",
    )
    assert len(data["workflows"]["demo"]) == days


@pytest.mark.parametrize(
    "day,hours", [("2026-03-07", [24, 23, 24]), ("2026-10-31", [24, 25, 24])]
)
def test_daily_slots_and_current_waiting_follow_dst_boundaries(tmp_path, day, hours):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    with sqlite3.connect(store.path) as connection:
        add_run(connection, "waiting", state="queued")
    start = datetime.fromisoformat(f"{day}T00:00:00").replace(
        tzinfo=ZoneInfo("America/New_York")
    )
    data = workflow_uptime(
        store.path,
        dag_ids=["demo"],
        start_at=start,
        end_at=start + timedelta(days=3),
        granularity="day",
        timezone_name="America/New_York",
        now=start + timedelta(days=1, hours=23, minutes=30),
    )
    slots = data["workflows"]["demo"]
    durations = [
        (
            datetime.fromisoformat(slot["end_at"])
            - datetime.fromisoformat(slot["start_at"])
        ).total_seconds()
        / 3600
        for slot in slots
    ]
    assert durations == hours
    assert [slot["state"] for slot in slots] == ["idle", "waiting", "future"]


@pytest.mark.parametrize(
    "zone,first,last,hours",
    [
        ("America/Santiago", "2026-09-06T01:00:00-03:00", "2026-09-08T00:00:00-03:00", [23, 24]),
        ("America/Asuncion", "2017-10-01T01:00:00-03:00", "2017-11-01T00:00:00-03:00", [23] + [24] * 30),
    ],
)
def test_daily_boundaries_allow_normalized_midnight_without_carrying_hour(
    tmp_path, zone, first, last, hours
):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    data = workflow_uptime(
        store.path,
        dag_ids=["demo"],
        start_at=datetime.fromisoformat(first),
        end_at=datetime.fromisoformat(last),
        granularity="day",
        timezone_name=zone,
    )
    assert [
        (datetime.fromisoformat(slot["end_at"]) - datetime.fromisoformat(slot["start_at"])).total_seconds() / 3600
        for slot in data["workflows"]["demo"]
    ] == hours


def test_uptime_excludes_finished_history_before_python_time_conversion(tmp_path, monkeypatch):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    old_start, old_end = "2020-01-01T00:00:00+00:00", "2020-01-01T00:01:00+00:00"
    with sqlite3.connect(store.path) as connection:
        for index in range(200):
            for kind in ["task", "legacy"]:
                run_id = f"{kind}-{index}"
                add_run(connection, run_id, start=old_start, end=old_end, state="failed")
                if kind == "task":
                    add_try(connection, run_id, start=old_start, end=old_end, state="failed")
        # 开始于查询前的跨日执行必须保留，包括没有 ended_at 的旧尝试。
        for run_id, ended in [("cross", "2026-01-02T00:00:01+00:00"), ("open-ended", None)]:
            add_run(connection, run_id, start=old_start, end="2026-01-02T00:00:01+00:00")
            add_try(connection, run_id, start=old_start, end=ended)
        # 瞬时事件位于左边界；偏移时区和微秒不能被 SQLite 粗筛误删。
        for run_id, at in [
            ("instant", START.isoformat()),
            ("offset", "2026-01-02T08:00:00+08:00"),
            ("microsecond", "2026-01-02T00:29:59.999999+00:00"),
            ("python-iso", "2026-01-02T08:00:00+08:00:00"),
        ]:
            add_run(connection, run_id)
            add_try(connection, run_id, start=at, end=at, state="failed")

    converted = []
    original = uptime._timestamp
    plans = []
    original_connect = sqlite3.connect

    class RangeConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if "WITH intervals" in sql or "WITH errors" in sql:
                plans.extend(
                    row[3] for row in super().execute("EXPLAIN QUERY PLAN " + sql, parameters)
                )
            return super().execute(sql, parameters)

    def connect(*args, **kwargs):
        return original_connect(*args, **kwargs, factory=RangeConnection)

    def timestamp(value):
        converted.append(value)
        return original(value)

    monkeypatch.setattr(uptime, "_timestamp", timestamp)
    monkeypatch.setattr(uptime.sqlite3, "connect", connect)
    params = {"start_at": START, "end_at": START + timedelta(days=1), "now": START + timedelta(days=2)}
    slots = workflow_uptime(store.path, dag_ids=["demo"], **params)["workflows"]["demo"]
    assert slots[0]["run_count"] == 6
    assert slots[0]["failed_run_count"] == 4
    assert slots[1]["run_count"] == 0
    assert old_end not in converted
    assert "SCAN t" not in plans and "SCAN r" not in plans
    for index in ["ix_task_try_uptime_end", "ix_dag_run_uptime_end"]:
        assert any(index in detail and "<expr>>?" in detail for detail in plans)
    converted.clear()
    failures = workflow_failures(store.path, dag_id="demo", **params)
    assert failures["total"] == 4
    assert old_end not in converted
    # 索引须能在已有记录上重复建立，不依赖新数据库或清空历史。
    store.init_db()
    store.init_db()
