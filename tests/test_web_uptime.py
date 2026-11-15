from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from easyflow._internal.state_store import SQLiteStateStore
from easyflow.web.app import create_app
from easyflow.web.uptime import workflow_uptime

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
