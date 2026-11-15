import sqlite3
from datetime import UTC, datetime, timedelta
from itertools import pairwise

import pytest

from easyflow._internal.state_store import SQLiteStateStore
from easyflow.web.uptime import workflow_uptime


def test_recent_window_includes_last_night_and_clips_exact_edges(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    now = datetime.fromisoformat("2026-11-16T00:12:34.123456+08:00")
    left = now.astimezone(UTC) - timedelta(hours=24)
    with sqlite3.connect(store.path) as connection:
        for name, when in [
            ("before", left - timedelta(microseconds=1)),
            ("left", left),
            ("last-night", now - timedelta(hours=1)),
            ("right", now),
            ("future", now + timedelta(seconds=1)),
        ]:
            connection.execute(
                "INSERT INTO dag_run (dag_id, run_id, run_type, logical_date, data_interval_start, data_interval_end, state, queued_at, end_date) VALUES ('demo', ?, 'manual', ?, ?, ?, 'failed', ?, ?)",
                (
                    name,
                    when.isoformat(),
                    when.isoformat(),
                    when.isoformat(),
                    when.isoformat(),
                    when.isoformat(),
                ),
            )
        connection.execute(
            "INSERT INTO dag_run (dag_id, run_id, run_type, logical_date, data_interval_start, data_interval_end, state, queued_at) VALUES ('demo', 'queued', 'manual', ?, ?, ?, 'queued', ?)",
            (left.isoformat(), left.isoformat(), left.isoformat(), left.isoformat()),
        )
    data = workflow_uptime(store.path, dag_ids=["demo"], lookback_hours=24, now=now)
    slots = data["workflows"]["demo"]
    assert data["start_at"] == left.isoformat()
    assert data["end_at"] == now.astimezone(UTC).isoformat() == data["as_of"]
    assert len(slots) == 49
    assert slots[0]["start_at"] == data["start_at"]
    assert slots[-1]["end_at"] == data["end_at"]
    assert slots[-1]["state"] == "waiting" and slots[-1]["waiting_run_count"] == 1
    assert sum(slot["failed_run_count"] for slot in slots) == 2
    assert {slot["sample_run_id"] for slot in slots if slot["failed_run_count"]} == {
        "left",
        "last-night",
    }
    for previous, following in pairwise(slots):
        assert previous["end_at"] == following["start_at"]
    assert all(
        0
        < (
            datetime.fromisoformat(slot["end_at"])
            - datetime.fromisoformat(slot["start_at"])
        ).total_seconds()
        <= 1800
        for slot in slots
    )
    later = workflow_uptime(
        store.path, dag_ids=["demo"], lookback_hours=24, now=now + timedelta(seconds=5)
    )
    assert [slot["start_at"] for slot in slots[1:]] == [
        slot["start_at"] for slot in later["workflows"]["demo"][1:]
    ]


@pytest.mark.parametrize("hours", [1, 24, 168])
@pytest.mark.parametrize(
    "instant",
    [
        "2026-11-16T00:00:00+08:00",
        "2026-11-16T00:00:00.000001+08:00",
        "2026-03-08T03:15:00-04:00",
        "2026-11-01T01:15:00-05:00",
    ],
)
def test_recent_window_is_elapsed_hours_even_across_dst(tmp_path, hours, instant):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.init_db()
    data = workflow_uptime(
        store.path,
        dag_ids=["demo"],
        lookback_hours=hours,
        now=datetime.fromisoformat(instant),
    )
    assert datetime.fromisoformat(data["end_at"]) - datetime.fromisoformat(
        data["start_at"]
    ) == timedelta(hours=hours)
    slots = data["workflows"]["demo"]
    assert 2 * hours <= len(slots) <= 2 * hours + 1
    assert all(slot["state"] == "idle" for slot in slots)
    assert all(
        0
        < (
            datetime.fromisoformat(slot["end_at"])
            - datetime.fromisoformat(slot["start_at"])
        ).total_seconds()
        <= 1800
        for slot in slots
    )


@pytest.mark.parametrize("hours", [0, -1, 169, 1.5, True, "24"])
def test_recent_hours_reject_invalid_values(tmp_path, hours):
    with pytest.raises(ValueError, match="1 到 168"):
        workflow_uptime(tmp_path / "unused.db", dag_ids=["demo"], lookback_hours=hours)


def test_recent_hours_cannot_mix_with_absolute_or_daily_ranges(tmp_path):
    now = datetime(2026, 11, 16, tzinfo=UTC)
    for patch in [{"start_at": now}, {"end_at": now}, {"granularity": "day"}]:
        with pytest.raises(ValueError, match="混用"):
            workflow_uptime(
                tmp_path / "unused.db", dag_ids=["demo"], lookback_hours=24, **patch
            )
