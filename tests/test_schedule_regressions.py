from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from easyflow._internal.cron import cron_next_after, cron_previous_before
from easyflow._internal.dag_parser import load_dag_from_file
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.scheduler import create_backfill, run_scheduler_once
from easyflow._internal.schedules import backfill_intervals, scheduler_intervals
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.sdk import DAG

UTC = timezone.utc


def test_timedelta_uses_stable_anchor_and_start_end_constraints():
    start = datetime(2026, 7, 1, 10, 10, tzinfo=UTC)
    dag = DAG("delta", schedule=timedelta(hours=1), start_date=start)
    first = scheduler_intervals(dag, start + timedelta(hours=2, minutes=15))
    second = scheduler_intervals(dag, start + timedelta(hours=2, minutes=15, seconds=5))
    assert first == second
    assert first[0].logical_date == start + timedelta(hours=1)
    assert scheduler_intervals(dag, start - timedelta(minutes=1)) == []
    dag.catchup = True
    dag.end_date = start + timedelta(hours=1, minutes=30)
    assert [
        i.logical_date for i in scheduler_intervals(dag, start + timedelta(hours=8))
    ] == [start, start + timedelta(hours=1)]
    unanchored = DAG("unanchored", schedule=timedelta(hours=7))
    point = datetime(2026, 7, 8, 23, tzinfo=UTC)
    scheduled = scheduler_intervals(unanchored, point)[0]
    backfilled = backfill_intervals(
        unanchored.schedule,
        scheduled.logical_date.date(),
        scheduled.logical_date.date(),
    )
    assert scheduled in backfilled
    assert scheduler_intervals(unanchored, datetime(1969, 12, 31, tzinfo=UTC))


def test_boundaries_are_not_floored_before_start_and_stop_at_end():
    start = datetime(2026, 7, 1, 12, tzinfo=UTC)
    dag = DAG(
        "bounded",
        schedule="@daily",
        start_date=start,
        end_date=datetime(2026, 7, 3, tzinfo=UTC),
        catchup=True,
    )
    assert scheduler_intervals(dag, datetime(2026, 7, 2, 12, tzinfo=UTC)) == []
    result = scheduler_intervals(dag, datetime(2026, 7, 10, tzinfo=UTC))
    assert [i.logical_date for i in result] == [
        datetime(2026, 7, 2, tzinfo=UTC),
        datetime(2026, 7, 3, tzinfo=UTC),
    ]
    dag.catchup = False
    assert scheduler_intervals(dag, datetime(2026, 7, 10, tzinfo=UTC)) == result[-1:]


def test_once_is_deduplicated_and_backfill_terminates(tmp_path):
    path = tmp_path / "once.py"
    path.write_text(
        'from easyflow.sdk import DAG\ndag = DAG("once", schedule="@once")\n'
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    now = datetime(2026, 7, 8, tzinfo=UTC)
    assert (
        len(run_scheduler_once(dag_id="once", file_path=path, store=store, now=now))
        == 1
    )
    assert (
        run_scheduler_once(
            dag_id="once", file_path=path, store=store, now=now + timedelta(seconds=5)
        )
        == []
    )
    assert len(backfill_intervals("@once", date(2026, 1, 1), date(2026, 12, 31))) == 1
    backfill_store = SQLiteStateStore(f"sqlite:///{tmp_path / 'backfill.db'}")
    backfill = create_backfill(
        dag_id="once",
        file_path=path,
        store=backfill_store,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
    )
    assert backfill.complete and len(backfill.runs) == 1


def test_timedelta_scheduler_does_not_create_a_run_each_scan(tmp_path):
    path = tmp_path / "delta.py"
    path.write_text(
        "from datetime import timedelta\nfrom easyflow.sdk import DAG\n"
        'dag = DAG("delta", schedule=timedelta(hours=1))\n'
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    now = datetime(2026, 7, 8, 12, 12, tzinfo=UTC)
    assert (
        len(run_scheduler_once(dag_id="delta", file_path=path, store=store, now=now))
        == 1
    )
    assert (
        run_scheduler_once(
            dag_id="delta", file_path=path, store=store, now=now + timedelta(seconds=5)
        )
        == []
    )


def test_non_utc_scheduler_and_backfill_context(tmp_path):
    path = tmp_path / "tz.py"
    path.write_text("""
from datetime import datetime, timedelta, timezone
from easyflow.sdk import DAG, task, get_current_context
with DAG("tz", schedule="0 9 * * *", start_date=datetime(2026, 7, 1, 9, tzinfo=timezone(timedelta(hours=8)))) as dag:
    @task
    def context_values():
        c = get_current_context()
        return [c["ds"], c["data_interval_start"].isoformat(), c["data_interval_end"].isoformat()]
    context_values()
""")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    summaries = run_scheduler_once(
        dag_id="tz",
        file_path=path,
        store=store,
        now=datetime(2026, 7, 8, 2, tzinfo=UTC),
    )
    run_id = summaries[0].run_id
    assert store.pull_xcom("tz", run_id, "context_values", "return_value") == [
        "2026-07-07",
        "2026-07-07T09:00:00+08:00",
        "2026-07-08T09:00:00+08:00",
    ]
    backfill = create_backfill(
        dag_id="tz",
        file_path=path,
        store=store,
        start_date=date(2026, 7, 2),
        end_date=date(2026, 7, 2),
    )
    assert backfill.complete
    assert store.pull_xcom(
        "tz", backfill.runs[0].run_id, "context_values", "return_value"
    )[1:] == ["2026-07-02T09:00:00+08:00", "2026-07-03T09:00:00+08:00"]


def test_manual_context_preserves_timezone_and_saved_interval(tmp_path):
    path = tmp_path / "manual.py"
    path.write_text("""
from easyflow.sdk import DAG, task, get_current_context
with DAG("manual", schedule="@daily") as dag:
    @task
    def values():
        c = get_current_context()
        return [c["ds"], c["data_interval_end"].isoformat()]
    values()
""")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    dag = load_dag_from_file(path, "manual")
    local = datetime.fromisoformat("2026-07-06T00:00:00+08:00")
    assert dag.test(local).task_results["values"][0] == "2026-07-06"
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run = runner.create_manual_run(dag, file_path=path, logical_date=local)
    with sqlite3.connect(store.path) as db:
        db.execute(
            "UPDATE dag_run SET data_interval_end = ? WHERE run_id = ?",
            ("2026-07-08T00:00:00+00:00", run),
        )
    assert runner.run_existing(dag, run_id=run, file_path=path).state.value == "success"
    assert store.pull_xcom("manual", run, "values", "return_value") == [
        "2026-07-06",
        "2026-07-08T08:00:00+08:00",
    ]


def test_cron_skips_spring_gap_and_orders_both_fall_folds():
    zone = ZoneInfo("America/New_York")
    before_gap = datetime(2026, 3, 8, 1, 59, tzinfo=zone)
    after_gap = cron_next_after("* * * * *", before_gap)
    assert after_gap.isoformat() == "2026-03-08T03:00:00-04:00"
    before_fold = datetime(2026, 11, 1, 1, 59, tzinfo=zone, fold=0)
    after_fold = cron_next_after("* * * * *", before_fold)
    assert after_fold.isoformat() == "2026-11-01T01:00:00-05:00"
    assert after_fold.astimezone(UTC) > before_fold.astimezone(UTC)
    assert cron_previous_before("* * * * *", after_fold).astimezone(
        UTC
    ) == before_fold.astimezone(UTC)


def test_backfill_intervals_follow_local_day_across_dst():
    zone = ZoneInfo("America/New_York")
    intervals = backfill_intervals("@daily", date(2026, 3, 8), date(2026, 3, 8), zone)
    assert len(intervals) == 1
    interval = intervals[0]
    assert interval.data_interval_start.isoformat() == "2026-03-08T00:00:00-05:00"
    assert interval.data_interval_end.isoformat() == "2026-03-09T00:00:00-04:00"
    assert interval.data_interval_end.astimezone(
        UTC
    ) - interval.data_interval_start.astimezone(UTC) == timedelta(hours=23)


def test_local_executor_preserves_named_timezone_across_dst(tmp_path):
    path = tmp_path / "local_tz.py"
    path.write_text("""
from datetime import datetime
from zoneinfo import ZoneInfo
from easyflow.sdk import DAG, task, get_current_context
with DAG("local_tz", schedule="@daily", start_date=datetime(2026, 3, 1, tzinfo=ZoneInfo("America/New_York"))) as dag:
    @task
    def values():
        c = get_current_context()
        return [c["logical_date"].tzinfo.key, c["data_interval_end"].isoformat()]
    values()
""")
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'db.sqlite'}")
    dag = load_dag_from_file(path, "local_tz")
    logical = datetime(2026, 3, 8, tzinfo=ZoneInfo("America/New_York"))
    expected = dag.test(logical).task_results["values"]
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run = runner.create_manual_run(dag, file_path=path, logical_date=logical)
    assert (
        runner.run_existing(
            dag, run_id=run, file_path=path, executor="local"
        ).state.value
        == "success"
    )
    assert store.pull_xcom("local_tz", run, "values", "return_value") == expected
    assert expected == ["America/New_York", "2026-03-09T00:00:00-04:00"]
