from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from easyflow._internal.cron import validate_cron
from easyflow._internal.dag_parser import discover_dag_sources
from easyflow._internal.schedules import backfill_intervals, latest_ended_interval
from easyflow.cli import main


def test_cron_latest_interval_skips_weekend() -> None:
    interval = latest_ended_interval(
        "30 9 * * 1-5",
        datetime(2026, 7, 6, 10, 0, tzinfo=timezone.utc),
    )

    assert interval is not None
    assert interval.logical_date == datetime(2026, 7, 3, 9, 30, tzinfo=timezone.utc)
    assert interval.data_interval_start == datetime(2026, 7, 3, 9, 30, tzinfo=timezone.utc)
    assert interval.data_interval_end == datetime(2026, 7, 6, 9, 30, tzinfo=timezone.utc)


def test_cron_backfill_includes_end_date_and_skips_weekend() -> None:
    intervals = backfill_intervals(
        "30 9 * * 1-5",
        date(2026, 7, 3),
        date(2026, 7, 6),
    )

    assert [interval.logical_date for interval in intervals] == [
        datetime(2026, 7, 3, 9, 30, tzinfo=timezone.utc),
        datetime(2026, 7, 6, 9, 30, tzinfo=timezone.utc),
    ]
    assert intervals[0].data_interval_end == datetime(2026, 7, 6, 9, 30, tzinfo=timezone.utc)


def test_croniter_extended_last_day_expression_is_supported() -> None:
    intervals = backfill_intervals(
        "0 0 L * *",
        date(2026, 7, 1),
        date(2026, 7, 31),
    )

    assert [interval.logical_date for interval in intervals] == [
        datetime(2026, 7, 31, 0, 0, tzinfo=timezone.utc)
    ]
    assert intervals[0].data_interval_end == datetime(2026, 8, 31, 0, 0, tzinfo=timezone.utc)


def test_cron_rejects_second_level_expression_until_scheduler_supports_seconds() -> None:
    with pytest.raises(ValueError, match="5 字段"):
        validate_cron("*/15 * * * * *")


def test_discover_dag_sources_keeps_good_dags_and_reports_bad_files(tmp_path) -> None:
    dags_folder = tmp_path / "dags"
    dags_folder.mkdir()
    (dags_folder / "good_a.py").write_text(
        """
from easyflow.sdk import DAG

dag = DAG("good_a", schedule="@daily")
""",
        encoding="utf-8",
    )
    nested = dags_folder / "nested"
    nested.mkdir()
    (nested / "good_b.py").write_text(
        """
from easyflow.sdk import DAG

dag = DAG("good_b", schedule="0 0 * * *")
""",
        encoding="utf-8",
    )
    (dags_folder / "bad_import.py").write_text(
        """
raise RuntimeError("broken import")
""",
        encoding="utf-8",
    )
    (dags_folder / "dup_1.py").write_text(
        """
from easyflow.sdk import DAG

dag = DAG("dup", schedule="@daily")
""",
        encoding="utf-8",
    )
    (dags_folder / "dup_2.py").write_text(
        """
from easyflow.sdk import DAG

dag = DAG("dup", schedule="@hourly")
""",
        encoding="utf-8",
    )

    result = discover_dag_sources(dags_folder)

    assert [source.dag_id for source in result.sources] == ["good_a", "good_b"]
    assert any("broken import" in issue.message for issue in result.issues)
    assert sum(1 for issue in result.issues if issue.dag_id == "dup") == 2


def test_dags_list_and_scheduler_scan_dags_folder(tmp_path, capsys) -> None:
    dags_folder = tmp_path / "dags"
    dags_folder.mkdir()
    daily_output = tmp_path / "daily.txt"
    cron_output = tmp_path / "cron.txt"
    (dags_folder / "daily.py").write_text(
        f"""
from pathlib import Path
from easyflow.sdk import DAG, get_current_context, task

with DAG("daily_dir", schedule="@daily") as dag:
    @task
    def write_daily():
        Path({str(daily_output)!r}).write_text(get_current_context()["ds"])

    write_daily()
""",
        encoding="utf-8",
    )
    (dags_folder / "cron.py").write_text(
        f"""
from pathlib import Path
from easyflow.sdk import DAG, get_current_context, task

with DAG("cron_dir", schedule="0 0 * * *") as dag:
    @task
    def write_cron():
        Path({str(cron_output)!r}).write_text(get_current_context()["ds"])

    write_cron()
""",
        encoding="utf-8",
    )
    db_url = f"sqlite:///{tmp_path / 'easyflow.db'}"

    assert main(["dags", "list", "--dags-folder", str(dags_folder)]) == 0
    list_out = capsys.readouterr().out
    assert "cron_dir" in list_out
    assert "daily_dir" in list_out

    assert (
        main(
            [
                "scheduler",
                "--once",
                "--dags-folder",
                str(dags_folder),
                "--now",
                "2026-07-08T12:00:00+00:00",
                "--database-url",
                db_url,
                "--log-root",
                str(tmp_path / "logs"),
            ]
        )
        == 0
    )
    scheduler_out = capsys.readouterr().out
    assert "cron_dir scheduled__2026-07-07T00:00:00+00:00 success" in scheduler_out
    assert "daily_dir scheduled__2026-07-07T00:00:00+00:00 success" in scheduler_out
    assert cron_output.read_text() == "2026-07-07"
    assert daily_output.read_text() == "2026-07-07"


def test_trigger_and_backfill_can_resolve_dag_from_folder(tmp_path, capsys) -> None:
    dags_folder = tmp_path / "dags"
    dags_folder.mkdir()
    output = tmp_path / "dates.txt"
    (dags_folder / "job.py").write_text(
        f"""
from pathlib import Path
from easyflow.sdk import DAG, get_current_context, task

with DAG("folder_job", schedule="@daily") as dag:
    @task
    def append_date():
        context = get_current_context()
        path = Path({str(output)!r})
        old = path.read_text() if path.exists() else ""
        path.write_text(old + context["ds"] + "\\n")

    append_date()
""",
        encoding="utf-8",
    )
    db_url = f"sqlite:///{tmp_path / 'easyflow.db'}"
    log_root = tmp_path / "logs"

    assert (
        main(
            [
                "dags",
                "trigger",
                "folder_job",
                "--dags-folder",
                str(dags_folder),
                "--logical-date",
                "2026-07-06T00:00:00+00:00",
                "--database-url",
                db_url,
                "--log-root",
                str(log_root),
            ]
        )
        == 0
    )
    assert "folder_job manual__2026-07-06T00:00:00+00:00 success" in capsys.readouterr().out

    assert (
        main(
            [
                "backfill",
                "create",
                "--dag-id",
                "folder_job",
                "--dags-folder",
                str(dags_folder),
                "--start-date",
                "2026-07-07",
                "--end-date",
                "2026-07-07",
                "--backfill-id",
                "folder_bf",
                "--database-url",
                db_url,
                "--log-root",
                str(log_root),
            ]
        )
        == 0
    )
    assert "backfill_id=folder_bf" in capsys.readouterr().out
    assert output.read_text().splitlines() == ["2026-07-06", "2026-07-07"]
