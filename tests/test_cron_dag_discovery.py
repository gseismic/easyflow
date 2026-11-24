from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from easyflow._internal.cron import validate_cron
from easyflow._internal.dag_parser import (
    DagSource,
    definition_matches,
    discover_dag_sources,
    inspect_dags_from_file,
    load_dag_snapshot,
    loaded_definition_hash,
)
from easyflow._internal.schedules import backfill_intervals, scheduler_intervals
from easyflow.sdk import DAG
from easyflow.cli import main


def test_snapshot_serialization_error_does_not_hide_sibling_dags(tmp_path):
    path = tmp_path / "two_dags.py"
    path.write_text(
        """from pathlib import Path
from easyflow.sdk import DAG
broken = DAG('broken', description=Path('not-json'))
valid = DAG('valid', description='可用 DAG')
""",
        encoding="utf-8",
    )
    result = inspect_dags_from_file(path)
    assert [(source.dag_id, source.dag.description) for source in result.sources] == [
        ("valid", "可用 DAG")
    ]
    assert len(result.issues) == 1 and result.issues[0].dag_id == "broken"
    assert "JSON serializable" in result.issues[0].message


def test_definition_fingerprint_tracks_each_symlink_even_with_identical_sources(tmp_path):
    left, right = tmp_path / "left.data", tmp_path / "right.data"
    for target in left, right:
        target.write_text("VALUE = 1\n", encoding="utf-8")
    first = tmp_path / "alias_a.py"
    second = tmp_path / "alias_b.py"
    first.symlink_to(left)
    second.symlink_to(right)
    path = tmp_path / "links.py"
    path.write_text(
        "from alias_a import VALUE\nfrom alias_b import VALUE as SECOND\n"
        "from easyflow.sdk import DAG\ndag = DAG('links')\n",
        encoding="utf-8",
    )
    before = DagSource("links", path, load_dag_snapshot(path, "links"))
    expected = loaded_definition_hash(before)
    assert definition_matches(before, expected)
    first.unlink()
    second.unlink()
    first.symlink_to(right)
    second.symlink_to(left)
    after = DagSource("links", path, load_dag_snapshot(path, "links"))
    assert before.dag._easyflow_dependency_hashes == after.dag._easyflow_dependency_hashes
    assert loaded_definition_hash(after) != expected
    assert not definition_matches(before, expected)


def test_snapshot_rejects_two_imports_of_different_bytes_from_one_target(tmp_path):
    target = tmp_path / "shared.py"
    target.write_text("VALUE = 'old'\n", encoding="utf-8")
    for name in ("alias_a.py", "alias_b.py"):
        (tmp_path / name).symlink_to(target)
    path = tmp_path / "changing.py"
    path.write_text(
        f"""from pathlib import Path
from alias_a import VALUE
Path({str(target)!r}).write_text("VALUE = 'new'\\n", encoding='utf-8')
from alias_b import VALUE as SECOND
from easyflow.sdk import DAG
dag = DAG('changing')
""",
        encoding="utf-8",
    )
    result = inspect_dags_from_file(path)
    assert not result.sources
    assert len(result.issues) == 1 and result.issues[0].dag_id == "changing"
    assert "解析期间本地依赖已改变" in result.issues[0].message
    refreshed = DagSource("changing", path, load_dag_snapshot(path, "changing"))
    assert definition_matches(refreshed, loaded_definition_hash(refreshed))


def test_cron_latest_interval_skips_weekend() -> None:
    dag = DAG(
        "cron",
        schedule="30 9 * * 1-5",
        start_date=datetime(2026, 7, 1, tzinfo=timezone.utc),
    )
    interval = scheduler_intervals(
        dag, datetime(2026, 7, 6, 10, 0, tzinfo=timezone.utc)
    )[0]

    assert interval is not None
    assert interval.logical_date == datetime(2026, 7, 6, 9, 30, tzinfo=timezone.utc)
    assert interval.data_interval_start == datetime(
        2026, 7, 6, 9, 30, tzinfo=timezone.utc
    )
    assert interval.data_interval_end == datetime(
        2026, 7, 6, 9, 30, tzinfo=timezone.utc
    )


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
    assert intervals[0].data_interval_end == datetime(
        2026, 7, 3, 9, 30, tzinfo=timezone.utc
    )


def test_croniter_extended_last_day_expression_is_supported() -> None:
    intervals = backfill_intervals(
        "0 0 L * *",
        date(2026, 7, 1),
        date(2026, 7, 31),
    )

    assert [interval.logical_date for interval in intervals] == [
        datetime(2026, 7, 31, 0, 0, tzinfo=timezone.utc)
    ]
    assert intervals[0].data_interval_end == datetime(
        2026, 7, 31, 0, 0, tzinfo=timezone.utc
    )


def test_cron_rejects_second_level_expression_until_scheduler_supports_seconds() -> (
    None
):
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
from datetime import datetime, timezone
from pathlib import Path
from easyflow.sdk import DAG, get_current_context, task

with DAG("daily_dir", schedule="@daily", start_date=datetime(2026, 7, 1, tzinfo=timezone.utc)) as dag:
    @task
    def write_daily():
        Path({str(daily_output)!r}).write_text(get_current_context()["ds"])

    write_daily()
""",
        encoding="utf-8",
    )
    (dags_folder / "cron.py").write_text(
        f"""
from datetime import datetime, timezone
from pathlib import Path
from easyflow.sdk import DAG, get_current_context, task

with DAG("cron_dir", schedule="0 0 * * *", start_date=datetime(2026, 7, 1, tzinfo=timezone.utc)) as dag:
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
    assert "cron_dir scheduled__2026-07-08T00:00:00+00:00 success" in scheduler_out
    assert "daily_dir scheduled__2026-07-08T00:00:00+00:00 success" in scheduler_out
    assert cron_output.read_text() == "2026-07-08"
    assert daily_output.read_text() == "2026-07-08"


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
    assert (
        "folder_job manual__2026-07-06T00:00:00+00:00 success"
        in capsys.readouterr().out
    )

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
