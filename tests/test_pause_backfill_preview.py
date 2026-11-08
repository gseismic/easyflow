from __future__ import annotations

import threading
import time
from datetime import date, datetime, timezone

from easyflow._internal.dag_parser import load_dag_snapshot
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.scheduler import preview_backfill, run_scheduler_once
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.cli import main


def test_pause_finishes_running_task_and_resumes_pending_task(tmp_path):
    started, release, finished = (
        tmp_path / name for name in ("started", "release", "finished")
    )
    path = tmp_path / "dag.py"
    path.write_text(
        f"""from easyflow.sdk import DAG, task
with DAG("pause", max_active_runs=2) as dag:
    @task
    def first():
        import time
        from pathlib import Path
        Path({str(started)!r}).touch()
        while not Path({str(release)!r}).exists(): time.sleep(0.02)
    @task
    def second():
        from pathlib import Path
        Path({str(finished)!r}).touch()
    first() >> second()
""",
        encoding="utf-8",
    )
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.sqlite'}")
    dag = load_dag_snapshot(path, "pause")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run = runner.create_manual_run(
        dag, file_path=path, logical_date=datetime.now(timezone.utc)
    )
    results = []
    thread = threading.Thread(
        target=lambda: results.append(
            runner.run_existing(dag, run_id=run, file_path=path)
        )
    )
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while not started.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert started.exists()
        store.set_dag_paused("pause", True)
        release.touch()
        thread.join(10)
        assert not thread.is_alive()
        assert results[0].state.value == "running"
        assert not finished.exists()
        assert SQLiteStateStore(store.database_url).is_dag_paused("pause")
        assert (
            run_scheduler_once(
                dag_id="pause", file_path=path, store=store, log_root=tmp_path / "logs"
            )
            == []
        )
        store.set_dag_paused("pause", False)
        assert (
            runner.run_existing(dag, run_id=run, file_path=path).state.value
            == "success"
        )
        assert finished.exists()
    finally:
        release.touch()
        thread.join(10)


def test_backfill_preview_has_no_runs_or_task_side_effects(tmp_path, capsys):
    path = tmp_path / "dag.py"
    path.write_text(
        'from easyflow.sdk import DAG\ndag = DAG("daily", schedule="@daily")\n',
        encoding="utf-8",
    )
    url = f"sqlite:///{tmp_path / 'state.sqlite'}"
    store = SQLiteStateStore(url)
    dag = load_dag_snapshot(path, "daily")
    PersistentDagRunner(store).create_manual_run(
        dag,
        file_path=path,
        logical_date=datetime(2026, 7, 1, tzinfo=timezone.utc),
        run_id="existing",
    )
    preview = preview_backfill(
        dag=dag, store=store, start_date=date(2026, 7, 1), end_date=date(2026, 7, 2)
    )
    assert [
        (item.latest_state.value if item.latest_state else None, item.eligible)
        for item in preview
    ] == [("queued", False), (None, True)]
    assert (
        main(
            [
                "backfill",
                "create",
                "--dag-id",
                "daily",
                "--file",
                str(path),
                "--start-date",
                "2026-07-01",
                "--end-date",
                "2026-07-02",
                "--database-url",
                url,
                "--dry-run",
            ]
        )
        == 0
    )
    output = capsys.readouterr().out
    assert "skip" in output and "create" in output
    assert store.count_dag_runs(dag_id="daily", run_type="backfill") == 0
