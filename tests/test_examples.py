from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace

import pytest

from easyflow._internal.dag_parser import discover_dag_sources, load_dag_from_file
from easyflow.sdk.context import set_current_context
from easyflow.sdk.states import TaskInstanceState


EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
LOGICAL_DATE = datetime(2026, 7, 3, tzinfo=timezone.utc)
CASES = [
    ("001_basic_manual.py", "example_manual_hello", "say_hello", "Hello, workflow!"),
    (
        "002_basic_taskflow_etl.py",
        "example_taskflow_etl",
        "load",
        {"count": 3, "total": 55},
    ),
    (
        "101_schedule_daily.py",
        "example_daily_schedule",
        "show_logical_date",
        LOGICAL_DATE.isoformat(),
    ),
    ("102_schedule_weekday.py", "example_weekday_schedule", "prepare_report", "生成工作日报表"),
    ("103_schedule_once.py", "example_run_once", "initialize", "一次性初始化完成"),
    ("201_dependency_fanout.py", "example_fanout_fanin", "east", "east"),
    ("202_dependency_trigger_rule.py", "example_trigger_rule", "after_completion", None),
    ("301_operator_bash.py", "example_bash_operator", "report", "ready"),
    ("302_operator_python.py", "example_python_operator", "report", 42),
    (
        "401_runtime_params.py",
        "example_runtime_params",
        "summarize",
        {"symbol": "BTC", "quantity": 3},
    ),
    ("402_runtime_retry.py", "example_retry", "call_service", "模拟服务调用成功"),
]


def test_example_names_and_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EASYFLOW_EXAMPLE_ENGINE", raising=False)
    groups = {
        "0": "basic",
        "1": "schedule",
        "2": "dependency",
        "3": "operator",
        "4": "runtime",
        "5": "quant",
    }
    files = sorted(path for path in EXAMPLES.glob("*.py") if path.name != "__init__.py")
    for path in files:
        match = re.fullmatch(r"([0-9])([0-9]{2})_([a-z]+)_[a-z0-9_]+\.py", path.name)
        assert match is not None, path.name
        assert int(match.group(2)) > 0
        assert groups[match.group(1)] == match.group(3)

    result = discover_dag_sources(EXAMPLES)
    assert result.issues == []
    assert {source.dag_id for source in result.sources} == {
        *(case[1] for case in CASES),
        "quant_daily_prices",
    }
    assert len(result.sources) == len(files)


@pytest.mark.parametrize(("filename", "dag_id", "task_id", "expected"), CASES)
def test_example_runs(
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    dag_id: str,
    task_id: str,
    expected: object,
) -> None:
    monkeypatch.delenv("EASYFLOW_EXAMPLE_ENGINE", raising=False)
    if filename == "301_operator_bash.py" and not (
        Path("/bin/bash").is_file() or shutil.which("bash")
    ):
        pytest.skip("BashOperator 示例需要 Bash")
    dag = load_dag_from_file(EXAMPLES / filename, dag_id)
    result = dag.test(LOGICAL_DATE, run_conf={"symbol": "BTC", "quantity": 3})
    assert result.success
    assert result.task_results[task_id] == expected
    if dag_id == "example_trigger_rule":
        assert result.task_states == {
            "skip": TaskInstanceState.SKIPPED,
            "only_on_success": TaskInstanceState.SKIPPED,
            "after_completion": TaskInstanceState.SUCCESS,
        }
    if dag_id == "example_fanout_fanin":
        assert dag.get_task("finish").upstream_task_ids == {"east", "west"}


def test_trigger_rule_example_does_not_require_bash(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("EASYFLOW_EXAMPLE_ENGINE", raising=False)
    dag = load_dag_from_file(EXAMPLES / "202_dependency_trigger_rule.py", "example_trigger_rule")
    real_isfile = os.path.isfile
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setattr(
        os.path, "isfile", lambda path: False if path == "/bin/bash" else real_isfile(path)
    )
    result = dag.test(LOGICAL_DATE)
    assert result.task_states == {
        "skip": TaskInstanceState.SKIPPED,
        "only_on_success": TaskInstanceState.SKIPPED,
        "after_completion": TaskInstanceState.SUCCESS,
    }


def test_weekday_example_uses_packaged_time_zone_data() -> None:
    env = os.environ.copy()
    env["PYTHONTZPATH"] = ""
    env["EASYFLOW_EXAMPLE_ENGINE"] = "easyflow"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import runpy, sys, zoneinfo; "
            "sys.path.insert(0, sys.argv[1]); "
            "assert not zoneinfo.TZPATH; "
            "dag = runpy.run_path(sys.argv[2])['dag']; "
            "assert dag.start_date.tzinfo.key == 'Asia/Shanghai'",
            str(EXAMPLES.parent / "src"),
            str(EXAMPLES / "102_schedule_weekday.py"),
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_retry_example_retries_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EASYFLOW_EXAMPLE_ENGINE", raising=False)
    dag = load_dag_from_file(EXAMPLES / "402_runtime_retry.py", "example_retry")
    result = dag.test(LOGICAL_DATE, run_conf={"fail": True})
    assert result.task_states["call_service"] == TaskInstanceState.FAILED
    assert result.try_numbers["call_service"] == 3


def test_daily_example_without_logical_date(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EASYFLOW_EXAMPLE_ENGINE", raising=False)
    dag = load_dag_from_file(EXAMPLES / "101_schedule_daily.py", "example_daily_schedule")
    show_logical_date = dag.get_task("show_logical_date").python_callable
    with set_current_context({}):
        assert show_logical_date() == "未提供"


def test_quant_example_explicit_date_and_no_logical_date(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("EASYFLOW_EXAMPLE_ENGINE", raising=False)
    dag = load_dag_from_file(EXAMPLES / "501_quant_daily_prices.py", "quant_daily_prices")
    run_conf = {"symbol": "BTC", "data_root": str(tmp_path), "trade_date": "2026-07-02"}
    result = dag.test(datetime(2026, 11, 8, 8, 30, tzinfo=timezone.utc), run_conf=run_conf)
    assert result.success
    output = tmp_path / "symbol=BTC" / "date=2026-07-02" / "data.json"
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["date"] == "2026-07-02"
    assert payload["data_interval_start"] == "2026-07-02T00:00:00+00:00"
    assert payload["data_interval_end"] == "2026-07-03T00:00:00+00:00"

    fetch = dag.get_task("fetch_daily_prices").python_callable
    context = {"params": {"symbol": "BTC", "data_root": str(tmp_path)}}
    with set_current_context({**context, "dag_run": SimpleNamespace(conf={})}):
        with pytest.raises(ValueError, match="trade_date"):
            fetch()
    with set_current_context({**context, "dag_run": SimpleNamespace(conf=run_conf)}):
        assert fetch()["path"] == str(output)


def test_quant_example_concurrent_writes_to_same_partition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("EASYFLOW_EXAMPLE_ENGINE", raising=False)
    dag = load_dag_from_file(EXAMPLES / "501_quant_daily_prices.py", "quant_daily_prices")
    fetch = dag.get_task("fetch_daily_prices").python_callable
    barrier = Barrier(2)
    real_replace = Path.replace

    def synchronized_replace(path: Path, target: Path) -> Path:
        barrier.wait(timeout=5)
        return real_replace(path, target)

    monkeypatch.setattr(Path, "replace", synchronized_replace)

    def run(logical_date: datetime) -> dict[str, object]:
        context = {
            "params": {"symbol": "BTC", "data_root": str(tmp_path)},
            "dag_run": SimpleNamespace(conf={"trade_date": "2026-07-02"}),
            "logical_date": logical_date,
        }
        with set_current_context(context):
            return fetch()

    with ThreadPoolExecutor(max_workers=2) as pool:
        outputs = list(
            pool.map(
                run,
                [
                    datetime(2026, 11, 8, 8, 30, tzinfo=timezone.utc),
                    datetime(2026, 11, 8, 8, 31, tzinfo=timezone.utc),
                ],
            )
        )
    partition = tmp_path / "symbol=BTC" / "date=2026-07-02"
    output = partition / "data.json"
    assert [item["path"] for item in outputs] == [str(output), str(output)]
    assert json.loads(output.read_text(encoding="utf-8"))["date"] == "2026-07-02"
    assert list(partition.iterdir()) == [output]


def test_quant_example_failed_replace_keeps_existing_data(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("EASYFLOW_EXAMPLE_ENGINE", raising=False)
    dag = load_dag_from_file(EXAMPLES / "501_quant_daily_prices.py", "quant_daily_prices")
    fetch = dag.get_task("fetch_daily_prices").python_callable
    partition = tmp_path / "symbol=BTC" / "date=2026-07-02"
    partition.mkdir(parents=True)
    output = partition / "data.json"
    output.write_text("用户已有数据", encoding="utf-8")

    def fail_replace(path: Path, target: Path) -> Path:
        raise OSError("模拟替换失败")

    monkeypatch.setattr(Path, "replace", fail_replace)
    context = {
        "params": {"symbol": "BTC", "data_root": str(tmp_path)},
        "dag_run": SimpleNamespace(conf={"trade_date": "2026-07-02"}),
    }
    with set_current_context(context), pytest.raises(OSError, match="模拟替换失败"):
        fetch()
    assert output.read_text(encoding="utf-8") == "用户已有数据"
    assert list(partition.iterdir()) == [output]
