from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
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
