from __future__ import annotations

from datetime import datetime, timezone
import json

import pytest

from easyflow.sdk import (
    DAG,
    ContextNotAvailableError,
    DuplicateTaskIdError,
    TemplateRenderError,
    TriggerRule,
    XComSerializationError,
    dag,
    get_current_context,
    task,
)
from easyflow.providers.standard.operators.bash import BashOperator
from easyflow.providers.standard.operators.empty import EmptyOperator
from easyflow.providers.standard.operators.python import PythonOperator
from easyflow.sdk.states import TaskInstanceState


def test_taskflow_resolves_xcom_and_auto_dependency() -> None:
    calls: list[str] = []
    with DAG("taskflow_demo") as workflow:

        @task
        def extract() -> dict[str, int]:
            calls.append("extract")
            return {"value": 41}

        @task
        def transform(data: dict[str, int]) -> int:
            calls.append("transform")
            return data["value"] + 1

        result = transform(extract())

    assert calls == []
    assert result.operator.upstream_task_ids == {"extract"}
    outcome = workflow.test(datetime(2026, 7, 6, tzinfo=timezone.utc))
    assert outcome.success
    assert outcome.task_results["transform"] == 42
    assert calls == ["extract", "transform"]


def test_multiple_outputs_xcomarg_key_lookup() -> None:
    with DAG("multi_outputs") as workflow:

        @task(multiple_outputs=True)
        def extract() -> dict[str, int]:
            return {"rows": 10}

        @task
        def consume(rows: int) -> int:
            return rows + 1

        consume(extract()["rows"])

    outcome = workflow.test(datetime(2026, 7, 6, tzinfo=timezone.utc))
    assert outcome.task_results["consume"] == 11


def test_dependency_operators_and_chain_lists() -> None:
    with DAG("deps") as workflow:
        a = EmptyOperator(task_id="a")
        b = EmptyOperator(task_id="b")
        c = EmptyOperator(task_id="c")
        d = EmptyOperator(task_id="d")
        a >> [b, c] >> d

    assert b.upstream_task_ids == {"a"}
    assert c.upstream_task_ids == {"a"}
    assert d.upstream_task_ids == {"b", "c"}
    assert workflow.test(datetime(2026, 7, 6, tzinfo=timezone.utc)).success


def test_duplicate_explicit_task_id_fails() -> None:
    with pytest.raises(DuplicateTaskIdError):
        with DAG("dupe"):
            EmptyOperator(task_id="same")
            EmptyOperator(task_id="same")


def test_taskflow_repeated_call_gets_suffix() -> None:
    with DAG("suffix") as workflow:

        @task
        def work() -> int:
            return 1

        first = work()
        second = work()

    assert first.operator.task_id == "work"
    assert second.operator.task_id == "work__1"
    assert workflow.test(datetime(2026, 7, 6, tzinfo=timezone.utc)).success


def test_template_context_and_get_current_context() -> None:
    seen: dict[str, str] = {}

    def record() -> str:
        context = get_current_context()
        seen["run_id"] = context["run_id"]
        return context["ds"]

    with DAG("template", params={"symbol": "AAPL"}) as workflow:
        bash = BashOperator(
            task_id="bash",
            bash_command="printf '{{ ds }} {{ params.symbol }}'",
        )
        py = PythonOperator(task_id="py", python_callable=record)
        bash >> py

    outcome = workflow.test(datetime(2026, 7, 6, tzinfo=timezone.utc))
    assert outcome.task_results["bash"] == "2026-07-06 AAPL"
    assert outcome.task_results["py"] == "2026-07-06"
    assert seen["run_id"].startswith("test__")


def test_get_current_context_outside_task_fails() -> None:
    with pytest.raises(ContextNotAvailableError):
        get_current_context()


def test_template_missing_variable_fails_task() -> None:
    with DAG("bad_template") as workflow:
        BashOperator(task_id="bad", bash_command="{{ missing }}")

    outcome = workflow.test(datetime(2026, 7, 6, tzinfo=timezone.utc))
    assert outcome.task_states["bad"] == TaskInstanceState.FAILED
    assert isinstance(outcome.task_results["bad"], TemplateRenderError)


def test_xcom_rejects_non_json_value() -> None:
    with DAG("bad_xcom") as workflow:

        @task
        def bad() -> object:
            return object()

        bad()

    outcome = workflow.test(datetime(2026, 7, 6, tzinfo=timezone.utc))
    assert outcome.task_states["bad"] == TaskInstanceState.FAILED
    assert isinstance(outcome.task_results["bad"], XComSerializationError)


def test_retries_before_failure() -> None:
    calls = 0

    def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls < 2:
            raise RuntimeError("boom")
        return "ok"

    with DAG("retries") as workflow:
        PythonOperator(task_id="flaky", python_callable=flaky, retries=1)

    outcome = workflow.test(datetime(2026, 7, 6, tzinfo=timezone.utc))
    assert outcome.success
    assert calls == 2
    assert outcome.try_numbers["flaky"] == 2


def test_trigger_rule_marks_upstream_failed() -> None:
    def fail() -> None:
        raise RuntimeError("boom")

    with DAG("trigger") as workflow:
        a = PythonOperator(task_id="a", python_callable=fail)
        b = EmptyOperator(task_id="b")
        c = EmptyOperator(
            task_id="c",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        [a, b] >> c

    outcome = workflow.test(datetime(2026, 7, 6, tzinfo=timezone.utc))
    assert outcome.task_states["a"] == TaskInstanceState.FAILED
    assert outcome.task_states["c"] == TaskInstanceState.UPSTREAM_FAILED


def test_dag_decorator_builds_dag() -> None:
    @dag(dag_id="decorated")
    def build() -> None:
        EmptyOperator(task_id="a")

    workflow = build()
    assert workflow.dag_id == "decorated"
    assert workflow.test(datetime(2026, 7, 6, tzinfo=timezone.utc)).success


def test_quant_data_task_uses_data_interval_and_idempotent_partition(tmp_path) -> None:
    with DAG(
        "quant_partition",
        schedule="@daily",
        params={"data_root": str(tmp_path), "symbol": "AAPL"},
    ) as workflow:

        @task(retries=1)
        def fetch() -> dict[str, object]:
            context = get_current_context()
            partition = (
                tmp_path
                / f"symbol={context['params']['symbol']}"
                / f"date={context['ds']}"
            )
            partition.mkdir(parents=True, exist_ok=True)
            tmp_file = partition / "data.tmp"
            final_file = partition / "data.json"
            payload = {
                "symbol": context["params"]["symbol"],
                "start": context["data_interval_start"].isoformat(),
                "end": context["data_interval_end"].isoformat(),
                "rows": [{"close": 100.0}],
            }
            tmp_file.write_text(json.dumps(payload), encoding="utf-8")
            tmp_file.replace(final_file)
            return {"path": str(final_file), "row_count": 1}

        fetch()

    logical_date = datetime(2026, 7, 6, tzinfo=timezone.utc)
    first = workflow.test(logical_date)
    second = workflow.test(logical_date)
    assert first.success and second.success
    output = tmp_path / "symbol=AAPL" / "date=2026-07-06" / "data.json"
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["start"] == "2026-07-06T00:00:00+00:00"
    assert payload["end"] == "2026-07-06T00:00:00+00:00"
    assert first.task_results["fetch"]["path"] == second.task_results["fetch"]["path"]
