from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.dag_parser import discover_dag_sources, inspect_dags_from_file
from easyflow._internal.state_store import SQLiteStateStore
from easyflow.providers.standard.operators.bash import BashOperator
from easyflow.providers.standard.operators.empty import EmptyOperator
from easyflow.providers.standard.operators.python import PythonOperator
from easyflow.sdk import DAG, UnsupportedFeatureError, task
from easyflow.sdk.states import TriggerRule


NOW = datetime(2026, 7, 6, tzinfo=timezone.utc)


def test_operator_default_args_are_inherited_and_explicit_values_win() -> None:
    defaults = {
        "depends_on_past": True,
        "trigger_rule": "all_done",
        "retries": 2,
        "retry_exponential_backoff": True,
        "params": {"nested": {"value": 1}},
    }
    with DAG("defaults", default_args=defaults):
        inherited = EmptyOperator(task_id="inherited")
        explicit = EmptyOperator(
            task_id="explicit", depends_on_past=False, retry_exponential_backoff=False
        )

    defaults["params"]["nested"]["value"] = 9
    inherited.params["nested"]["value"] = 3
    assert inherited.depends_on_past is True
    assert inherited.trigger_rule == TriggerRule.ALL_DONE
    assert inherited.retries == 2
    assert inherited.retry_exponential_backoff is True
    assert explicit.depends_on_past is False
    assert explicit.retry_exponential_backoff is False
    assert explicit.params["nested"]["value"] == 1


def test_unsupported_default_args_are_rejected() -> None:
    with pytest.raises(UnsupportedFeatureError, match="execution_timeout"):
        DAG("unsupported", default_args={"execution_timeout": timedelta(seconds=1)})
    with pytest.raises(TypeError, match="typo"):
        DAG("unknown", default_args={"typo": True})


def test_taskflow_and_operator_exchange_values_in_both_runners(tmp_path) -> None:
    with DAG("interop") as dag:

        @task
        def producer() -> int:
            return 7

        consumer = PythonOperator(
            task_id="a_consumer",
            python_callable=lambda value: value["nested"][0] + 1,
            op_kwargs={"value": {"nested": [producer()]}},
        )

        @task
        def result(value: int) -> int:
            return value * 2

        final = result(consumer.output)

    assert consumer.upstream_task_ids == {"producer"}
    assert final.operator.upstream_task_ids == {"a_consumer"}
    test_result = dag.test(NOW)
    assert test_result.success
    assert test_result.task_results["result"] == 16

    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.sqlite'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(dag, file_path=None, logical_date=NOW)
    assert runner.run_existing(dag, run_id=run_id).state.value == "success"
    assert store.pull_xcom("interop", run_id, "result", "return_value") == 16


def test_context_injection_and_task_instance_xcom_in_both_runners(tmp_path) -> None:
    with DAG("context", params={"symbol": "default"}) as dag:

        @task
        def source(*, ti, logical_date) -> str:
            ti.xcom_push(key="day", value=logical_date.date().isoformat())
            return ti.task_id

        @task
        def consumer(value: str, *, task_instance, dag_run, params, **kwargs) -> dict:
            return {
                "value": value,
                "day": task_instance.xcom_pull(task_ids="source", key="day"),
                "run_id": dag_run.run_id,
                "symbol": params["symbol"],
                "ds": kwargs["ds"],
            }

        consumer(source())

    result = dag.test(NOW, run_conf={"symbol": "override"})
    assert result.success
    assert result.task_results["consumer"] == {
        "value": "source",
        "day": "2026-07-06",
        "run_id": result.run_id,
        "symbol": "override",
        "ds": "2026-07-06",
    }

    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.sqlite'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(
        dag, file_path=None, logical_date=NOW, conf={"symbol": "override"}
    )
    assert runner.run_existing(dag, run_id=run_id).state.value == "success"
    value = store.pull_xcom("context", run_id, "consumer", "return_value")
    assert value["day"] == "2026-07-06"
    assert value["symbol"] == "override"
    assert value["run_id"] == run_id


def test_bash_output_and_skip_match_in_both_runners(tmp_path) -> None:
    with DAG("bash_compat") as dag:
        BashOperator(task_id="output", bash_command="printf 'first\\nsecond\\n'")
        BashOperator(task_id="skip", bash_command="exit 99", retries=3)
        BashOperator(task_id="failure", bash_command="exit 99", skip_on_exit_code=None)

    result = dag.test(NOW)
    assert result.task_results["output"] == "second"
    assert result.task_states["skip"].value == "skipped"
    assert result.try_numbers["skip"] == 1
    assert result.task_states["failure"].value == "failed"

    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.sqlite'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(dag, file_path=None, logical_date=NOW)
    persistent = runner.run_existing(dag, run_id=run_id)
    assert persistent.task_states["skip"].value == "skipped"
    assert persistent.task_states["failure"].value == "failed"
    assert store.pull_xcom("bash_compat", run_id, "output", "return_value") == "second"


def test_auto_registers_context_and_decorated_dags(tmp_path) -> None:
    (tmp_path / "dags.py").write_text(
        """from easyflow.sdk import DAG, dag, task
from easyflow.providers.standard.operators.empty import EmptyOperator

with DAG("context"):
    EmptyOperator(task_id="a")

@dag(dag_id="decorated")
def pipeline():
    @task
    def work(): return 1
    work()

pipeline()
with DAG("ignored", auto_register=False):
    EmptyOperator(task_id="b")
""",
        encoding="utf-8",
    )
    result = discover_dag_sources(tmp_path)
    assert [source.dag_id for source in result.sources] == ["context", "decorated"]
    assert result.issues == []


def test_parser_isolates_exit_and_timeout(tmp_path) -> None:
    exit_dag = tmp_path / "exit.py"
    exit_dag.write_text("import sys\nsys.exit(42)\n", encoding="utf-8")
    stopped = inspect_dags_from_file(exit_dag)
    assert not stopped.sources
    assert "42" in stopped.issues[0].message

    hanging_dag = tmp_path / "hang.py"
    hanging_dag.write_text("while True:\n    pass\n", encoding="utf-8")
    timed_out = inspect_dags_from_file(hanging_dag, timeout=0.3)
    assert not timed_out.sources
    assert "超时" in timed_out.issues[0].message

    good_dag = tmp_path / "good.py"
    good_dag.write_text(
        'from easyflow.sdk import DAG\ndag = DAG("good")\n', encoding="utf-8"
    )
    assert inspect_dags_from_file(good_dag).sources[0].dag_id == "good"


def test_run_detects_changed_imported_python_module(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    helper = tmp_path / "review_helper_module.py"
    helper.write_text("def work(): return 1\n", encoding="utf-8")
    dag_file = tmp_path / "dag.py"
    dag_file.write_text(
        """from easyflow.sdk import DAG
from easyflow.providers.standard.operators.python import PythonOperator
from review_helper_module import work
with DAG("dependencies") as dag:
    PythonOperator(task_id="work", python_callable=work)
""",
        encoding="utf-8",
    )
    dag = inspect_dags_from_file(dag_file).sources[0].dag
    store = SQLiteStateStore(f"sqlite:///{tmp_path / 'state.sqlite'}")
    runner = PersistentDagRunner(store, log_root=tmp_path / "logs")
    run_id = runner.create_manual_run(dag, file_path=dag_file, logical_date=NOW)

    helper.write_text("def work(): return 2\n", encoding="utf-8")
    result = runner.run_existing(dag, run_id=run_id, file_path=dag_file)
    assert result.state.value == "failed"
    assert result.task_states["work"] is None
