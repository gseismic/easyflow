"""触发规则：上游跳过后，仅 all_done 的下游继续运行。"""

import os
from datetime import datetime, timezone

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.exceptions import AirflowSkipException
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import DAG, task
else:
    from easyflow.providers.standard.operators.empty import EmptyOperator
    from easyflow.sdk import DAG, AirflowSkipException, task


with DAG(
    "example_trigger_rule",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["example", "dependency"],
) as dag:

    @task
    def skip() -> None:
        raise AirflowSkipException("演示跳过")

    skipped = skip()
    on_success = EmptyOperator(task_id="only_on_success")
    always = EmptyOperator(task_id="after_completion", trigger_rule="all_done")
    skipped >> [on_success, always]
