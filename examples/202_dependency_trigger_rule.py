"""触发规则：上游跳过后，仅 all_done 的下游继续运行。"""

import os
from datetime import datetime, timezone

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import DAG
else:
    from easyflow.providers.standard.operators.bash import BashOperator
    from easyflow.providers.standard.operators.empty import EmptyOperator
    from easyflow.sdk import DAG


with DAG(
    "example_trigger_rule",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["example", "dependency"],
) as dag:
    skipped = BashOperator(task_id="skip", bash_command="exit 99")
    on_success = EmptyOperator(task_id="only_on_success")
    always = EmptyOperator(task_id="after_completion", trigger_rule="all_done")
    skipped >> [on_success, always]
