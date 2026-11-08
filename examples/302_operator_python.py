"""PythonOperator：传统 Operator 与 TaskFlow 共享返回值。"""

import os
from datetime import datetime, timezone

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import DAG, task
else:
    from easyflow.providers.standard.operators.python import PythonOperator
    from easyflow.sdk import DAG, task


def double(number: int) -> int:
    return number * 2


with DAG(
    "example_python_operator",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["example", "operator"],
) as dag:
    result = PythonOperator(
        task_id="double",
        python_callable=double,
        op_kwargs={"number": 21},
    )

    @task
    def report(value: int) -> int:
        print(f"计算结果：{value}")
        return value

    report(result.output)
