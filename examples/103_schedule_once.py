"""一次性调度：适合只需要调度一次的初始化任务。"""

import os
from datetime import datetime, timezone

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.sdk import DAG, task
else:
    from easyflow.sdk import DAG, task


with DAG(
    "example_run_once",
    schedule="@once",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["example", "schedule"],
) as dag:

    @task
    def initialize() -> str:
        message = "一次性初始化完成"
        print(message)
        return message

    initialize()
