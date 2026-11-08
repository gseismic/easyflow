"""手动触发：运行一个最简单的 Python 任务。"""

import os
from datetime import datetime, timezone

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.sdk import DAG, task
else:
    from easyflow.sdk import DAG, task


with DAG(
    "example_manual_hello",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["example", "basic"],
) as dag:

    @task
    def say_hello() -> str:
        message = "Hello, workflow!"
        print(message)
        return message

    say_hello()
