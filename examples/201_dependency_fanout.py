"""依赖关系：一个起点并行分发两个任务，再汇合到终点。"""

import os
from datetime import datetime, timezone

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import DAG, task
else:
    from easyflow.providers.standard.operators.empty import EmptyOperator
    from easyflow.sdk import DAG, task


with DAG(
    "example_fanout_fanin",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["example", "dependency"],
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    @task
    def process(region: str) -> str:
        print(f"处理区域：{region}")
        return region

    east = process.override(task_id="east")("east")
    west = process.override(task_id="west")("west")
    start >> [east, west] >> finish
