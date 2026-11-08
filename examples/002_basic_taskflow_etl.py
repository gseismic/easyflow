"""TaskFlow：提取、转换、加载，任务返回值自动建立依赖。"""

import os
from datetime import datetime, timezone

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.sdk import dag, task
else:
    from easyflow.sdk import dag, task


@dag(
    dag_id="example_taskflow_etl",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["example", "basic"],
)
def taskflow_etl():
    @task
    def extract() -> list[int]:
        return [12, 25, 18]

    @task
    def transform(amounts: list[int]) -> dict[str, int]:
        return {"count": len(amounts), "total": sum(amounts)}

    @task
    def load(summary: dict[str, int]) -> dict[str, int]:
        print(f"订单数：{summary['count']}，合计：{summary['total']}")
        return summary

    load(transform(extract()))


workflow = taskflow_etl()
