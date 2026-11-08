from __future__ import annotations

import os

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.sdk import DAG, task
else:
    from easyflow.sdk import DAG, task


with DAG(
    "example_project_quant",
    description="量化项目的行情任务示例",
    schedule=None,
    tags=["project:quant", "daily", "prices"],
) as dag:

    @task
    def preview_prices() -> str:
        return "量化行情示例"

    preview_prices()
