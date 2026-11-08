from __future__ import annotations

import os

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.sdk import DAG, task
else:
    from easyflow.sdk import DAG, task


with DAG(
    "example_project_analytics",
    description="分析项目的日报任务示例",
    schedule=None,
    tags=["project:analytics", "daily", "report"],
) as dag:

    @task
    def preview_report() -> str:
        return "分析日报示例"

    preview_report()
