"""每日定时：展示逻辑日期，也允许没有逻辑日期的手动运行。"""

import os
from datetime import datetime, timezone

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.sdk import DAG, get_current_context, task
else:
    from easyflow.sdk import DAG, get_current_context, task


with DAG(
    "example_daily_schedule",
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["example", "schedule"],
) as dag:

    @task
    def show_logical_date() -> str:
        logical_date = get_current_context().get("logical_date")
        value = logical_date.isoformat() if logical_date is not None else "未提供"
        print(f"本次运行的逻辑时间：{value}")
        return value

    show_logical_date()
