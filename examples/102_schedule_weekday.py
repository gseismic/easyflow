"""Cron 和时区：每周一至周五的北京时间 09:00 运行。"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.sdk import DAG, task
else:
    from easyflow.sdk import DAG, task


with DAG(
    "example_weekday_schedule",
    schedule="0 9 * * 1-5",
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("Asia/Shanghai")),
    catchup=False,
    tags=["example", "schedule"],
) as dag:

    @task
    def prepare_report() -> str:
        message = "生成工作日报表"
        print(message)
        return message

    prepare_report()
