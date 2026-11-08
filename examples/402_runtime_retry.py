"""任务重试：默认成功；设置 fail=true 可观察失败和重试。"""

import os
from datetime import datetime, timedelta, timezone

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.sdk import DAG, get_current_context, task
else:
    from easyflow.sdk import DAG, get_current_context, task


with DAG(
    "example_retry",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=3)},
    tags=["example", "runtime"],
) as dag:

    @task
    def call_service() -> str:
        conf = get_current_context()["dag_run"].conf or {}
        if conf.get("fail", False):
            raise RuntimeError("演示失败：任务会按重试配置再次执行")
        message = "模拟服务调用成功"
        print(message)
        return message

    call_service()
