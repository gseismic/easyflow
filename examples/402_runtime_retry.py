"""任务重试：模拟首次失败、重试成功；设置 fail=true 可观察重试耗尽。

运行：uv run easyflow dags test example_retry --file examples/402_runtime_retry.py
预期：首次调用失败，第二次成功；任务进程需要共享本机临时目录。
"""

import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

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
    def prepare_request() -> str:
        # 每次运行生成独立标记，重试换进程也能观察到首次调用。
        with tempfile.NamedTemporaryFile(prefix="easyflow-retry-", delete=False) as marker:
            return marker.name

    @task
    def call_service(first_attempt_marker: str) -> str:
        marker = Path(first_attempt_marker)
        first_attempt = marker.exists()
        marker.unlink(missing_ok=True)
        conf = get_current_context()["dag_run"].conf or {}
        if conf.get("fail", False):
            raise RuntimeError("模拟持续故障：重试耗尽后任务失败")
        if first_attempt:
            raise RuntimeError("模拟临时故障：下一次重试恢复")
        message = "模拟服务重试后成功"
        print(message)
        return message

    call_service(prepare_request())
