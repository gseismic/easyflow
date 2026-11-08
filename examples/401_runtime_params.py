"""运行参数：从默认 params 和手动触发参数中读取任务输入。"""

import os
from datetime import datetime, timezone

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.sdk import DAG, get_current_context, task
else:
    from easyflow.sdk import DAG, get_current_context, task


with DAG(
    "example_runtime_params",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    params={"symbol": "DEMO", "quantity": 2},
    tags=["example", "runtime"],
) as dag:

    @task
    def summarize() -> dict[str, object]:
        context = get_current_context()
        params = context["params"]
        result = {"symbol": params["symbol"], "quantity": params["quantity"]}
        print(f"运行参数：{result}")
        return result

    summarize()
