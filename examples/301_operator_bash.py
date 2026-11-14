"""按品种和运行日期执行 shell，再把最后一行输出传给 Python 任务。

运行：uv run easyflow dags test example_bash_operator --file examples/301_operator_bash.py
可用 --conf-json '{"symbol":"BTC"}' 覆盖品种；输出包含品种和逻辑日期。
"""

import os
from datetime import datetime, timezone

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG, task
else:
    from easyflow.providers.standard.operators.bash import BashOperator
    from easyflow.sdk import DAG, task


with DAG(
    "example_bash_operator",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    params={"symbol": "DEMO"},
    tags=["example", "operator"],
) as dag:
    shell = BashOperator(
        task_id="run_shell",
        bash_command='printf \'checking\\n%s %s\\n\' "$SYMBOL" "$RUN_DATE"',
        # 模板值通过环境变量传入，避免把参数拼进 shell 源码。
        env={
            "SYMBOL": "{{ params.symbol }}",
            "RUN_DATE": "{{ ds | default('未提供', true) }}",
        },
    )

    @task
    def report(output: str) -> str:
        print(f"shell 最后一行：{output}")
        return output

    report(shell.output)
