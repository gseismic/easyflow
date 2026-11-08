"""BashOperator：执行 shell，并把最后一行输出传给 Python 任务。"""

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
    tags=["example", "operator"],
) as dag:
    shell = BashOperator(
        task_id="run_shell",
        bash_command="printf 'checking\nready\n'",
    )

    @task
    def report(output: str) -> str:
        print(f"shell 最后一行：{output}")
        return output

    report(shell.output)
