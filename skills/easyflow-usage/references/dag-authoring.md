# DAG 编写参考

> 创建时间：2026-11-09 01:29 Asia/Shanghai。创建模型：Codex，基于 GPT-6；环境未提供完整模型 ID。分析范围：easyflow 0.1.0 的 DAG、任务、Operator 与测试实现；源码基准提交 `d4778622be685143381220ed86a5917b84e282fc`。

## 定义和发现

- `from easyflow.sdk import DAG, dag, task`；也可以使用 `with DAG("my_workflow", ...) as workflow:` 定义。`@dag(...)` 装饰后的函数是工厂，需调用一次生成 DAG。
- DAG ID 只允许字母、数字、`.`、`_`、`-`；同一 DAG 的任务 ID 不得冲突。重复调用同一个 TaskFlow 任务会自动生成 `task__1` 等后缀；可用 `task_function.override(task_id="new_id")(...)` 指定名称。
- `schedule` 支持 `None`、`@once`、`@hourly`、`@daily`、`@weekly`、`@monthly`、`@yearly`、五字段 cron 和正的 `datetime.timedelta`。不支持 cron 秒或年份字段。`start_date`、`end_date` 必须带时区，且开始早于结束。
- `catchup=True` 补齐漏过的计划时间；`False` 跳过中间时间。另有 `description`、`params`、`tags`、`max_active_tasks`、`max_active_runs`。默认 DAG 活跃运行上限是 1；它与整个状态库的任务并发上限是不同约束。
- 文件解析时会执行模块顶层代码，宜把实际业务动作放进任务函数。发现多个文件或改动已有文件前，确保 DAG ID 唯一并检查解析错误。

## 依赖和结果

`from easyflow.providers.standard.operators.empty import EmptyOperator`，以及对应的 `.python`、`.bash` 路径分别导入 `PythonOperator`、`BashOperator`；也可从 `easyflow.operators` 导入这三个类。

```python
from easyflow.operators import EmptyOperator, PythonOperator
from easyflow.sdk import DAG, task


def double(value: int) -> int:
    return value * 2


with DAG("operator_example", schedule=None) as workflow:
    start = EmptyOperator(task_id="start")
    calc = PythonOperator(task_id="calc", python_callable=double, op_args=[21])

    @task
    def report(value: int) -> None:
        print(value)

    result = report(calc.output)  # 从 Operator 返回值传参，自动建立 calc → report。
    start >> calc
```

也支持 `a >> [b, c] >> d`、`<<` 和 `chain(...)`。TaskFlow 返回值默认通过 `return_value` XCom 传递；使用 `@task(multiple_outputs=True)` 时，函数需返回键为字符串的映射，才能通过 `result["key"]` 取单个键。XCom 的每个值必须可 JSON 序列化，编码后不超过 1,048,576 字节；大型数据传文件或外部存储的引用。`do_xcom_push=False` 会关闭结果推送，不要再依赖该输出。

支持的触发规则为 `all_success`（默认）、`all_done`、`one_success`、`one_failed`、`none_failed_min_one_success`、`always`；可传字符串或 `easyflow.sdk.TriggerRule`。任务中抛 `AirflowSkipException` 可标为跳过。DAG 运行结果根据叶任务判断：如果失败上游之后唯一叶任务以 `all_done` 成功，整体可能成功；需要时显式设计失败传播并检查每个任务状态。

## 参数、上下文和模板

- `DAG(params={...})` 是默认运行参数，任务的 `params` 覆盖 DAG 同名值，手动运行的 `--conf-json` / `run_conf` 再覆盖二者；这些是浅层字典合并。
- 任务执行期间可用 `get_current_context()`；常用键有 `params`、`dag_run`、`run_id`、`logical_date`、`data_interval_start`、`data_interval_end`、`ds`、`ts`、`ti` / `task_instance`。其中 `dag_run.conf` 是本次运行的原始配置。任务函数也可以声明同名参数接收上下文；不要在 DAG 文件顶层调用 `get_current_context()`。
- `PythonOperator` 的 `op_args`、`op_kwargs` 和 `BashOperator` 的 `bash_command`、`env`、`cwd` 是模板字段；Jinja 未定义变量会导致任务失败。避免将未经处理的外部输入直接拼入 shell 命令。
- `BashOperator` 使用 Bash，默认临时工作目录、返回输出最后一行；退出码 99 默认标为跳过，`skip_on_exit_code=None` 可关闭该规则。若命令依赖文件或当前目录，显式指定合适的 `cwd`。

## 重试和限制

`DAG(default_args={"retries": 2, "retry_delay": timedelta(seconds=30)})` 可设置任务默认值，任务显式设置优先。还支持 `retry_exponential_backoff`、`max_retry_delay`、`depends_on_past` 等；内存 `DAG.test()` 不等待重试延迟，也不能验证跨运行的 `depends_on_past`。例如 `workflow.test(logical_date=aware_datetime, run_conf={"name": "Ada"})` 返回的结果对象有 `success`、`task_states`、`task_results`、`try_numbers`，其中 `aware_datetime` 须为带时区的 `datetime`。

不要使用未经核实的 Airflow 特性：动态任务映射、完整 Provider、`execution_timeout`、任务组、pool、callback 和自定义 timetable 等当前不受支持；不支持的公开参数会报错。要确认具体构造参数及兼容行为，查看所用版本的 `src/easyflow/sdk/dag.py`、`src/easyflow/sdk/operators/base.py`、`src/easyflow/sdk/_compat.py` 和对应测试。
