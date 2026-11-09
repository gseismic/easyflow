# DAG 编写进阶

> 创建时间：2026-11-09 01:29 Asia/Shanghai；更新时间：2026-11-10 04:36 Asia/Shanghai。创建和更新模型：Codex，基于 GPT-6；环境未提供完整模型 ID。创建基准为提交 `d4778622be685143381220ed86a5917b84e282fc`；本次整理依据提交 `6fc1614feae89124a5040f22dbd1b41b791bbaa6` 的 easyflow 0.1.0 源码。

首次编写、运行 DAG 及基本依赖和返回值，先看[使用教程](quickstart.md)。本文补充其他 DAG 写法和任务配置。

## 定义和发现

`@dag(...)` 装饰后的函数是工厂，需要调用一次生成 DAG：

```python
from easyflow.sdk import dag, task


@dag(dag_id="factory_example", schedule=None)
def factory():
    @task
    def hello() -> None:
        print("Hello from a DAG factory!")

    hello()


workflow = factory()
```

同一 DAG 的任务 ID 不得冲突。重复调用同一个 TaskFlow 任务会自动生成 `task__1` 等后缀；可用 `task_function.override(task_id="new_id")(...)` 指定名称。

`schedule` 支持 `None`、`@once`、`@hourly`、`@daily`、`@weekly`、`@monthly`、`@yearly`、五字段 cron 和正的 `datetime.timedelta`，不支持 cron 秒或年份字段。`start_date`、`end_date` 如有指定，必须带时区，且开始早于结束。

DAG 还支持 `max_active_tasks` 和 `max_active_runs`。默认 DAG 活跃运行上限是 1；它与整个状态库的任务并发上限是不同约束。

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

也支持 `a >> [b, c] >> d`、`<<` 和 `chain(...)`。TaskFlow 返回值默认通过 `return_value` XCom 传递；使用 `@task(multiple_outputs=True)` 时，函数需返回键为字符串的映射，才能通过 `result["key"]` 取单个键。`do_xcom_push=False` 会关闭结果推送，不要再依赖该输出。

支持的触发规则为 `all_success`（默认）、`all_done`、`one_success`、`one_failed`、`none_failed_min_one_success`、`always`；可传字符串或 `easyflow.sdk.TriggerRule`。任务中抛 `AirflowSkipException` 可标为跳过。DAG 运行结果根据叶任务判断：如果失败上游之后唯一叶任务以 `all_done` 成功，整体可能成功；需要时显式设计失败传播并检查每个任务状态。

## 参数、上下文和模板

- 任务的 `params` 会覆盖 DAG 的同名默认值，手动运行的 `--conf-json` / `run_conf` 再覆盖二者；这些是浅层字典合并。
- 任务执行期间可用 `get_current_context()`；常用键有 `params`、`dag_run`、`run_id`、`logical_date`、`data_interval_start`、`data_interval_end`、`ds`、`ts`、`ti` / `task_instance`。其中 `dag_run.conf` 是本次运行的原始配置。任务函数也可以声明同名参数接收上下文；不要在 DAG 文件顶层调用 `get_current_context()`。
- `PythonOperator` 的 `op_args`、`op_kwargs` 和 `BashOperator` 的 `bash_command`、`env`、`cwd` 是模板字段；Jinja 未定义变量会导致任务失败。避免将未经处理的外部输入直接拼入 shell 命令。
- `BashOperator` 使用 Bash，默认临时工作目录、返回输出最后一行；退出码 99 默认标为跳过，`skip_on_exit_code=None` 可关闭该规则。若命令依赖文件或当前目录，显式指定合适的 `cwd`。

## 重试和限制

除[教程中的默认重试参数](quickstart.md#配置任务重试)外，还支持 `retry_exponential_backoff`、`max_retry_delay`、`depends_on_past` 等；内存 `DAG.test()` 不能验证跨运行的 `depends_on_past`。例如 `workflow.test(logical_date=aware_datetime, run_conf={"name": "Ada"})` 返回的结果对象有 `success`、`task_states`、`task_results`、`try_numbers`，其中 `aware_datetime` 须为带时区的 `datetime`。

不要使用未经核实的 Airflow 特性：动态任务映射、完整 Provider、`execution_timeout`、任务组、pool、callback 和自定义 timetable 等当前不受支持；不支持的公开参数会报错。要确认具体构造参数及兼容行为，可查看所用版本的源码和测试；如果有 easyflow 源码仓库，可检查仓库根目录的 `src/easyflow/sdk/dag.py`、`src/easyflow/sdk/operators/base.py` 和 `src/easyflow/sdk/_compat.py`。
