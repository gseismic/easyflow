# 示例

这些示例覆盖 easyflow 和 Airflow 3.2 的常用交集。每个 `.py` 都是独立的 DAG 文件，默认使用 easyflow；在 Airflow 环境中运行时设置 `EASYFLOW_EXAMPLE_ENGINE=airflow`。Airflow 的 DAG 解析进程和任务执行进程都需要这个设置，即使两个引擎安装在同一个环境中也应明确选择。示例不需要业务数据库、网络服务等外部业务依赖；调度引擎仍需自己的状态库，量化示例还会写本地文件。

`301_operator_bash.py` 需要系统安装 Bash；其他示例不依赖 Bash。项目安装时会提供 `tzdata`，供缺少系统时区数据库的环境解析 IANA 时区。

文件采用分级编号：大类编号 1 位、类内序号 2 位，例如 `201` 表示大类 `2` 的第 `01` 个示例。DAG ID 是独立标识，不随文件名推导。

| 文件 | DAG ID | 场景 |
| --- | --- | --- |
| [001_basic_manual.py](001_basic_manual.py) | `example_manual_hello` | 手动触发一个 Python 任务 |
| [002_basic_taskflow_etl.py](002_basic_taskflow_etl.py) | `example_taskflow_etl` | `@dag`、`@task`、ETL 和任务间返回值 |
| [101_schedule_daily.py](101_schedule_daily.py) | `example_daily_schedule` | 每日调度和逻辑日期 |
| [102_schedule_weekday.py](102_schedule_weekday.py) | `example_weekday_schedule` | Cron、工作日和上海时区 |
| [103_schedule_once.py](103_schedule_once.py) | `example_run_once` | 调度一次的初始化任务 |
| [201_dependency_fanout.py](201_dependency_fanout.py) | `example_fanout_fanin` | 并行分发、汇合和显式依赖 |
| [202_dependency_trigger_rule.py](202_dependency_trigger_rule.py) | `example_trigger_rule` | 上游跳过、`all_success` 与 `all_done` |
| [301_operator_bash.py](301_operator_bash.py) | `example_bash_operator` | Bash 输出传给 TaskFlow 任务 |
| [302_operator_python.py](302_operator_python.py) | `example_python_operator` | PythonOperator 与 TaskFlow 任务协作 |
| [401_runtime_params.py](401_runtime_params.py) | `example_runtime_params` | 默认参数和手动运行时覆盖 |
| [402_runtime_retry.py](402_runtime_retry.py) | `example_retry` | 默认重试次数和延迟 |
| [501_quant_daily_prices.py](501_quant_daily_prices.py) | `quant_daily_prices` | 每日行情分区、业务日期和本地文件输出 |

## easyflow 运行

在项目根目录启动 `uv run easyflow web --dags-folder examples`，到 Web 页面选择工作流并手动触发。`schedule=None` 的示例只会手动运行；`@daily`、Cron、`@once` 示例启用调度后也可能自动运行。除了量化示例，任务只打印或返回演示数据。

也可以在命令行执行一次不写入 easyflow 状态库的内存测试：

```bash
uv run easyflow dags test example_taskflow_etl --file examples/002_basic_taskflow_etl.py
uv run easyflow dags test example_runtime_params --file examples/401_runtime_params.py \
  --conf-json '{"symbol":"BTC","quantity":3}'
```

在 Web 的“触发运行”中可输入 `{"symbol":"BTC","quantity":3}` 来试运行参数示例。重试示例正常情况下成功；输入 `{"fail":true}` 将故意失败，并在初次执行后最多重试两次。`all_done` 表示上游完成后继续，即使上游失败也可能继续；当它是成功的叶任务时，整个 DAG 的结果可能掩盖上游失败，实际工作流应按需要另行处理失败。

并行分发示例展示的是依赖关系；是否同时执行还取决于调度并发。easyflow 新状态库的任务并发上限默认为 1，需要时可用 `web --parallelism 2` 调整；该设置会持久化到状态库。

## Airflow 3.2 运行

在单独的终端中，从项目根目录准备隔离的 Airflow 环境和临时状态目录。下面使用 Python 3.12、Airflow 3.2.0 的[官方约束文件](https://airflow.apache.org/docs/apache-airflow/3.2.0/installation/installing-from-pypi.html)，不会修改 easyflow 的状态库：

```bash
curl -fsSL https://raw.githubusercontent.com/apache/airflow/constraints-3.2.0/constraints-3.12.txt \
  -o /tmp/easyflow-airflow-constraints.txt
printf '%s\n' '-c /tmp/easyflow-airflow-constraints.txt' \
  'apache-airflow==3.2.0' 'apache-airflow-providers-standard' \
  > /tmp/easyflow-airflow-requirements.txt

export AIRFLOW_HOME=/tmp/easyflow-airflow-demo
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/examples"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAG_RUN_CONF_OVERRIDES_PARAMS=True
export EASYFLOW_EXAMPLE_ENGINE=airflow

airflow_example() {
  uv run --no-project --python 3.12 \
    --with-requirements /tmp/easyflow-airflow-requirements.txt airflow "$@"
}
airflow_example db migrate
airflow_example dags test example_taskflow_etl 2026-07-03T00:00:00+00:00 \
  --dagfile-path "$PWD/examples/002_basic_taskflow_etl.py"
```

同样可把其他表格中的 DAG ID 和文件传给 `airflow_example dags test`。示例中的 `params` 覆盖依赖 Airflow 的 `dag_run_conf_overrides_params` 配置，以上命令显式启用。回到 easyflow 环境时，在该终端执行 `unset EASYFLOW_EXAMPLE_ENGINE`，或设置为 `easyflow`。

## 开始时间、逻辑日期和数据分区

- 原 `quant_data_dag.py` 没有设置 `start_date`。本例现显式设置为 `2026-11-08 08:00 UTC`，从这个时点起参与每日自动调度；第一个对齐的每日刻度为 `2026-11-09 00:00 UTC`。固定日期便于理解和回补，新部署时可按业务需要修改。其他定时示例使用固定的带时区开始时间和 `catchup=False`。
- 当 DAG 和任务都没有 `start_date` 且没有历史自动运行时，首次运行时间取决于 timetable 和当前时间。例如对 Airflow 3.2 的默认 CronTriggerTimetable 做 `@daily`、`catchup=False` 对照，00:01 首次计算会选当天 00:00，12:00 则选次日 00:00；easyflow 在这两个时刻的处理相同。具体规则可查看 [Airflow 3.2 timetable 实现](https://github.com/apache/airflow/blob/3.2.0/airflow-core/src/airflow/timetables/trigger.py)。
- `start_date` 是调度边界，不保证任务恰好在该时刻开始执行。Cron 会按时区对齐时钟刻度，`catchup=False` 不会逐个补齐过去所有周期；首次发现、暂停恢复和数据区间还受引擎与 timetable 配置影响。Airflow 3.2 的 trigger timetable 默认使用触发时点，data interval timetable 则有不同的区间语义，不能混为一谈。
- Airflow 3.2 的部分手动触发方式默认没有逻辑日期；easyflow 当前为手动运行提供逻辑日期。`example_daily_schedule` 会显示“未提供”。量化示例在没有逻辑日期时要求手动输入 `trade_date`，例如 `{"trade_date":"2026-07-01"}`；在两个引擎都可显式传入它以指定同一天的数据。
- 用 `dags test` 指定量化示例的逻辑日期时，日期应不早于其 `start_date`；否则 easyflow 当前仍执行任务，Airflow 3.2 则可能得到没有任务实际执行的成功运行。测试时应核对任务状态或输出文件。已确认的差异见 [KI-20261108-1637-001](../docs/known-issues/KI-20261108-1637-001-test-before-start-date.md)。`trade_date` 是业务日期，可早于逻辑日期和 `start_date`。
- 量化示例指定 `trade_date` 时，业务窗口是该 UTC 日期的 `[00:00, 次日 00:00)`；没有指定时，以运行逻辑时间为结束计算前 24 小时。它是示例定义的业务窗口，不假定 Airflow 不同 timetable 的数据区间相同。
- 量化示例默认写入 `./data/prices/symbol=DEMO/date=日期/data.json`。相同日期重跑会替换该文件；并发写入使用各自的临时文件，最终文件以最后一次替换为准。可在运行参数中指定独立的绝对 `data_root`，并确认 Airflow 任务进程有写入权限。
- 原量化示例已更名但保留 `quant_daily_prices` ID。已有运行记录仍可查看；由于 easyflow 会检查已创建运行对应的源码，旧运行清理后重跑可能被阻止，请使用更新后的 DAG 创建新运行。

参考：[Airflow 3.2 TaskFlow 教程](https://airflow.apache.org/docs/apache-airflow/3.2.0/tutorial/taskflow.html)、[基础教程](https://airflow.apache.org/docs/apache-airflow/3.2.0/tutorial/fundamentals.html)、[Timetables](https://airflow.apache.org/docs/apache-airflow/3.2.0/authoring-and-scheduling/timetable.html)、[3.2 发布说明中的手动运行语义](https://airflow.apache.org/docs/apache-airflow/3.2.0/release_notes.html)。
