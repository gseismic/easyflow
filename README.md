# easyflow

easyflow 是单机、轻量的工作流调度工具，面向 Airflow 3.2 的常用 DAG 编写与运行场景。它使用 SQLite 保存状态，提供本机 Web 界面和 Typer 命令行；它实现的是 Airflow 的一个子集，不是完整替代品。

## 快速开始

需要 Python 3.11 或更高版本。使用 [uv](https://docs.astral.sh/uv/)：

```bash
uv sync
uv run easyflow web --dags-folder examples
```

在本机浏览器打开 <http://127.0.0.1:8790>。`web` 默认启动本机 scheduler，也可以用 `--no-scheduler` 连接已启动的 scheduler。默认状态库是 `.easyflow/easyflow.db`，日志在 `.easyflow/logs`；可用 `--database-url`、`--log-root` 和 `--port` 修改。Web 仅监听 `127.0.0.1`，没有多用户或登录功能，请在可信的本机账户环境中使用。

界面可以查看 DAG、依赖图、最近 100 次运行和各次任务日志，触发运行，暂停或恢复调度，预览并清理任务后重跑，以及预览回补日期。清理任务会再次执行任务，外部写入应由任务保证幂等；只清理单个任务时，已成功的下游任务不会自动重跑。

## 编写 DAG

```python
from easyflow.sdk import DAG, task

with DAG("example", schedule="@daily") as dag:
    @task
    def extract():
        return [1, 2, 3]

    @task
    def summarize(values):
        print(sum(values))

    summarize(extract())
```

将文件放到 DAG 目录。支持 `with DAG(...)`、`@dag`、`@task`、`PythonOperator`、`BashOperator`、`EmptyOperator`、常见依赖写法、有限 Context 和 JSON XCom。任务参数引用上游输出时自动建立依赖。可在 `default_args` 设置支持的任务默认参数；不支持的参数会报错。具体可用接口以代码签名和测试为准。

## 常用命令

所有命令可加 `--help` 查看选项：

```bash
# 发现并在内存中测试 DAG，不创建持久化运行
uv run easyflow dags list --dags-folder examples
uv run easyflow dags test quant_daily_prices --file examples/quant_data_dag.py

# 手动触发；可通过 --logical-date 指定带时区的 ISO 时间
uv run easyflow dags trigger quant_daily_prices --file examples/quant_data_dag.py

# 单独运行 scheduler；--parallelism 是此状态库的实例级任务上限
uv run easyflow scheduler --dags-folder examples --executor local --parallelism 4

# 暂停后正在运行的任务会完成，其余任务等待恢复
uv run easyflow dags pause quant_daily_prices
uv run easyflow dags unpause quant_daily_prices

# 先查看回补范围，再移除 --dry-run 执行
uv run easyflow backfill create --dag-id quant_daily_prices \
  --file examples/quant_data_dag.py --start-date 2026-07-01 --end-date 2026-07-05 --dry-run

# 查看重跑影响，确认后移除 --dry-run 并加 --yes
uv run easyflow tasks clear quant_daily_prices --run-id RUN_ID --task-id TASK_ID --dry-run
uv run easyflow tasks clear quant_daily_prices --run-id RUN_ID --task-id TASK_ID --yes
```

`--parallelism N` 会持久化更新该状态库的实例级任务并发上限；省略时沿用已保存值，新状态库默认 1。不同入口共用这个上限，降低上限不会中断正在运行的任务。过期租约不再占用容量；如果失联进程仍在外部执行，无法撤销其副作用。

DAG 暂停状态会持久化。暂停时仍可以创建手动运行，但运行排队等待恢复；已运行任务允许完成。Web 创建或清理运行后由 scheduler 继续执行；scheduler 不在线时界面会提示。`dags trigger` 命令同步等待执行，暂停时返回排队状态及非零退出码。

停止 `easyflow web` 会同时停止由它启动的 scheduler；正在执行的本地任务可能中断，重启后按租约与重试策略恢复。已单独启动、由 Web 复用的 scheduler 不受影响。

回补支持 `none`、`failed`、`completed` 重处理策略和 `--max-active-runs`。回补并发上限独立于 DAG 普通运行的 `max_active_runs`，但仍受实例任务并发上限约束；不指定回补上限时按一次一个运行执行。预览反映查询时的状态，实际创建时会重新检查。容量或依赖阻塞时，命令报告未完成日期，已创建的运行可由 scheduler 继续；对尚未创建的日期可再次执行回补。

## 时间和可靠性边界

- 默认定时语义对齐 Airflow 3.2 的 trigger timetable：cron 与 `timedelta` 的 `logical_date`、`data_interval_start`、`data_interval_end` 均为触发时刻。cron 对齐时钟刻度；`timedelta` 的首次运行由开始时间或当前时刻决定。需要处理前一日数据时，应按业务规则明确计算窗口。示例 `quant_data_dag.py` 演示了这一方式。
- 调度使用 DAG `start_date` 的时区；未设置时使用 UTC。`catchup=True` 补齐错过的计划时间，`catchup=False` 跳过中间时间。当前手动运行默认以当前 UTC 时刻作为 `logical_date`，并未实现 Airflow 中空 `logical_date` 的全部场景。
- 支持 `@once`、常见 cron preset、5 字段 cron 和 `timedelta`；不支持秒字段或年份字段。默认 DAG 活跃运行上限为 1，与 Airflow 的部署默认值可能不同。
- DAG 文件在独立子进程解析。常驻模式逐文件异步发现，单个缓慢文件不会阻止其他文件；文件代码仍有本机进程权限，不是不可信代码沙箱。跨文件重复 DAG ID 在相关文件都解析后才能识别，已执行的运行无法撤回；部署前可用 `dags list` 检查解析错误和重复 ID。
- 运行启动前校验 DAG 文件和已追踪到的本地 Python 依赖；运行期动态导入、第三方库、环境变量和外部文件不在此保证内。代码变化会阻止旧运行继续使用新定义，不保证冻结完整运行环境。
- 租约和重试提供故障恢复，不保证外部副作用 exactly-once。LocalExecutor 仅支持本机进程；不支持分布式执行、多用户权限、动态任务映射或完整 Airflow Provider/REST/CLI。
- 当前处于开发阶段，不承诺旧版 easyflow 数据库和时间行为的兼容或自动迁移。升级前请自行备份已有数据库。

## 开发与验证

```bash
uv sync --extra dev
uv run pytest
uv run ruff check src tests examples

# 仅修改 Web 源码时需要 Node.js 与 pnpm
cd web
pnpm install
pnpm build
```

前端构建结果随 Python 包一起提供；用户日常启动 Web 界面不需要安装 Node.js。设计和范围见 [优化设计](docs/design/easyflow-20261108-optimization.md)。
