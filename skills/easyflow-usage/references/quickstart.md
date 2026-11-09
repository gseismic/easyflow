# easyflow 使用教程

> 创建时间：2026-11-10 03:57 Asia/Shanghai；更新时间：2026-11-10 04:36 Asia/Shanghai。创建和更新模型：Codex，基于 GPT-6；环境未提供完整模型 ID。本文依据提交 `6fc1614feae89124a5040f22dbd1b41b791bbaa6` 的源码和示例编写，适用于该版本的 easyflow 0.1.0。

本教程带你从零创建一个订单汇总工作流，在命令行测试它，在 Web 界面触发运行并查看日志，然后了解定时、重试和回补。示例使用模拟数据，不访问外部服务，也不写业务文件。

easyflow 是单机工作流调度工具。一个 **DAG** 是由任务和依赖关系组成的工作流；一次执行称为一次**运行**。例如，下面的工作流先提取订单，再计算汇总，最后打印报告：

```text
extract → summarize → report
```

## 1. 准备环境

需要 Python 3.11 或更高版本、[uv](https://docs.astral.sh/uv/) 和 easyflow 的源码。以下命令以 macOS 或 Linux 的 shell 为例，均在 **easyflow 源码仓库根目录**运行。先确认当前目录中的 `pyproject.toml` 属于 easyflow，再安装依赖：

```bash
uv sync
```

后续使用 `uv run easyflow ...`，由 uv 使用 easyflow 的 Python 环境。若已将 easyflow 安装到其他 Python 环境，也可在该环境中直接运行 `easyflow ...`；路径仍以执行命令时的工作目录为准。Skill 可以单独复制，这并不代表当前目录就是 easyflow 源码仓库。

创建教程目录，用来存放 DAG、状态库和日志：

```bash
mkdir -p tutorial-work/dags
```

本教程显式使用 `tutorial-work/easyflow.db` 和 `tutorial-work/logs`，与默认的 `.easyflow` 状态库分开。如果这些路径已用于其他用途，请改用新的目录，并同步修改后续命令中的路径。示例只会发现 `tutorial-work/dags` 中的工作流；不要把不希望执行的文件放进去。

## 2. 编写第一个工作流

将下面的内容保存为 `tutorial-work/dags/order_report.py`。如果同名文件已存在，请另选文件名，并同步修改后续命令。

```python
from datetime import datetime, timezone

from easyflow.sdk import DAG, get_current_context, task


with DAG(
    dag_id="tutorial_order_report",
    description="汇总模拟订单",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    params={"currency": "CNY"},
    tags=["project:tutorial", "beginner"],
) as workflow:

    @task
    def extract() -> list[int]:
        return [12, 25, 18]

    @task
    def summarize(orders: list[int]) -> dict[str, int]:
        return {"count": len(orders), "total": sum(orders)}

    @task
    def report(summary: dict[str, int]) -> None:
        currency = get_current_context()["params"]["currency"]
        print(f"订单数：{summary['count']}，合计：{summary['total']} {currency}")

    report(summarize(extract()))
```

这里的关键部分：

- `dag_id` 是整个 easyflow 实例中的工作流唯一标识，允许字母、数字、点、下划线和连字符。
- `schedule=None` 表示只手动触发；`start_date` 使用带时区的时间。`catchup=False` 表示启用定时调度后不逐个补齐错过的周期。
- `@task` 把 Python 函数声明为任务。最后一行在**构建**工作流时连接任务；函数体在运行任务时才执行。
- `summarize(extract())` 和 `report(...)` 将上游结果传给下游，并自动建立依赖。返回值通过 XCom 传递，必须能够序列化为 JSON，单个值不超过 1 MiB；大数据应传文件或外部存储的引用。
- `params` 提供默认运行参数，`get_current_context()` 在任务执行时读取参数。`project:tutorial` 用于界面分组，`beginner` 是普通标签；项目分组不隔离数据或权限。

发现 DAG 时会执行 Python 文件的顶层代码，因此请将实际业务操作放入任务函数，不要在顶层直接读写业务数据或调用外部服务。

## 3. 在命令行测试

先检查是否能发现工作流：

```bash
uv run easyflow dags list --dags-folder tutorial-work/dags
```

输出应包含 `tutorial_order_report` 和 `<manual>`。同时检查是否有解析错误或重复 DAG ID 的提示；发现错误时先修正文件。不同文件不能使用相同的 DAG ID。

执行一次内存测试，覆盖默认的 `currency` 参数：

```bash
uv run easyflow dags test tutorial_order_report \
  --file tutorial-work/dags/order_report.py \
  --conf-json '{"currency":"USD"}'
```

输出应包含 `订单数：3，合计：55 USD`，以及以 `success` 结尾的运行结果。去掉 `--conf-json` 后，报告使用默认值 `CNY`。

`dags test` 会真正执行任务，但不把这次运行写入 easyflow 状态库，因此 Web 历史中看不到它。该模式在当前进程中顺序执行，重试不等待配置的延迟；它不能代替对调度、并发或故障恢复的验证。自己的任务若会写文件或访问外部系统，测试也会产生这些副作用。

## 4. 启动 Web 并运行工作流

### 创建管理员

在仓库根目录执行：

```bash
uv run easyflow users create tutorial_admin --role admin \
  --database-url sqlite:///./tutorial-work/easyflow.db
```

按提示输入并确认密码，至少 10 个字符；输入时终端不会显示密码。若账号已存在，可用相同的 `--database-url` 执行 `uv run easyflow users list` 查看，或换一个账号名。

### 启动并登录

```bash
uv run easyflow web \
  --dags-folder tutorial-work/dags \
  --database-url sqlite:///./tutorial-work/easyflow.db \
  --log-root tutorial-work/logs
```

在本机浏览器打开 <http://127.0.0.1:8790>，使用刚创建的账号和密码登录。命令会默认启动本机 scheduler（调度器），也可能复用同一状态库中已在线的调度器。只要调度器运行，目录中的定时 DAG 就可能执行；本节的 `tutorial_order_report` 是手动 DAG，不会自动运行。

Web 只监听本机地址，不自带公网访问或 TLS。用 `--port 8791` 可以改端口。需要停止本次 Web 及其自行启动的 scheduler 时，在启动终端按 `Ctrl+C`；正在执行的任务可能中断。向 Web 发送 `SIGTERM` 时，已知可能遗留其自行启动的 scheduler（`KI-20261108-1718-001`）。

### 触发并查看结果

1. 在工作流列表中选择 `tutorial_order_report`，进入详情。
2. 点击“触发运行”，在“运行参数”中填入 `{"currency":"USD"}`，点击“创建运行”。
3. 在“运行与任务”中选择这次运行，等待状态变为成功。调度器需要在线；工作流暂停时运行会先排队。
4. 在“任务依赖”图中点击 `report`，查看“任务日志”，应看到 `订单数：3，合计：55 USD`。也可以点击其他任务查看状态和日志。
5. 查看工作流详情中的“运行历史”，或通过主导航的“运行历史”查看所有工作流的持久运行。

默认参数和本次运行参数会浅层合并，本次传入的同名字段优先。手动运行也可以传 `{}`，此时示例输出 `CNY`。任务日志可能包含业务数据或凭证，只应向可信用户开放。

## 5. 用命令行管理运行

保持 Web 运行时，可以打开第二个终端，进入同一仓库根目录执行以下命令。若停止了 Web，下面的 `dags trigger` 也能自行创建并执行手动运行。

```bash
uv run easyflow dags trigger tutorial_order_report \
  --file tutorial-work/dags/order_report.py \
  --conf-json '{"currency":"USD"}' \
  --database-url sqlite:///./tutorial-work/easyflow.db \
  --log-root tutorial-work/logs
```

该命令创建**持久运行**并等待执行结束；输出包含 DAG ID、运行 ID 和状态。将以下命令中的 `RUN_ID` 替换成实际运行 ID：

```bash
uv run easyflow dags state tutorial_order_report RUN_ID \
  --database-url sqlite:///./tutorial-work/easyflow.db

uv run easyflow tasks state tutorial_order_report RUN_ID report \
  --database-url sqlite:///./tutorial-work/easyflow.db

uv run easyflow tasks logs tutorial_order_report RUN_ID report \
  --database-url sqlite:///./tutorial-work/easyflow.db
```

状态库保存运行和任务状态，日志保存在启动或触发时指定的日志目录。不同终端应使用同一个 `--database-url`，并能访问该目录。相对路径均以执行命令时的工作目录为准。

暂停和恢复工作流：

```bash
uv run easyflow dags pause tutorial_order_report \
  --database-url sqlite:///./tutorial-work/easyflow.db
uv run easyflow dags unpause tutorial_order_report \
  --database-url sqlite:///./tutorial-work/easyflow.db
```

暂停后，已在执行的任务允许完成，其余任务等待恢复；仍可创建手动运行，但它会排队。暂停状态保存在状态库中。暂停时 `dags trigger` 会返回排队状态及非零退出码。

## 6. 添加定时工作流

新建 `tutorial-work/dags/workday.py`，与前面的手动工作流并存：

```python
from datetime import datetime
from zoneinfo import ZoneInfo

from easyflow.sdk import DAG, get_current_context, task


with DAG(
    dag_id="tutorial_workday",
    description="工作日演示",
    schedule="0 9 * * 1-5",
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("Asia/Shanghai")),
    catchup=False,
    tags=["project:tutorial", "schedule"],
) as workflow:

    @task
    def show_time() -> None:
        logical_date = get_current_context()["logical_date"]
        print(f"本次运行的逻辑时间：{logical_date.isoformat()}")

    show_time()
```

这个 cron 表达式表示按上海时区的周一至周五 09:00 调度。它只按星期判断，不会自动排除法定节假日。`start_date` 是调度边界，不保证任务恰好在该时间执行；实际入队和开始时间可能更晚。工作流需要 scheduler 在线才会按计划执行，保存文件后可在列表中等待发现，再进入“调度详情”查看候选时间。

也可以使用 `@once`、`@hourly`、`@daily` 等预设、五字段 cron 或正的 `timedelta`。调度时区来自 `start_date`；未设置时使用 UTC。当前 cron 和 `timedelta` 的逻辑时间及数据区间起止均为触发时刻；如果任务需要处理前一天的数据，应按业务规则计算窗口，不能直接把该区间当成前一天。

`catchup=False` 不会逐个补齐过去所有周期。若改为 `catchup=True`，scheduler 可能创建多个历史运行，实际任务要能承受重复或延迟执行。启动前先检查时间、任务副作用和数据范围。

## 7. 重试、重跑与回补

### 配置任务重试

可以在 DAG 中添加如下参数，为任务设置默认重试策略：

```python
from datetime import timedelta

# 放在 DAG(...) 参数内：
default_args={"retries": 2, "retry_delay": timedelta(seconds=30)}
```

示例表示首次失败后最多再尝试两次，重试间隔为 30 秒；任务自身的配置可以覆盖默认值。easyflow 源码仓库的 `examples/402_runtime_retry.py` 提供完整示例。重试、恢复和重跑可能重复执行任务，外部写入应由业务保证幂等。

### 预览和清理任务

在 Web 中选择一次运行，点击任务，再点击“预览重跑”；选择仅当前任务或全部任务，先查看影响，确认后才清理。清理后由在线 scheduler 再次执行。仅清理单个任务时，已成功的下游任务不会自动重跑。

也可以在命令行先预览，替换为实际的运行 ID：

```bash
uv run easyflow tasks clear tutorial_order_report \
  --run-id RUN_ID --task-id report --dry-run \
  --database-url sqlite:///./tutorial-work/easyflow.db
```

确认影响后，移除 `--dry-run` 并加上 `--yes` 才会执行清理。若工作流文件或其已追踪的本地依赖在运行创建后发生变化，旧运行可能无法继续使用新定义；请先检查界面提示。

### 预览历史回补

回补用于为指定历史时间创建定时运行。对于第 6 节的工作流，可先预览一个工作周：

```bash
uv run easyflow backfill create --dag-id tutorial_workday \
  --file tutorial-work/dags/workday.py \
  --start-date 2026-11-02 --end-date 2026-11-06 --dry-run \
  --database-url sqlite:///./tutorial-work/easyflow.db
```

日期范围按 DAG 时区覆盖开始和结束的整日。预览会列出可创建或跳过的时间，不会创建运行；确认日期、已有状态和任务副作用后，移除 `--dry-run` 才会实际创建并执行回补，并建议加上 `--log-root tutorial-work/logs`。默认只处理缺失时间；`--reprocessing-behavior failed` 或 `completed` 可选择重处理范围。预览与实际创建之间状态可能变化，实际创建时会重新检查。

Web 详情中的“回补预览”同样只预览；实际回补使用 CLI。单次预览最多 1000 个区间，高频任务可分段预览，或通过 CLI 使用带时区的 ISO 时间指定精确起止。更多语义见[运行与管理进阶](operations.md#暂停清理和回补)。

## 8. 常见问题

| 现象 | 检查方法 |
| --- | --- |
| 工作流没有出现 | 确认当前目录、`--dags-folder` 和文件后缀；运行 `dags list`，检查解析错误和重复 DAG ID。Web 异步发现文件，可能需要稍候。 |
| 无法登录 | 确认创建账号和启动 Web 使用相同的 `--database-url`；密码输入不会显示，创建时至少需要 10 个字符。 |
| 运行一直排队 | 查看调度器是否在线、工作流是否暂停，以及其他运行是否占满并发。`web --no-scheduler` 不会自行启动调度器。 |
| 看不到测试结果 | `dags test` 不写状态库；在 Web 触发或用 `dags trigger` 才会产生可查看的持久运行。 |
| 任务失败 | 选择具体运行和任务，查看“任务日志”与执行次数；检查参数、文件权限、业务依赖和异常信息。 |
| 找不到日志或历史 | 确认命令的工作目录、状态库和日志目录与原运行一致，且相关文件仍存在。 |
| 定时没有按预期运行 | 检查 `schedule`、`start_date` 的时区、`catchup`、暂停状态和调度器状态；“下次计划”是候选时间，并非准时执行的保证。 |

## 9. 下一步

- [DAG 编写进阶](dag-authoring.md)：Operator、触发规则、模板和任务参数。
- [运行与管理进阶](operations.md)：独立调度器、并发、账户和回补选项。
- 如有 easyflow 源码，可查仓库根目录的 `examples/README.md`、`README.md` 和 `docs/KNOWN-ISSUES.md`；这些文件不是 Skill 的组成部分。
- 任意命令可加 `--help` 查看可用选项，例如 `uv run easyflow scheduler --help`。

easyflow 实现的是 Airflow 的常用子集，不支持分布式执行、动态任务映射和完整的 Airflow Provider。不要将任务重试或故障恢复理解为外部副作用只执行一次。当前项目处于开发阶段，升级前请备份已有状态库。
