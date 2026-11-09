# easyflow

easyflow 是单机、轻量的工作流调度工具，面向 Airflow 3.2 的常用 DAG 编写与运行场景。它使用 SQLite 保存状态，提供本机 Web 界面和 Typer 命令行；它实现的是 Airflow 的一个子集，不是完整替代品。

## 快速开始

需要 Python 3.11 或更高版本。使用 [uv](https://docs.astral.sh/uv/)：

```bash
uv sync
# 首次使用时创建管理员，密码在终端中隐藏输入
uv run easyflow users create admin --role admin
uv run easyflow web --dags-folder examples
```

在本机浏览器打开 <http://127.0.0.1:8790>，使用刚创建的账户登录。`web` 默认启动本机 scheduler，也可以用 `--no-scheduler` 连接已启动的 scheduler。默认状态库是 `.easyflow/easyflow.db`，日志在 `.easyflow/logs`；可用 `--database-url`、`--log-root` 和 `--port` 修改。创建账户时必须指定与 Web 相同的 `--database-url`。Web 仅监听 `127.0.0.1`，不提供公网部署或 TLS。

示例覆盖手动触发、定时、任务依赖、TaskFlow、Operator、参数、重试和量化数据；场景清单、命名规则及 Airflow 3.2 运行方法见 [示例说明](examples/README.md)。

界面可以搜索和筛选工作流，查看调度器状态、调度计划、依赖图、最近 100 次运行、任务详情与日志。管理员可以查看当前 DAG 文件源码，触发运行，暂停或恢复调度，预览并清理任务后重跑，以及预览回补日期。清理任务会再次执行任务，外部写入应由任务保证幂等；只清理单个任务时，已成功的下游任务不会自动重跑。

主导航的“运行历史”按入队时间分页展示当前状态库中所有工作流的运行记录，并可查看每次运行的任务状态和日志。即使工作流文件已移除，仍可查看已保存的历史记录；历史任务定义不可确认时，界面不会把当前定义当作历史定义。工作流详情中的运行选择器展示最近 100 次，“运行历史”标签和“查看全部运行”可查看本工作流的完整历史，支持搜索、状态过滤和分页，且不受全局项目及标签过滤影响。进入运行详情后可返回原工作流历史；刷新地址保留搜索和状态，从第一页开始。

## 按项目查找工作流

在 DAG 的 `tags` 中添加一个 `project:<名称>` 标签表示项目，其他标签用于横向分类。例如 `tags=["project:quant", "daily", "prices"]` 会归入 `quant` 项目，同时带有 `daily` 和 `prices` 标签。工作流 ID 仍在整个实例内唯一，项目只用于组织和筛选，不隔离数据或权限。未设置项目显示“未分组”；项目标签为空或指定多个不同项目时显示“项目异常”。[项目示例](examples/003_basic_project_quant.py)和[分析示例](examples/004_basic_project_analytics.py)仅支持手动触发，不写业务文件。

工作流列表和运行历史将关键词搜索与项目、标签、状态过滤分开，二者可以组合使用。搜索自动生效；多个关键词用空格分隔，必须全部匹配。工作流搜索覆盖工作流 ID、描述、项目、标签和当前定义中的任务 ID；运行历史还覆盖运行 ID 和该次运行实际保存的任务 ID。历史搜索中的描述、项目和标签来自工作流当前定义，已移除工作流仍可按已保存的运行和任务 ID 搜索。

选择多个标签时，包含任一所选标签即可匹配。标签面板的“全选当前列表”仅选中当前标签搜索结果，“全部不选”清除所有标签限制，包含没有普通标签的工作流。项目和标签选择在两个页面之间保留；点击表格中的项目或标签也可直接过滤。搜索框的清空按钮只清空关键词；“清除过滤”清除当前页面的状态及共享的项目和标签，并移除运行历史当前限定的工作流，不清除关键词。工作流的“最近运行失败”只匹配最近一次运行失败的工作流，全部失败记录可在运行历史中查找。

搜索和过滤都发生在分页之前，旧记录也可搜索到。当前列表的关键词和过滤条件保存在地址中，可刷新或分享；分页位置不保存在地址中，刷新后从第一页开始。

项目下拉框显示已发现的项目数和每个项目的记录数；设置搜索、标签或状态条件时显示“当前匹配数 / 总数”。工作流页按工作流计数，历史页按运行次数计数，均覆盖所有分页。统计会定期更新并显示计算时间；工作流仍在发现或解析不完整时，数量显示为未知而非零。

历史记录按工作流**当前**定义的项目和标签归类；修改归属后，旧记录会随之显示在新项目下。工作流文件被移除或暂时无法解析时，历史显示“项目未知”，在“全部项目”或“项目未知”中仍可查看。项目和标签由 DAG 文件提供；修改文件前请注意，已创建的运行可能因定义变更而不能继续执行。

## 账户与权限

Web 和业务 API 必须登录。`admin` 可以执行界面中的管理操作和查看 DAG 源码；`viewer` 可以查看状态、运行、任务详情和日志。日志可能包含业务数据、凭证或代码片段，请只授予可信用户读取权限。账户由有本机权限的操作者通过 CLI 管理：

```bash
uv run easyflow users create reader --role viewer
uv run easyflow users list
uv run easyflow users passwd admin
uv run easyflow users remove reader --yes
```

密码至少 10 个字符，创建或重置时在终端隐藏输入，不作为命令行参数传递。重置密码会使该账户所有现有会话失效；移除账户会使其会话失效，最后一个管理员不可移除。会话有效期为 12 小时，退出登录立即注销；多次登录失败后须稍等再试。状态库仅保存密码的 Argon2id 散列和随机会话令牌的摘要。Web 认证不隔离本机操作系统用户对数据库、日志或 DAG 文件的直接访问。

调度器在线表示租约仍有效，任务心跳表示最近一次报告，并不能保证任务内部持续推进。列表中的下次计划是按当前定义和历史计算的候选时间，不保证准时执行。实际入队和开始时间与逻辑时间分开显示。源码页显示当前 DAG 文件，不保存历史源码快照；运行与当前定义不一致或无法确认时会提示。

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
# 发现并在内存中测试 DAG；任务会实际执行，以下示例不写业务文件
uv run easyflow dags list --dags-folder examples
uv run easyflow dags test example_taskflow_etl --file examples/002_basic_taskflow_etl.py

# 手动触发量化示例会写入本地文件；可通过 --logical-date 指定带时区的 ISO 时间
uv run easyflow dags trigger quant_daily_prices --file examples/501_quant_daily_prices.py

# 单独运行 scheduler；--parallelism 是此状态库的实例级任务上限
uv run easyflow scheduler --dags-folder examples --executor local --parallelism 4

# 暂停后正在运行的任务会完成，其余任务等待恢复
uv run easyflow dags pause quant_daily_prices
uv run easyflow dags unpause quant_daily_prices

# 先查看回补范围，再移除 --dry-run 执行
uv run easyflow backfill create --dag-id example_daily_schedule \
  --file examples/101_schedule_daily.py --start-date 2026-07-01 --end-date 2026-07-05 --dry-run

# 查看重跑影响，确认后移除 --dry-run 并加 --yes
uv run easyflow tasks clear quant_daily_prices --run-id RUN_ID --task-id TASK_ID --dry-run
uv run easyflow tasks clear quant_daily_prices --run-id RUN_ID --task-id TASK_ID --yes
```

`--parallelism N` 会持久化更新该状态库的实例级任务并发上限；省略时沿用已保存值，新状态库默认 1。不同入口共用这个上限，降低上限不会中断正在运行的任务。过期租约不再占用容量；如果失联进程仍在外部执行，无法撤销其副作用。

DAG 暂停状态会持久化。暂停时仍可以创建手动运行，但运行排队等待恢复；已运行任务允许完成。Web 创建或清理运行后由 scheduler 继续执行；scheduler 不在线时界面会提示。`dags trigger` 命令同步等待执行，暂停时返回排队状态及非零退出码。

交互运行时使用 `Ctrl+C`（`SIGINT`）会停止 Web 自行启动的 scheduler；正在执行的本地任务可能中断，重启后按租约与重试策略恢复。向 Web 进程发送 `SIGTERM` 可能留下仍在运行的 scheduler，详见 [已知问题](docs/known-issues/KI-20261108-1718-001-web-sigterm-orphan-scheduler.md)。已单独启动、由 Web 复用的 scheduler 不受 Web 停止控制。

回补支持 `none`、`failed`、`completed` 重处理策略和 `--max-active-runs`。回补并发上限独立于 DAG 普通运行的 `max_active_runs`，但仍受实例任务并发上限约束；不指定回补上限时按一次一个运行执行。Web 预览和 CLI `--dry-run` 单次最多预览 1000 个区间，超出时请分段预览；实际创建不受此预览上限限制。日期输入按 DAG 时区覆盖开始至结束的整日；高频任务可在 Web 切换“精确时间”，按浏览器本地时区输入，或在 CLI 的 `--start-date`、`--end-date` 传入带时区的 ISO 时间（例如 `2026-07-01T00:00:00+08:00` 至 `2026-07-01T00:09:00+08:00`）。精确时间包含起止时刻，预览与实际创建使用相同范围。预览反映查询时的状态，实际创建时会重新检查。容量或依赖阻塞时，命令报告未完成日期，已创建的运行可由 scheduler 继续；对尚未创建的日期可再次执行回补。

已回补的逻辑日期不会再创建同日 scheduled 运行；未来日期的回补不会提前推进定时调度游标。

## 时间和可靠性边界

- 默认定时语义对齐 Airflow 3.2 的 trigger timetable：cron 与 `timedelta` 的 `logical_date`、`data_interval_start`、`data_interval_end` 均为触发时刻。cron 对齐时钟刻度；`timedelta` 的首次运行由开始时间或当前时刻决定。需要处理前一日数据时，应按业务规则明确计算窗口。示例 `501_quant_daily_prices.py` 演示了这一方式。
- 调度使用 DAG `start_date` 的时区；未设置时使用 UTC。`catchup=True` 补齐错过的计划时间，`catchup=False` 跳过中间时间。当前手动运行默认以当前 UTC 时刻作为 `logical_date`，并未实现 Airflow 中空 `logical_date` 的全部场景。
- 支持 `@once`、常见 cron preset、5 字段 cron 和 `timedelta`；不支持秒字段或年份字段。默认 DAG 活跃运行上限为 1，与 Airflow 的部署默认值可能不同。
- DAG 文件在独立子进程解析。常驻模式逐文件异步发现，单个缓慢文件不会阻止其他文件；文件代码仍有本机进程权限，不是不可信代码沙箱。跨文件重复 DAG ID 在相关文件都解析后才能识别，已执行的运行无法撤回；部署前可用 `dags list` 检查解析错误和重复 ID。
- 运行启动前校验 DAG 文件和已追踪到的本地 Python 依赖；运行期动态导入、第三方库、环境变量和外部文件不在此保证内。代码变化会阻止旧运行继续使用新定义，不保证冻结完整运行环境。
- 租约和重试提供故障恢复，不保证外部副作用 exactly-once。LocalExecutor 仅支持本机进程；Web 提供固定的管理员与只读角色，不支持分布式执行、动态任务映射或完整 Airflow Provider/REST/CLI。
- 当前处于开发阶段，不承诺旧版 easyflow 数据库和时间行为的兼容或自动迁移。升级前请自行备份已有数据库。

## 开发与验证

```bash
uv sync --extra dev
uv run pytest
uv run ruff check src tests examples

# 仅修改 Web 源码时需要 Node.js 与 pnpm
cd web
pnpm install
pnpm test
pnpm build
```

前端构建结果随 Python 包一起提供；用户日常启动 Web 界面不需要安装 Node.js。设计和范围见 [优化设计](docs/design/easyflow-20261108-optimization.md)。
