# 运行与管理参考

> 创建时间：2026-11-09 01:29 Asia/Shanghai。创建模型：Codex，基于 GPT-6；环境未提供完整模型 ID。分析范围：easyflow 0.1.0 的 CLI、调度器、Web、示例及已知问题；源码基准提交 `d4778622be685143381220ed86a5917b84e282fc`。

## 环境和路径

需要 Python 3.11+。在 easyflow 源码仓库根目录可以 `uv sync` 后使用 `uv run easyflow ...`；也可以把本地 easyflow 源码安装到目标 Python 环境，例如 `python -m pip install -e /path/to/easyflow`。下文假设该环境中的 `easyflow` 已在 `PATH`；在仓库使用 uv 时在命令前加 `uv run`。

默认 DAG 目录是当前工作目录下的 `dags`，默认数据库为 `sqlite:///./.easyflow/easyflow.db`，默认日志目录为 `./.easyflow/logs`。相对路径依赖运行命令时的工作目录。跨进程使用时，为各命令传入相同的 `--database-url`；查询任务日志还要能访问创建任务时所用的日志路径。常规操作通过 CLI 或 Web，不要直接修改 SQLite 中的状态表。

## 发现、测试和触发

```bash
easyflow dags list --dags-folder dags
easyflow dags test easyflow_greeting --file dags/greeting.py \
  --logical-date 2026-07-01T00:00:00+00:00 --conf-json '{"name":"Ada"}'

# 以下命令会实际执行任务并写状态与日志；只在确认目标和副作用后运行。
easyflow dags trigger easyflow_greeting --file dags/greeting.py \
  --conf-json '{"name":"Ada"}'
```

`--file` 指定 DAG 源文件，或者用 `--dags-folder` 从目录发现；`--logical-date` 必须是带时区的 ISO 时间，缺省为当前 UTC；`--conf-json` 必须是 JSON 对象。`dags test` 不写运行状态库但真实执行；`dags trigger` 同步执行并打印 DAG ID、运行 ID 和状态。若 DAG 暂停则运行保持排队并以非零退出，恢复后由 scheduler 处理。

查询时用实际打印的 ID 替换 `RUN_ID` 和 `TASK_ID`：

```bash
easyflow dags state easyflow_greeting RUN_ID
easyflow tasks state easyflow_greeting RUN_ID TASK_ID
easyflow tasks logs easyflow_greeting RUN_ID TASK_ID
```

## Web 和 scheduler

```bash
# 密码在终端隐藏输入，至少 10 个字符。
easyflow users create admin --role admin
easyflow web --dags-folder dags
```

浏览器访问 `http://127.0.0.1:8790`。`admin` 可查看源码、触发、暂停/恢复及清理；`viewer` 可查看状态、运行和日志。`easyflow users create reader --role viewer`、`users list`、`users passwd NAME`、`users remove NAME --yes` 管理本机账户，最后一个管理员不能移除；账户与 Web 必须使用同一数据库。Web 和业务 API 必须登录，监听地址仅为本机。

Web 默认按需启动 scheduler；`--no-scheduler` 可连接单独运行的 scheduler：

```bash
# 终端 A：单独运行 scheduler。
easyflow scheduler --dags-folder dags --executor local --parallelism 2

# 终端 B：使用相同工作目录和状态库启动 Web。
easyflow web --dags-folder dags --no-scheduler
```

`scheduler` 命令的默认执行器是 `sequential`，显式 `local` 使用本机进程；Web 自启的 scheduler 使用 `local`。可通过 `--parallelism` 提高整个状态库的任务并发上限，该值会持久化。新库默认 1，省略时保留已有值；降低上限不会中断已运行任务，实际并发还受 DAG 限制。调度开始前先检查目录中的 DAG 及其副作用。

交互运行 Web 时使用 `Ctrl+C` 可停止其自行启动的 scheduler；正在执行的本地任务可能中断。当前版本收到 `SIGTERM` 后可能遗留 Web 自启的 scheduler，要核实进程状态；独立启动的 scheduler 本来就不由 Web 控制。租约与重试提供恢复能力，但不保证外部副作用恰好一次。

## 暂停、清理和回补

```bash
easyflow dags pause easyflow_greeting
easyflow dags unpause easyflow_greeting

# 首先预览；多次指定 --task-id 可选择多个任务。
easyflow tasks clear easyflow_greeting --run-id RUN_ID --task-id TASK_ID --dry-run
easyflow tasks clear easyflow_greeting --run-id RUN_ID --task-id TASK_ID --yes

# 对带 schedule 的 DAG 预览回补日期。
easyflow backfill create --dag-id DAG_ID --file dags/scheduled.py \
  --start-date 2026-07-01 --end-date 2026-07-05 --dry-run
```

暂停会持久化；已运行的任务可完成。`clear` 不指定 `--task-id` 时选择整个运行，清理后由 scheduler 重跑；只清理单个任务不会自动重跑已成功的下游。确认影响后移除回补的 `--dry-run` 才会创建并执行；`--reprocessing-behavior` 可选 `none`、`failed`、`completed`。`--max-active-runs` 是回补独立的运行上限，仍受实例任务并发约束；默认一次一个运行。预览后实际执行会重新检查状态。若报告未完成，已创建运行可由 scheduler 继续，尚未创建的日期可再次回补。

## 调度和排查要点

- 调度基于 `start_date` 的时区；`catchup=False` 不逐个补齐错过的周期。cron 与 `timedelta` 的 `logical_date`、`data_interval_start`、`data_interval_end` 均为触发时刻；手动运行默认当前 UTC。需要上一日或其他业务窗口时，在任务中明确计算，不要假设它是前一个自然周期。
- scheduler 在线只代表租约有效；任务心跳不保证业务内部推进。检查 DAG 是否暂停、调度器是否在线、运行与任务状态、日志、并发限制、解析错误及运行源文件是否已变化。源码变化可能阻止既有运行继续。
- 常驻发现按文件异步进行：跨文件重复 ID 可能在冲突完全发现前已启动任务。运行 `dags list` 并检查标准错误可在部署前发现已有冲突，但不能阻止检查后发生的改动。项目仓库的 `docs/KNOWN-ISSUES.md` 有当前问题索引。
