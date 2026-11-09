# 运行与管理进阶

> 创建时间：2026-11-09 01:29 Asia/Shanghai；更新时间：2026-11-10 04:36 Asia/Shanghai。创建和更新模型：Codex，基于 GPT-6；环境未提供完整模型 ID。创建基准为提交 `d4778622be685143381220ed86a5917b84e282fc`；本次整理依据提交 `6fc1614feae89124a5040f22dbd1b41b791bbaa6` 的 easyflow 0.1.0 源码。

安装、启动 Web、触发运行、查询日志，以及清理和回补的基本操作，见[使用教程](quickstart.md)。本文补充独立调度器和其他进阶选项。

## 环境和路径

默认 DAG 目录是当前工作目录下的 `dags`，默认数据库为 `sqlite:///./.easyflow/easyflow.db`，默认日志目录为 `./.easyflow/logs`；教程为了隔离演示显式指定了其他路径。也可将本地 easyflow 源码安装到目标 Python 环境，例如 `python -m pip install -e /path/to/easyflow`，再直接运行 `easyflow`。常规操作通过 CLI 或 Web，不要直接修改 SQLite 中的状态表。

## 发现、测试和触发

`--file` 指定 DAG 源文件，也可以用 `--dags-folder` 从目录发现；`--logical-date` 使用带时区的 ISO 时间，省略时为当前 UTC；`--conf-json` 必须是 JSON 对象。发现时应检查标准错误：即使命令返回码为 0，也可能存在其他文件的解析错误或重复 DAG ID。常驻模式按文件异步发现，跨文件冲突可能在完全发现前已启动任务；检查也不能阻止检查后文件发生变化。

当前版本在 `dags test` 的逻辑时间早于 `start_date` 时仍会执行任务（`KI-20261108-1637-001`）；测试时应使用不早于开始时间的逻辑时间，并检查任务状态或输出。

## Web 和 scheduler

除教程中的 `admin` 外，还可创建 `viewer` 只读账户；它可以查看状态、运行和日志，但不能触发或管理工作流。使用 `easyflow users create NAME --role viewer`、`users list`、`users passwd NAME` 和 `users remove NAME --yes` 管理账户；记得为使用的状态库指定相同的 `--database-url`。最后一个管理员不能移除。

如需将 scheduler 单独运行，可先停止教程中的 Web，然后在 easyflow 源码仓库根目录打开两个终端：

```bash
# 终端 A：单独运行 scheduler，使用教程的状态库和日志目录。
uv run easyflow scheduler --dags-folder tutorial-work/dags \
  --database-url sqlite:///./tutorial-work/easyflow.db \
  --log-root tutorial-work/logs --executor local --parallelism 2

# 终端 B：使用同一状态库；Web 不自行启动 scheduler。
uv run easyflow web --dags-folder tutorial-work/dags \
  --database-url sqlite:///./tutorial-work/easyflow.db \
  --log-root tutorial-work/logs --no-scheduler
```

`scheduler` 命令的默认执行器是 `sequential`，显式 `local` 使用本机进程；Web 自启的 scheduler 使用 `local`。`--parallelism` 设置整个状态库的任务并发上限，并持久化到状态库；新库默认 1，省略时保留已有值。降低上限不会中断已运行任务，实际并发还受 DAG 限制。停止 Web 不会停止单独启动的 scheduler；需在终端 A 单独停止。

## 暂停、清理和回补

在教程的基本用法之外，`tasks clear` 可多次指定 `--task-id` 选择多个任务；不指定时选择整个运行。

回补的 `--max-active-runs` 是独立的运行上限，仍受实例任务并发约束；默认一次一个运行。CLI 的精确时间起止均包含端点，须使用带时区的 ISO 时间；Web 的精确时间按浏览器本地时区输入。若回补报告未完成，已创建运行可由 scheduler 继续，尚未创建的日期可再次回补。

## 调度和排查要点

scheduler 在线只代表租约有效；任务心跳不保证业务内部推进。常见排查步骤见[使用教程](quickstart.md#8-常见问题)。如果有 easyflow 源码仓库，可查看仓库根目录的 `docs/KNOWN-ISSUES.md`，以及对应版本的源码与测试。
