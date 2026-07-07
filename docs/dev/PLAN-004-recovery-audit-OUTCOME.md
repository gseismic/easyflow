# PLAN-004 恢复与审计操作实现结果

> 日期：2026-07-06
>
> 对应计划：`docs/dev/PLAN-004-recovery-audit.md`
>
> 对应设计：`docs/design/easyflow-20260706-airflow-lite.md`

## 1. 已完成

- 实现 scheduler lease：
  - `SQLiteStateStore.acquire_scheduler_lease(...)`
  - `SQLiteStateStore.release_scheduler_lease(...)`
  - `easyflow scheduler --once` 默认获取 lease；
  - lease 未过期且 owner 不同时拒绝启动；
  - lease 过期后允许新 owner 接管。
- 实现 DAG definition hash 恢复拦截：
  - 持久化 run 记录创建时 hash；
  - `PersistentDagRunner.run_existing(..., file_path=...)` 对比当前 hash；
  - hash 不一致时 DagRun 标记为 `failed`；
  - 写入 audit_log：`dag_definition_changed`；
  - 不执行新代码。
- 实现 `tasks clear`：
  - `easyflow tasks clear DAG_ID --run-id RUN_ID`
  - `--task-id` 可选择单个或多个任务；
  - `--dry-run` 只预览；
  - 无 `--yes` 时拒绝执行；
  - 执行后清空选中 TaskInstance 状态；
  - 删除当前 run 的可读 XCom；
  - 保留历史 TaskTry 和日志；
  - 已终态 DagRun 恢复为 `queued`；
  - 写入 audit_log：`tasks_clear`。

## 2. 验证结果

已执行：

```bash
python -m pytest -q
```

结果：

```text
29 passed
```

已执行：

```bash
python -m compileall -q src tests examples
```

结果：编译检查通过。

已执行 CLI smoke：

```bash
PYTHONPATH=src python -m easyflow.cli dags trigger ...
PYTHONPATH=src python -m easyflow.cli tasks clear ... --dry-run
PYTHONPATH=src python -m easyflow.cli tasks clear ... --yes
```

结果：dry-run、确认执行和状态迁移均成功。

## 3. 覆盖的关键行为

- 有效 scheduler lease 会阻止第二个 owner 启动；
- lease 过期后新 owner 可以接管；
- DAG 文件变化后，未完成历史 run 不会执行新代码；
- clear dry-run 不修改状态；
- clear 执行后删除 XCom、保留 TaskTry、DagRun 回到 queued；
- clear 后再次运行同一 run 会重新执行被清理任务；
- clear 和 definition hash 拦截都有审计记录。

## 4. 未完成能力

仍未完成：

- 常驻 scheduler 循环和定期续租；
- task lease、heartbeat 和 orphaned task 自动恢复；
- LocalExecutor 多进程；
- 任意 cron 表达式；
- max_active_runs / max_active_tasks 调度限流；
- DAG 文件隔离子进程解析；
- DAG source bundle 版本化。

## 5. 复审结论

本轮补齐了最重要的防误操作和恢复保护。easyflow 现在不会在 DAG 定义变化后悄悄用新代码继续
历史 run；`tasks clear` 也具备预览、显式确认、状态迁移和审计记录。

下一步应优先实现 task lease / heartbeat / orphaned task 恢复以及常驻 scheduler 续租循环，
这是进程异常退出后自动恢复能力的核心。
