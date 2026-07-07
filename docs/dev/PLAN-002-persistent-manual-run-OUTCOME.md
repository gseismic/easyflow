# PLAN-002 持久化手动运行实现结果

> 日期：2026-07-06
>
> 对应计划：`docs/dev/PLAN-002-persistent-manual-run.md`
>
> 对应设计：`docs/design/easyflow-20260706-airflow-lite.md`

## 1. 已完成

- 新增 DAG 文件解析：
  - `load_dag_from_file(file_path, dag_id)`
  - `definition_hash_for_dag(dag, file_path)`
- 扩展 SQLite StateStore：
  - 创建 DagRun 和初始 TaskInstance；
  - 查询 DagRun 状态；
  - 查询 TaskInstance 状态；
  - 启动 TaskTry；
  - 写入 TaskTry 日志路径；
  - 原子提交 success 和 XCom；
  - 原子提交 failure 和错误信息；
  - 写入 `upstream_failed` / `skipped`；
  - 查询持久化 XCom；
  - 查询最新 TaskTry 日志路径。
- 新增持久化顺序 runner：
  - `PersistentDagRunner.create_manual_run(...)`
  - `PersistentDagRunner.run_existing(...)`
  - 已成功 TaskInstance 重复执行时跳过；
  - TaskFlow 参数从持久化 XCom 解析；
  - stdout/stderr 写入本地 TaskTry 日志。
- 扩展 CLI：
  - `easyflow dags trigger DAG_ID --file PATH`
  - `easyflow dags state DAG_ID RUN_ID`
  - `easyflow tasks state DAG_ID RUN_ID TASK_ID`
  - `easyflow tasks logs DAG_ID RUN_ID TASK_ID`
- 更新 README 的持久化运行说明。

## 2. 验证结果

已执行：

```bash
python -m pytest -q
```

结果：

```text
20 passed
```

覆盖内容：

- 持久化 manual DagRun 成功运行；
- TaskFlow XCom 通过 SQLite 传递；
- TaskTry 日志可查询；
- 已 success 的 TaskInstance 再次执行同一 run 时不会重跑；
- 失败任务按 retry 写入两次 TaskTry 并使 DagRun 失败；
- CLI trigger/state/logs 可用。

## 3. 未完成能力

仍未完成：

- 常驻 scheduler；
- DAG 文件隔离子进程解析；
- LocalExecutor 多进程；
- cron/timedelta 自动调度；
- catchup；
- backfill 执行；
- `depends_on_past` 跨 DagRun；
- `tasks clear` 审计操作；
- scheduler/task lease 和 heartbeat 恢复；
- DAG definition hash 不一致后的恢复拦截。

## 4. 复审结论

本轮完成了 M1 的关键手动运行闭环：DAG 文件可以被 CLI 触发，运行状态、任务尝试、XCom 和日志
都进入持久化存储。TaskRunner 的成功/失败提交已经通过 StateStore 事务完成，避免依赖 scheduler
内存事件来保存已完成任务。

当前实现仍是同步顺序 runner，不是常驻 scheduler。后续应优先实现 scheduler loop、lease、
orphaned task 恢复和 backfill，因为这些能力决定进程重启后的完整稳定性。
