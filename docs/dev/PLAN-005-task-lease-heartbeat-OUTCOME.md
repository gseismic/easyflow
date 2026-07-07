# PLAN-005 Task Lease、Heartbeat 与 Orphan 恢复实现结果

> 日期：2026-07-06
>
> 对应计划：`docs/dev/PLAN-005-task-lease-heartbeat.md`
>
> 对应设计：`docs/design/easyflow-20260706-airflow-lite.md`

## 1. 已完成

- Task try 启动时写入：
  - `executor_id`
  - `worker_pid`
  - `lease_expires_at`
  - `last_heartbeat_at`
  - `task_heartbeat`
- 新增 `renew_task_lease(...)`：
  - 更新 TaskInstance lease；
  - upsert `task_heartbeat`。
- 新增 orphaned task 恢复：
  - `recover_orphaned_task(...)`
  - `recover_orphaned_tasks_for_run(...)`
  - running task lease 过期后记录 `TASK_PROCESS_LOST`；
  - 有剩余 retry 时转为 `up_for_retry`；
  - 无剩余 retry 时转为 `failed`；
  - 写入 audit_log：`task_process_lost`。
- `PersistentDagRunner.run_existing(...)` 执行前恢复当前 run 的 orphaned task。
- Task success/failure 提交时清空 lease 和 heartbeat 指针字段。
- 新增 scheduler 常驻循环：
  - `run_scheduler_loop(...)`
  - CLI `easyflow scheduler` 无 `--once` 时进入循环；
  - `--interval-seconds` 控制轮询间隔；
  - `--max-loops` 用于测试或有限运行；
  - 每轮续租 scheduler lease。
- 更新 README。

## 2. 验证结果

已执行：

```bash
python -m pytest -q
```

结果：

```text
32 passed
```

已执行：

```bash
python -m compileall -q src tests examples
```

结果：编译检查通过。

已执行 CLI smoke：

```bash
PYTHONPATH=src python -m easyflow.cli scheduler --dag-id loop_smoke --file dag.py --max-loops 2 --interval-seconds 0
```

结果：常驻 scheduler 有限循环可正常退出。

## 3. 覆盖的关键行为

- Task try 启动后能查询到 heartbeat；
- 未过期 lease 不会被恢复；
- 过期 running task 有 retry 时进入 `up_for_retry`；
- 过期 running task 无 retry 时进入 `failed`；
- orphaned task 恢复后，同一 run 可以继续执行并成功；
- scheduler 常驻循环会续租 scheduler lease。

## 4. 未完成能力

仍未完成：

- LocalExecutor 多进程；
- 独立 Worker 后台心跳线程；
- 任意 cron 表达式；
- max_active_runs / max_active_tasks 调度限流；
- DAG 文件隔离子进程解析；
- DAG source bundle 版本化。

## 5. 复审结论

本轮补齐了“进程退出后不让 running 任务永久卡住”的核心恢复路径。当前同步 runner 在 try 启动
和执行前续租；如果进程在 running 状态崩溃，后续 runner/scheduler 可以根据过期 lease 把任务
恢复为 retry 或 failed。

后续应优先实现 LocalExecutor 多进程和 Worker 心跳线程，让长任务运行期间也能持续续租。
