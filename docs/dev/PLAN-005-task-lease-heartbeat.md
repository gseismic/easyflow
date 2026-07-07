# PLAN-005 Task Lease、Heartbeat 与 Orphan 恢复实现计划

> 日期：2026-07-06
>
> 设计依据：`docs/design/easyflow-20260706-airflow-lite.md`
>
> 前置结果：`docs/dev/PLAN-004-recovery-audit-OUTCOME.md`

## 1. 目标

补齐 TaskInstance 级恢复能力：

- Task try 启动时写入 lease 和 heartbeat；
- TaskRunner 能续租当前 try；
- scheduler/runner 在恢复时识别 lease 过期的 running task；
- orphaned task 按 retry 策略进入 `up_for_retry` 或 `failed`；
- 提供常驻 scheduler 循环和 lease 续租的基础实现。

## 2. 范围

本计划覆盖：

- `task_instance.lease_expires_at` 和 `last_heartbeat_at` 写入；
- `task_heartbeat` 表写入和更新；
- `renew_task_lease(...)`；
- `recover_orphaned_tasks(...)`；
- running 任务 lease 过期后记录 `TASK_PROCESS_LOST`；
- `PersistentDagRunner.run_existing(...)` 执行前恢复 orphaned task；
- `easyflow scheduler` 常驻循环，支持 `--interval-seconds` 和测试用 `--max-loops`；
- scheduler 常驻循环中的 lease 续租。

本计划暂不覆盖：

- 独立 Worker 进程的后台心跳线程；
- LocalExecutor 多进程；
- 远程 Worker；
- 强制终止 OS 进程。

## 3. 验收标准

- `python -m pytest` 通过；
- Task try 启动后 `task_instance` 保存 lease 和 heartbeat；
- running task lease 过期后能按 retries 恢复为 `up_for_retry`；
- 无剩余 retry 时恢复为 `failed`；
- 恢复后 runner 能继续执行 `up_for_retry` 任务；
- scheduler 常驻循环会续租 scheduler lease；
- CLI `scheduler` 无 `--once` 时可通过 `--max-loops` 做有限循环测试。
