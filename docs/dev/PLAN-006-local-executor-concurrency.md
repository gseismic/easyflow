# PLAN-006 LocalExecutor 与并发限流实现计划

> 日期：2026-07-06
>
> 设计依据：`docs/design/easyflow-20260706-airflow-lite.md`
>
> 前置结果：`docs/dev/PLAN-005-task-lease-heartbeat-OUTCOME.md`

## 1. 目标

实现单机 LocalExecutor 多进程执行，并让调度过程遵守基础并发限制：

- 全局 `parallelism`；
- DAG `max_active_tasks`；
- Task Worker 后台 heartbeat；
- 父进程只做依赖判定和进程管理；
- 子进程执行 Task 并通过 StateStore 原子提交 success/failure。

## 2. 范围

本计划覆盖：

- `LocalExecutor` 内部实现；
- 子进程 worker 入口；
- worker 加载 DAG 文件并定位 Task；
- worker 执行期间后台续租 task lease；
- `PersistentDagRunner.run_existing(..., executor="local")`；
- CLI `--executor sequential|local`；
- CLI `--parallelism`；
- DAG `max_active_tasks` 限流；
- 测试覆盖并发执行速度、限流、失败状态和 XCom 传递。

本计划暂不覆盖：

- 远程 Worker；
- 分布式队列；
- 强制终止 OS 进程；
- 跨多个 DagRun 的全局并发调度；
- pool/queue/priority_weight 等 Airflow 参数。

## 3. 设计取舍

- LocalExecutor 使用 `multiprocessing.Process`，不引入消息队列；
- 子进程通过 SQLite StateStore 提交结果，符合已定稿的 TaskRunner 原子提交方案；
- 父进程不直接执行用户代码；
- 子进程 heartbeat 使用线程实现，后续可替换为独立 Worker runtime；
- 本轮并发限流限定在单个 `run_existing` 调用内，全局多 scheduler 限流后续实现。

## 4. 验收标准

- `python -m pytest` 通过；
- 两个无依赖 sleep task 在 LocalExecutor 下并发完成；
- `parallelism=1` 时同一 DAG 顺序执行；
- `max_active_tasks=1` 时即使 parallelism 更高也顺序执行；
- TaskFlow XCom 在 LocalExecutor 子进程之间通过 SQLite 传递；
- 失败子进程能写入 TaskTry 和 DagRun failed；
- CLI `dags trigger --executor local --parallelism N` 可用。
