# PLAN-002 持久化手动运行实现计划

> 日期：2026-07-06
>
> 设计依据：`docs/design/easyflow-20260706-airflow-lite.md`
>
> 前置结果：`docs/dev/PLAN-001-airflow-lite-implementation-OUTCOME.md`

## 1. 目标

实现 easyflow v0.1 M1 的核心闭环：从 DAG 文件加载 DAG，创建持久化 manual DagRun，顺序执行
TaskInstance，并把 TaskTry、XCom、TaskInstance 和 DagRun 终态写入 SQLite。

## 2. 范围

本计划覆盖：

- DAG 文件解析和按 `dag_id` 定位 DAG；
- `easyflow dags trigger DAG_ID --file PATH`；
- `easyflow dags state DAG_ID RUN_ID`；
- `easyflow tasks state DAG_ID RUN_ID TASK_ID`；
- `easyflow tasks logs DAG_ID RUN_ID TASK_ID`；
- SQLite StateStore 的 DagRun、TaskInstance、TaskTry、XCom 写入和查询；
- TaskRunner 在本地进程执行任务，并通过 StateStore 原子提交成功/失败；
- 已提交成功的 TaskInstance 再次触发执行时跳过，不自动重跑；
- 失败任务的 retry 基础语义；
- 测试覆盖持久化执行、XCom 传递、失败状态、CLI 查询。

本计划暂不覆盖：

- 常驻 scheduler；
- 多进程 LocalExecutor；
- cron/catchup/backfill；
- scheduler lease；
- `tasks clear` 审计重跑；
- DAG definition hash 不一致后的恢复命令；
- 严格隔离子进程 DAG 解析。

## 3. 设计取舍

- 先实现进程内 DAG 文件解析，保留接口边界，后续再替换为隔离子进程解析。
- `dags trigger` 本轮会同步执行到终态，便于建立可验证闭环；常驻 scheduler 后续接管调度。
- TaskRunner 只提交当前 try 的结果，不做依赖判定；依赖判定由本轮的顺序 runner 负责。
- SQLite 状态更新使用事务和前置状态，避免已成功任务被重复自动提交。

## 4. 验收标准

- `python -m pytest` 通过；
- CLI 可以触发示例 DAG 文件并在 SQLite 中看到 DagRun `success`；
- TaskFlow 上游返回值能通过持久化 XCom 传给下游；
- 任务失败时写入 TaskTry 错误信息，DagRun 进入 `failed`；
- 重复执行同一个 run 不重复运行已 `success` 的 TaskInstance；
- `tasks logs` 能读取本地 TaskTry 日志。
