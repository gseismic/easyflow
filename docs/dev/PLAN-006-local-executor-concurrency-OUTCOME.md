# PLAN-006 LocalExecutor 与并发限流实现结果

> 日期：2026-07-06
>
> 对应计划：`docs/dev/PLAN-006-local-executor-concurrency.md`
>
> 对应设计：`docs/design/easyflow-20260706-airflow-lite.md`

## 1. 已完成

- 新增 LocalExecutor：
  - 父进程负责依赖判定、进程管理和并发限流；
  - 子进程加载 DAG 文件并定位单个 Task；
  - 子进程执行 Task 并通过 StateStore 原子提交 success/failure；
  - 子进程执行期间使用 Task lease/heartbeat 续租。
- 扩展 `PersistentDagRunner.run_existing(...)`：
  - `executor="sequential"`；
  - `executor="local"`；
  - `parallelism=N`。
- 扩展 CLI：
  - `easyflow dags trigger ... --executor local --parallelism N`；
  - `easyflow scheduler ... --executor local --parallelism N`；
  - `easyflow backfill create ... --executor local --parallelism N`。
- 实现单个 run 内并发限流：
  - `parallelism`；
  - `DAG(max_active_tasks=...)`。
- 实现 scheduler/backfill 创建 run 前的 `max_active_runs` 基础限制。
- 更新 README。

## 2. 验证结果

已执行：

```bash
python -m pytest -q
```

结果：

```text
38 passed
```

已执行：

```bash
python -m compileall -q src tests examples
```

结果：编译检查通过。

已执行 CLI smoke：

```bash
PYTHONPATH=src python -m easyflow.cli dags trigger local_smoke \
  --file local_dag.py \
  --executor local \
  --parallelism 2
```

结果：LocalExecutor 子进程执行成功，TaskFlow XCom 跨进程传递成功。

## 3. 覆盖的关键行为

- 两个无依赖 sleep task 在 LocalExecutor 下并发完成；
- `max_active_tasks=1` 会限制并发；
- TaskFlow 上游 XCom 可以通过 SQLite 传给另一个子进程中的下游；
- 子进程失败会写入 TaskTry，并使 DagRun failed；
- CLI `--executor local --parallelism N` 可用；
- scheduler 遇到已有 active DagRun 时遵守 `max_active_runs`。

## 4. 未完成能力

仍未完成：

- 任意 cron 表达式；
- DAG 文件隔离子进程解析；
- DAG source bundle 版本化；
- 强制终止 OS 进程；
- 远程 Worker / 分布式队列。

## 5. 复审结论

本轮完成了单机并发执行的核心能力。easyflow 现在可以在同一机器上并发执行无依赖任务，并通过
SQLite 状态库完成跨进程 XCom、日志、TaskTry 和 TaskInstance 状态提交。

下一步应优先补任意 cron 表达式和 DAG 解析隔离；这两项分别影响调度覆盖面和加载不可信 DAG
文件时的稳定性。
