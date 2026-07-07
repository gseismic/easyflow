# PLAN-003 调度与 Backfill 实现计划

> 日期：2026-07-06
>
> 设计依据：`docs/design/easyflow-20260706-airflow-lite.md`
>
> 前置结果：`docs/dev/PLAN-002-persistent-manual-run-OUTCOME.md`

## 1. 目标

实现 easyflow v0.1 M2 的核心可验证切片：基于 DAG schedule 创建到期 scheduled DagRun，支持
历史区间 backfill，并实现 `depends_on_past` 在 scheduled/backfill 之间的跨 DagRun 依赖。

## 2. 范围

本计划覆盖：

- schedule 区间计算：
  - `@once`
  - `@hourly`
  - `@daily`
  - `@weekly`
  - `@monthly`
  - `@yearly`
  - `timedelta`
- `easyflow scheduler --once --file PATH --dag-id DAG_ID`；
- `easyflow backfill create --dag-id DAG_ID --file PATH --start-date DATE --end-date DATE`；
- scheduled run 去重；
- backfill run 使用独立 `backfill_id` 和独立 `run_id`；
- `depends_on_past=True` 对 scheduled/backfill 生效，对 manual 隔离；
- CLI 测试覆盖定时调度、backfill、前序失败阻断后序任务。

本计划暂不覆盖：

- cron 任意表达式；
- 常驻无限循环 scheduler；
- scheduler lease 和 heartbeat；
- LocalExecutor 多进程；
- max_active_runs 并发调度；
- `tasks clear`；
- DAG definition hash 变更恢复拦截。

## 3. 设计取舍

- `scheduler --once` 作为常驻 scheduler 的可测试前身：一次扫描、创建到期 run、同步执行。
- backfill 默认按 logical date 正序执行，符合量化时序数据处理需要。
- 任意 cron 字符串先明确报错，不做不完整模拟；预设 schedule 和 `timedelta` 先稳定落地。
- `depends_on_past` 使用 StateStore 查询同一 DAG/task 之前最近的 scheduled/backfill 任务状态。

## 4. 验收标准

- `python -m pytest` 通过；
- `scheduler --once` 对到期 `@daily` DAG 创建一个 scheduled run 并执行成功；
- 重复运行 `scheduler --once` 不重复创建相同 logical date 的 scheduled run；
- `backfill create` 为闭区间 logical date 创建 backfill run；
- `depends_on_past=True` 时前一 scheduled/backfill 失败会使后一 run 的同名任务进入 `upstream_failed`；
- manual run 不参与 `depends_on_past` 判定。
