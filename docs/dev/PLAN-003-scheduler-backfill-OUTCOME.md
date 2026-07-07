# PLAN-003 调度与 Backfill 实现结果

> 日期：2026-07-06
>
> 对应计划：`docs/dev/PLAN-003-scheduler-backfill.md`
>
> 对应设计：`docs/design/easyflow-20260706-airflow-lite.md`

## 1. 已完成

- 新增 schedule 计算模块：
  - `@once`
  - `@hourly`
  - `@daily`
  - `@weekly`
  - `@monthly`
  - `@yearly`
  - `timedelta`
- 新增一次性 scheduler：
  - `run_scheduler_once(...)`
  - `easyflow scheduler --once --dag-id DAG_ID --file PATH`
- 新增 backfill：
  - `create_backfill(...)`
  - `easyflow backfill create --dag-id DAG_ID --file PATH --start-date DATE --end-date DATE`
- 扩展 StateStore：
  - scheduled run 按 `(dag_id, logical_date)` 去重；
  - backfill run 使用独立 `backfill_id`；
  - 查询同一 DAG/task 的前序 scheduled/backfill TaskInstance 状态；
  - 记录 backfill 元数据。
- 实现 `depends_on_past` 跨 DagRun 子集：
  - scheduled/backfill 之间生效；
  - manual run 默认隔离；
  - 前序失败时当前 TaskInstance 进入 `upstream_failed`。
- 更新 README，补充 scheduler/backfill 用法和限制。

## 2. 验证结果

已执行：

```bash
python -m pytest -q
```

结果：

```text
25 passed
```

已执行：

```bash
python -m compileall -q src tests examples
```

结果：编译检查通过。

已执行 scheduler/backfill CLI smoke：

```bash
PYTHONPATH=src python -m easyflow.cli scheduler --once ...
PYTHONPATH=src python -m easyflow.cli backfill create ...
```

结果：scheduled run 和 backfill run 均创建并执行成功。

## 3. 覆盖的关键行为

- `scheduler --once` 对 `@daily` DAG 创建最近一个已结束数据区间；
- 重复 `scheduler --once` 不重复创建相同 scheduled logical date；
- `backfill create` 为闭区间 logical date 创建多个 backfill run；
- `depends_on_past=True` 能阻止前一 backfill 失败后的后一业务区间任务继续运行；
- manual run 失败不会阻塞后续 scheduled run；
- 任意 cron 字符串暂不模拟，明确留到后续实现。

## 4. 未完成能力

仍未完成：

- 常驻 scheduler 循环；
- scheduler lease；
- task lease 和 heartbeat 恢复；
- LocalExecutor 多进程；
- 任意 cron 表达式；
- max_active_runs / max_active_tasks 调度限流；
- `tasks clear` 审计重跑；
- DAG definition hash 不一致后的恢复拦截；
- DAG 文件隔离子进程解析。

## 5. 复审结论

本轮实现了 M2 的核心可验证路径：定时 run 创建、历史区间 backfill 和跨 DagRun 顺序依赖。
这已经能支撑量化数据按日或按小时自动抓取、历史回补和前序失败阻断后序处理。

下一步应优先补 scheduler lease、常驻循环、task heartbeat 和 orphaned task 恢复，这些能力决定
进程重启后的完整生产稳定性。
