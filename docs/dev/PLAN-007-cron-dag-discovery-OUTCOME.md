# PLAN-007 Cron 调度与 DAG 目录发现实现结果

> 日期：2026-07-07
>
> 对应计划：`docs/dev/PLAN-007-cron-dag-discovery.md`
>
> 对应设计：`docs/design/easyflow-20260706-airflow-lite.md`
>
> 后续修正：`docs/dev/PLAN-008-croniter-schedule-hardening.md` 已推翻本计划中“自研 cron 解析器足够
> 支撑当前阶段”的假设，当前实现改为使用 `croniter` 内部适配层。

## 1. 已完成

- 新增 5 字段 cron 解析器：
  - 支持 minute、hour、day-of-month、month、day-of-week；
  - 支持 `*`、列表、范围、步长；
  - 支持月份和星期英文名；
  - `7` 按 cron 习惯解释为 Sunday。
- 扩展 schedule 时间语义：
  - `scheduler_intervals` 支持 cron；
  - `backfill_intervals` 支持 cron；
  - `dag.test()` 和手动 DagRun 的 data interval 与正式运行使用同一套 schedule 计算。
- 新增 DAG 目录发现：
  - 递归扫描 `*.py`；
  - 收集模块顶层 `DAG`；
  - 单个文件或 DAG 解析失败时记录 issue，不阻塞其他 DAG；
  - 重复 `dag_id` 默认不调度，避免随机选择错误 DAG 文件。
- 扩展 CLI：
  - `easyflow dags list --dags-folder DIR`；
  - `easyflow scheduler --once --dags-folder DIR`；
  - `easyflow scheduler --dags-folder DIR --max-loops N`；
  - `dags trigger` 可在不传 `--file` 时通过 `--dags-folder` 定位 DAG；
  - `backfill create` 可在不传 `--file` 时通过 `--dags-folder` 定位 DAG。
- 保留旧入口：
  - `easyflow scheduler --once --dag-id DAG_ID --file path.py` 继续可用；
  - `dags trigger DAG_ID --file path.py` 继续可用；
  - `backfill create --dag-id DAG_ID --file path.py` 继续可用。
- 更新 README。

## 2. 关键取舍

- 未引入 `croniter`，而是实现轻量内建 cron 解析器。理由是 v0.1 优先控制依赖和语义边界，常见量化
  数据任务已经可覆盖，复杂 cron 方言以后可以在不改变 `DAG(schedule="...")` 用户接口的前提下替换内部实现。
- DAG 发现采用兼容式接口：生产推荐 `--dags-folder`，调试保留 `--file`。这样不破坏已有使用方式，同时让
  scheduler 可以无人值守扫描目录。
- 目录中重复 `dag_id` 视为不可调度问题。对于自动抓取数据流水线，这比随机选中某个同名 DAG 更安全。

## 3. 验证结果

已执行：

```bash
python -m pytest -q
```

结果：

```text
43 passed
```

已执行：

```bash
python -m compileall -q src tests examples
```

结果：编译检查通过。

## 4. 覆盖的关键行为

- 工作日 cron 可以跳过周末；
- cron backfill 会包含 end date 当天的运行点；
- DAG 目录能递归发现多个 DAG；
- DAG 目录中坏文件不会阻塞好 DAG 被发现；
- 重复 `dag_id` 会被识别并排除；
- `dags list` 可以列出目录内 DAG；
- `scheduler --once --dags-folder` 可以调度目录内多个 DAG；
- `dags trigger` 和 `backfill create` 可以通过 DAG 目录定位目标 DAG。

## 5. 未完成能力

仍未完成：

- 隔离子进程解析 DAG 文件；
- DAG source bundle 版本化；
- cron 秒字段、年份字段、`L/W/#` 等扩展语法；
- 交易所交易日历；
- 强制终止 OS 进程；
- 远程 Worker / 分布式队列。

## 6. 复审结论

本轮补齐了生产自动化的关键入口。easyflow 现在可以把多个量化数据 DAG 放在同一目录里，由 scheduler
按预设 schedule 或 5 字段 cron 自动创建 DagRun 并执行。对于“自动抓取数据、处理数据”的目标，当前
能力已经从单 DAG 手工调度推进到目录级无人值守调度。

下一步应优先实现隔离子进程 DAG 解析。当前解析仍在 scheduler 进程内执行，若 DAG 文件 import 阶段
卡死或有全局副作用，会影响 scheduler 稳定性。
