# PLAN-008 Croniter 调度加固实现结果

> 日期：2026-07-07
>
> 对应计划：`docs/dev/PLAN-008-croniter-schedule-hardening.md`
>
> 对应设计：`docs/design/easyflow-20260706-airflow-lite.md`

## 1. 已完成

- 删除 `PLAN-007` 中的自研 cron 解析和逐分钟搜索实现；
- 新增基于 `croniter` 的内部 cron 适配层：
  - `validate_cron`；
  - `cron_next_after`；
  - `cron_next_at_or_after`；
  - `cron_previous_before`；
  - `cron_previous_at_or_before`。
- 在 `pyproject.toml` 增加运行时依赖：
  - `croniter>=6.2.3,<7`。
- `schedules.py` 改为调用 croniter 适配层；
- 增加 `validate_schedule`，并在 DAG parser 中解析 DAG 时校验 schedule；
- 保留 easyflow 用户接口不变：
  - `DAG(schedule="30 21 * * 1-5")`；
  - `@daily` 等预设 schedule；
  - `timedelta` schedule。
- 当前仍明确限制为 5 字段 cron，避免秒级表达式在分钟级 scheduler 中产生不清晰语义；
- README 已更新为 croniter-backed cron。

## 2. 行为边界

- `croniter` 只在 `_internal/cron.py` 中直接使用；
- public SDK 不暴露 `croniter` 类型；
- day-of-month 和 day-of-week 使用 cron 标准 OR 语义；
- `L`、`W`、`#` 等 croniter 支持的 5 字段扩展表达式可用；
- 6 字段秒级表达式当前会明确拒绝；
- 无效 cron 会在 DAG 解析阶段作为 parse issue 暴露，不应等到 scheduler 执行中才静默失败。

## 3. 验证结果

已执行：

```bash
python -m pytest -q
```

结果：

```text
45 passed
```

已执行：

```bash
python -m compileall -q src tests examples
```

结果：编译检查通过。

## 4. 覆盖的关键行为

- 工作日 cron 仍能跳过周末；
- cron backfill 仍包含 end date 当天的运行点；
- `0 0 L * *` 这类 croniter 扩展表达式可用；
- `*/15 * * * * *` 秒级表达式会被明确拒绝；
- 目录 DAG 发现、目录 scheduler、目录 trigger/backfill 的已有行为保持通过。

## 5. 复审结论

用户目标是生产场景，不是 Airflow 外形相似的玩具实现；因此自研 cron 解析器不应保留。当前实现把 cron
语义下沉到成熟第三方库 `croniter`，同时通过内部适配层保持 easyflow 自己的调度契约。这减少了核心代码量，
也降低了长期维护 cron 边界的风险。

下一步仍应优先做隔离子进程 DAG 解析。即使 cron 已加固，DAG import 仍在 scheduler 进程内执行，生产
环境下一个卡死或有副作用的 DAG 文件仍可能影响 scheduler。
