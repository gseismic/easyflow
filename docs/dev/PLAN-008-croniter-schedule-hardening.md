# PLAN-008 Croniter 调度加固计划

> 日期：2026-07-07
>
> 设计依据：`docs/design/easyflow-20260706-airflow-lite.md`
>
> 前置结果：`docs/dev/PLAN-007-cron-dag-discovery-OUTCOME.md`

## 1. 背景判断

`PLAN-007` 的轻量内建 cron 解析器可以验证目录级调度，但不应作为生产 cron 引擎。它缺少足够多的
真实边界覆盖，例如：

- cron 方言兼容；
- day-of-month 和 day-of-week 组合语义；
- 闰年、月份边界和稀疏表达式；
- DST 和 timezone 相关边界；
- 非法表达式诊断；
- 长距离搜索性能。

因此，“零依赖可控”这个假设在生产目标下不成立，应被推翻。

## 2. 目标

- 使用 `croniter` 替代自研 cron 解析和搜索；
- 保持用户接口不变：`DAG(schedule="30 21 * * 1-5")`；
- easyflow 明确定义内部调度语义，不把第三方库对象暴露给 DAG 作者；
- 增加 croniter 能力边界测试，防止后续升级改变核心时间语义。

## 3. 方案比较

方案 A：继续完善自研 cron

- 优点：依赖少，可完全控制实现；
- 缺点：需要长期维护大量 cron 边界，生产风险高；容易变成“不完整但看起来可用”的隐患。

方案 B：直接在调度代码各处调用 `croniter`

- 优点：改动小；
- 缺点：第三方库调用散落，`day_or`、秒字段、返回类型等关键参数容易不一致，未来难以审计。

方案 C：新增内部 cron 适配层，统一封装 `croniter`

- 优点：用户接口稳定；内部契约集中；便于测试和未来替换；第三方语义变化时影响面可控；
- 缺点：比直接调用多一层代码，但这是生产系统应该支付的复杂度。

定稿：采用方案 C。

## 4. 接口和语义

- 用户接口仍为 `DAG(schedule: str | timedelta | None)`；
- `@daily` 等预设 schedule 仍走 easyflow 自己的显式语义；
- 非 `@` 开头的字符串视为 croniter cron 表达式；
- 默认使用 cron 标准的 day-of-month/day-of-week OR 语义；
- 通过 `croniter.is_valid()` 做解析期验证；
- `latest_ended_interval(cron, now)`：
  - 找到 `now` 之前或等于 `now` 的最近一个 fire time 作为 interval end；
  - 再找 end 之前的 fire time 作为 logical date 和 interval start；
- `interval_for_logical_date(cron, logical_date)`：
  - logical date 必须是一个 fire time；
  - interval end 为下一个 fire time；
- `backfill_intervals(cron, start_date, end_date)`：
  - 从 start date 当天 00:00 起找第一个 fire time；
  - 包含 logical date 落在 end date 当天 23:59:59.999999 之前的所有 fire time。

## 5. 依赖策略

- 运行时依赖增加 `croniter>=6.2.3,<7`；
- 不在公共 API 暴露 `croniter` 类型；
- 后续若要生产部署，应再引入 lock 文件或部署镜像固定依赖版本。

## 6. 验收标准

- `python -m pytest -q` 通过；
- `python -m compileall -q src tests examples` 通过；
- 工作日 cron 仍能正确跳过周末；
- `L` 等 croniter 支持的表达式可用；
- 非法 cron 在 DAG 解析或调度阶段明确失败；
- croniter 适配层集中在 `_internal/cron.py`。
