# PLAN-007 Cron 调度与 DAG 目录发现实现计划

> 日期：2026-07-07
>
> 设计依据：`docs/design/easyflow-20260706-airflow-lite.md`
>
> 前置结果：`docs/dev/PLAN-006-local-executor-concurrency-OUTCOME.md`

## 1. 目标

补齐无人值守调度的两个关键缺口：

- 支持 v0.1 设计稿要求的 5 字段 cron 字符串；
- 支持从 DAG 目录发现多个 DAG，并让 `easyflow scheduler` 能扫描目录执行。

这两项直接服务量化数据场景：用户可以把行情、财报、公告、因子等流水线放在同一目录中，由
scheduler 按各自 schedule 自动创建 DagRun，而不是人工逐个传 `--file --dag-id`。

## 2. 范围

本计划覆盖：

- 内建 cron 解析与时间区间计算；
- `scheduler_intervals`、`backfill_intervals` 对 cron 的支持；
- DAG 文件递归发现；
- 多 DAG 解析结果与解析问题收集；
- `easyflow dags list`；
- `easyflow scheduler --dags-folder ...`；
- `dags trigger` 和 `backfill create` 支持通过 DAG 目录定位 DAG；
- README 与测试更新。

本计划暂不覆盖：

- 隔离子进程解析 DAG 文件；
- 完整 DAG source bundle 版本化；
- cron 的秒字段、年份字段和自定义 Timetable；
- 交易所交易日历；
- 多机 scheduler HA。

## 3. 方案比较

### 3.1 cron 实现

方案 A：引入 `croniter`

- 优点：成熟，覆盖复杂 cron 边界更多；
- 缺点：新增运行时依赖，当前环境未安装；调度核心行为依赖第三方默认语义，v0.1 API 需要额外锁定兼容边界。

方案 B：实现轻量 5 字段 cron 解析器

- 优点：零新增依赖；语义可控；足够覆盖常见量化数据任务，如每日收盘后、每小时、工作日固定时间；
- 缺点：不能覆盖所有 cron 方言，需要明确报错而不是假装支持。

定稿：采用方案 B。支持 minute、hour、day-of-month、month、day-of-week 五字段，支持 `*`、列表、
范围、步长、月份/星期英文名。暂不支持秒、年份、`L/W/#` 等扩展语法；遇到这些语法必须明确报错。

设计理由：v0.1 的稳定性优先于功能表面积。内建语义可以被测试完整覆盖，也避免量化生产部署因为新增依赖
安装失败而不可启动。若未来需要完整 cron 方言，可在保持 `DAG(schedule="...")` 用户接口不变的前提下，
把内部解析器替换为 `croniter` 或可插拔 timetable。

### 3.2 DAG 发现接口

方案 A：只保留 `--file --dag-id`

- 优点：实现简单，定位明确；
- 缺点：不能作为生产 scheduler 入口；新增 DAG 需要修改启动命令，不适合无人值守。

方案 B：只提供 `--dags-folder`

- 优点：贴近 Airflow 使用方式；
- 缺点：破坏已有测试和手工调试路径；单文件开发体验变差。

方案 C：保留单文件入口，同时新增目录入口

- 优点：兼容当前实现；生产使用目录扫描；调试时仍可精确指定文件；
- 缺点：CLI 参数组合需要校验，内部需要统一为 `DagSource`。

定稿：采用方案 C。`--file` 继续可用；`--dags-folder` 作为目录扫描入口，默认值为 `dags`。
`scheduler` 的 `--dag-id` 从必填改为可选：传入时只调度该 DAG，不传时调度目录内所有可运行 DAG。

## 4. 接口约束

- 用户接口继续使用 `DAG(schedule="30 21 * * 1-5")`，不新增 cron 专用对象；
- DAG 作者不接触 `StateStore`、`DagSource`、解析缓存等内部概念；
- 发现目录中某个 DAG 解析失败时，不阻塞其他 DAG 被列出或调度，但必须把问题输出给 CLI；
- 发现重复 `dag_id` 时，不调度该 `dag_id`，避免同名 DAG 随机选择文件；
- 旧命令 `easyflow scheduler --once --dag-id DAG_ID --file path.py` 必须继续可用。

## 5. 验收标准

- `python -m pytest -q` 通过；
- `python -m compileall -q src tests examples` 通过；
- `DAG(schedule="0 0 * * *")` 与 `@daily` 产生一致的日级 logical date；
- 工作日 cron 能跳过周末并产生正确 data interval；
- `easyflow dags list --dags-folder DIR` 能列出多个 DAG；
- `easyflow scheduler --once --dags-folder DIR` 能调度目录内多个 DAG；
- `dags trigger` 和 `backfill create` 不传 `--file` 时能通过 `--dags-folder` 找到目标 DAG。
