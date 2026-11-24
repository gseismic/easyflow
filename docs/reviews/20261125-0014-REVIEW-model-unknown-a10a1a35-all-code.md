# EasyFlow 全量代码 Review 报告

- 创建于 2026-11-25 00:14（Asia/Shanghai）。整理工具为 Codex，已知模型系列为 GPT-6，完整模型 ID 与原审查模型 ID 均未提供。
- 审查范围为 `d2a1585296ef2d0dd62e9888e77ee3c7b03daf4e..a10a1a35b883d65208caaaf5aca5c64f1a1c0bda` 的累计代码，行号对应审查终点。
- 原审查发现 14 项问题，包括 2 项 P1、9 项 P2 和 3 项 P3。原审查的 342 项后端测试、38 项前端测试、隔离构建及既有浏览器回归均通过。补充验证复现了以下缺陷，本文的复现数据来自原审查。

## P1

### R01　CLI 清理前校验历史运行的 DAG 定义

修改 DAG 主文件或本地依赖后，`tasks clear --dry-run` 仍会列出旧任务，`--yes` 会删除原 XCom 并以 0 退出。调度器随后因指纹失配拒绝重跑，原结果已经丢失。CLI 应在预览和清理事务内均校验当前定义。失配时应保留原运行、任务和 XCom，恢复原定义后应能重跑。[清理命令](../../src/easyflow/cli.py#L329) · [CLI 重跑约定](../../README.md#L121)

### R02　把终态运行中无法执行的前序任务判为失败依赖

前一次定时运行在领取任务前因定义变更而失败时，会留下 `NULL` 或 `queued` 的任务。后续启用 `depends_on_past` 的运行一直等待该任务，可能占满唯一的活跃运行名额。判定前序任务是否仍可运行时应考虑其运行状态，明确已终止的未完成前序任务为失败依赖。[前序任务查询](../../src/easyflow/_internal/state_store.py#L589) · [跨运行依赖规则](../design/easyflow-20260706-airflow-lite.md#L998)

## P2

### R03　检测本地依赖符号链接更换目标

本地导入器仅保存 `resolve()` 后的目标。部署切换依赖链接并保留旧目标时，缓存继续校验旧目标。原审查中 Web 能创建随后因指纹失配失败的运行，也能清理无法重跑的旧运行。应记录原导入路径，复核当前解析目标及源码。[本地导入器](../../src/easyflow/_internal/source_imports.py#L65) · [依赖校验约定](../../README.md#L142)

### R04　回收 worker 时停止它启动的 Bash 进程

`BashOperator` worker 租约到期后，回收只杀死 Python worker。原审查复现了第二次尝试完成后第一次 shell 仍在写入文件。应在允许再次执行前终止执行器可控制的本地进程组。[回收路径](../../src/easyflow/_internal/local_executor.py#L135) · [Bash 执行](../../src/easyflow/providers/standard/operators/bash.py)

### R05　DAG 快照保留实际生效的重试设置

DAG 设有 `default_args={"retries": 1}` 且任务传入 `retries=None` 时，worker 会使用 DAG 默认值，快照恢复后却失去该设置。普通异常可重试，租约过期则直接失败。快照应保存实际生效的次数和延迟。[任务快照](../../src/easyflow/_internal/dag_parser.py#L520)

### R06　保留清理重跑前的运行级失败事件

定义失配且没有失败 `task_try` 时，时段图和失败明细只依赖当前 `dag_run.state` 与 `end_date`。恢复源码、清理并重跑会覆盖这两个值，历史红格和明细随之消失。应保存运行级错误及发生时间，供两种查询持续使用。[时段查询](../../src/easyflow/web/uptime.py#L191) · [错误保留约定](../../README.md#L32)

### R07　整日回补纳入结束日重复的最后一小时

结束日使用默认 `fold=0` 的 `time.max` 时，夏令时回拨会漏掉第二次出现的最后一小时。原审查在 `America/Santiago` 的 2026-04-04 观察到两个有效的 23:30 调度时刻，预览和回补仅包含第一次。结束边界应使用次日的当地日期边界。[日期边界](../../src/easyflow/_internal/schedules.py#L241) · [整日回补约定](../../README.md#L132)

### R08　工作流任务页独立核对历史与当前的定义指纹

当工作流详情先收到新定义且运行详情仍报告旧的 `definition_matches=true` 时，任务页会把新定义的上下游与旧运行状态混在一起。应比较两份响应的指纹。发现不一致时显示提示，保留历史状态与日志，并停用当前定义的任务元数据。[任务页](../../web/src/pages/DagDetail.jsx#L254) · [历史详情](../../web/src/pages/HistoryRun.jsx)

### R09　回补因定义变化中断时报告进度和剩余区间

长批次中途修改 DAG 或依赖后，下次建单抛出 `DagSourceChanged`。原审查复现首日已写入状态库，CLI 却只报告定义变化，缺少回补 ID 和剩余范围。原区间用 `completed` 重试可能重复执行已完成日期。应保留建单保护并返回已有结果和首个未处理日期。[回补建单](../../src/easyflow/_internal/scheduler.py#L571) · [中断约定](../../README.md#L132)

### R10　将序列化错误隔离到对应 DAG

同一源码文件中，一个 DAG 的 `description` 等元数据无法 JSON 序列化时，错误会在解析进程汇总快照时抛出，导致有效 DAG 的快照也被丢弃。应在逐 DAG 的异常处理里校验序列化，并为出错定义记录解析问题。[快照写入](../../src/easyflow/_internal/dag_parser.py#L634) · [发现隔离约定](../dev/PLAN-007-cron-dag-discovery-OUTCOME.md#L26)

### R11　跳过已入队任务的重复写事务

全部槽位占用时，执行器仍逐次写入已入队的任务。原审查用 500 个独立任务复现了零派发却申请 499 次 SQLite 写事务的情况，重复写锁与续租争用。应复用观察到的已入队状态，保留领取事务的校验。[执行器派发](../../src/easyflow/_internal/local_executor.py#L207)

## P3

### R12　最近窗口的已选等待格随轮询更新

选中等待格后，轮询若将该格更新为失败，按钮会变红且仍标为展开，但详情使用选中时的旧数据。应在选中格变为失败前使用实时格子，并继续固定已打开的失败详情。[时段组件](../../web/src/components/UptimeStrip.jsx#L13) · [选中约定](../design/easyflow-20261116-rolling-uptime.md#L22)

### R13　明确拒绝异步 `@dag` 工厂

`async def` 经 `@dag` 装饰后，调用产生未运行的协程，却仍返回空 DAG。原审查中 `dag.test()` 报成功，工厂内任务均未注册或执行。应明确拒绝异步工厂。[DAG 工厂](../../src/easyflow/sdk/dag.py#L245)

### R14　任务的 `SystemExit(0)` 按任务失败及重试策略处理

测试任务调用 `sys.exit(0)` 时，退出异常会穿过任务重试逻辑。`dags test` 因而以 0 退出，未输出 DAG 结果或执行下游。进程内测试应与持久化执行一致，记录任务失败并尝试适用的重试。[进程内重试](../../src/easyflow/sdk/testing.py#L224)
