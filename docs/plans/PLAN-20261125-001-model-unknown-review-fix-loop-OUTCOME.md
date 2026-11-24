# EasyFlow 全量 Review 修复与复审结果

- 创建于 2026-11-25 01:03（Asia/Shanghai）。执行工具为 Codex，已知模型系列为 GPT-6，环境未提供完整模型 ID。
- 复审范围为 `d2a1585296ef2d0dd62e9888e77ee3c7b03daf4e` 至验收时的 `WORKTREE`。代码修复以 `a10a1a35b883d65208caaaf5aca5c64f1a1c0bda` 为基线。报告和提交约定已保存于 `804a28b911bcdfc976642993b9755f7c6b9f5a33`。验收时的改动包括调度和执行代码、CLI、存储和监控查询、前端代码与构建资源、回归测试和相关文档。[修复计划](PLAN-20261125-001-model-unknown-review-fix-loop.md) · [原 Review 报告](../reviews/20261125-0014-REVIEW-model-unknown-a10a1a35-all-code.md)

## 修复与回归

| 编号 | 修复和验收证据 |
| --- | --- |
| R01 | CLI 在预览和清理事务内校验当前定义。修改主文件、依赖或在事务内改动时，原 XCom、任务和运行均保留。恢复源码后原运行可以重跑。[CLI 回归](../../tests/test_state_store_cli.py#L29) |
| R02 | 跨运行依赖识别已终止运行中的未执行任务。前序任务为 `NULL` 或 `queued` 时，后续任务进入 `upstream_failed` 并释放活跃运行名额。[调度回归](../../tests/test_scheduler_reliability.py#L29) |
| R03 | 本地依赖同时记录导入路径、解析目标和源码摘要。链接切换会使缓存失效并阻止旧运行的不安全清理。目标字节相同或多个别名交换目标时，路径映射仍能识别变更。[Web 回归](../../tests/test_web.py#L113) · [符号链接回归](../../tests/test_cron_dag_discovery.py#L39) |
| R04 | POSIX worker 使用独立的进程组，租约过期、异常退出或执行取消时停止可控的子进程后再继续。验证了 Bash 后台进程、两类 CLI 中断和 macOS 进程组清理 `EPERM` 的恢复路径。[进程组回归](../../tests/test_local_executor.py#L27) · [CLI 中断回归](../../tests/test_scheduler_concurrency.py#L26) |
| R05 | 快照保存实际生效的重试次数、延迟和上限。显式传入 `None` 的任务在断租后仍遵循继承的策略，第二次尝试成功。[真实 worker 回归](../../tests/test_local_executor.py#L87) |
| R06 | 时间图和失败明细读取同一份运行级历史事件。旧库迁移、清理排队、成功重跑后均保留原失败时间，旧版重复审计得到去重。失败明细同时显示独立的任务失败和定义失配。[事件留存回归](../../tests/test_web_uptime.py#L87) · [并存失败回归](../../tests/test_web_uptime.py#L160) |
| R07 | 整日回补以本地日期边界计算实际时段。Santiago 的两次 23:30 均进入预览和执行。另覆盖跨午夜回拨、跳过整天和 23 小时历史回拨。[CLI 回归](../../tests/test_scheduler_backfill.py#L17) · [日期回归](../../tests/test_schedule_regressions.py#L28) |
| R08 | 工作流任务页核对两份响应的定义指纹。当前定义先更新时，历史任务状态与日志继续可读，当前图单独展示。[浏览器回归](../../web/checks/frontend-review-loop.js#L365) |
| R09 | 定义失配中断回补时，CLI 返回回补 ID、已创建运行结果和未处理的各段范围。用这些精确范围继续执行，已完成日期只运行一次。[CLI 回归](../../tests/test_scheduler_backfill.py#L50) · [分段回归](../../tests/test_scheduler_backfill.py#L92) |
| R10 | 每个 DAG 独立验证快照序列化。无效 `description` 只产生该 DAG 的解析问题，文件中的有效 DAG 继续发现。[解析回归](../../tests/test_cron_dag_discovery.py#L21) |
| R11 | 已入队状态用于派发扫描，领取仍在事务内检查。500 个任务中一个占用槽位、其余 499 个已入队时，扫描的 SQLite 写事务数为 0。[写事务回归](../../tests/test_local_executor.py#L145) |
| R12 | 等待格被选中后继续随轮询更新，转红时自动打开失败详情。已打开的失败格移出窗口后，原时段及所选尝试继续可读。[浏览器回归](../../web/checks/frontend-review-loop.js#L379) |
| R13 | `@dag` 拒绝异步工厂及返回异步对象的同步包装函数，避免注册空工作流。[SDK 回归](../../tests/test_dag_test.py#L57) |
| R14 | `dags test` 将 `SystemExit(0)` 按任务失败和重试策略处理。重试用尽时命令返回非零并输出失败结果，成功重试后下游正常运行。[重试回归](../../tests/test_dag_test.py#L67) · [CLI 回归](../../tests/test_dag_test.py#L90) |

## 最终复审与验证

- 在独立快照运行 `pytest -q`，376 项通过，包含 34 项新增回归。逐文件核对 193 个代码、测试和资源文件与工作区一致。测试输出含一条既有 Starlette 对 `httpx` TestClient 的弃用提示。
- `pnpm test` 的 39 项测试及 `pnpm build` 通过。浏览器回归覆盖轮询顺序、最近时段、日志、清理、导航、只读账户和 320–1440 px 窗口。本机隔离 FastAPI 环境中，定义失配后清理并重跑成功，真实浏览器仍展示原失败，并可打开重跑日志。隔离 TaskFlow 订单示例处理 3 笔订单，合计 55。[前端回归](../../web/checks/frontend-review-loop.js) · [导航回归](../../web/checks/navigation-stability.js) · [订单示例](../../examples/002_basic_taskflow_etl.py)
- 离线 wheel 构建通过。包内 43 个 Python 源文件与工作区一致，HTML 引用的 `index-DeGA4p74.js`、`index-BsNUirX_.css` 以及 35 个既有静态资源均已核对。`uv lock --locked`、`git diff --check` 和 Ruff 核心规则通过。
- 默认 Ruff 0.17.4 仍有 191 项既有诊断。与基线按相对路径、规则和消息对比，新增 0 项，减少 1 项。[Ruff 已知问题](../known-issues/KI-20261108-2311-001-ruff-locked-baseline.md)
- 终轮复查覆盖定义校验、执行和恢复、时间边界、持久化、CLI、Web API、前端轮询及打包入口。终轮未再修改应用代码，本次 Review 范围内未发现待修复的逻辑问题。进程组回归在 macOS 验证。升级前经符号链接加载依赖的旧运行可能需要重新建单。[运行边界](../../README.md#L142) · [既有问题索引](../KNOWN-ISSUES.md)
