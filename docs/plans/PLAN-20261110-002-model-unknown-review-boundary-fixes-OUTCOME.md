# 第二轮 Review 边界问题修复结果

- 创建时间：2026-11-10 02:56 Asia/Shanghai。
- 执行模型：Codex，基于 GPT-6；环境未提供实际模型 ID，沿用计划文件名的 `model-unknown`。
- 分析与实施范围：`WORKTREE`，基准提交 `b7e769fc1f7fe94d8e8a76fa7c5c390505bae666`。本次代码、测试、文档和构建资源尚未提交，内容尚未固定为提交；开始时工作区与暂存区均无其他改动。
- 对应计划：`docs/plans/PLAN-20261110-002-model-unknown-review-boundary-fixes.md`。

## 完成情况

本轮 Review 的三项问题均已修复并验证：

1. 本地 Python 依赖在 DAG 加载范围内直接编译当前源码，绕过 pyc；刷新本地源码模块与父包缓存，避免同进程重复加载继续使用旧函数。依赖指纹取实际编译字节的摘要，文件在 import 期间改变时仍能阻止旧运行执行。临时 finder 退出时移除；easyflow、标准库和第三方运行库不参与刷新。
2. 领取任务的 SQLite 写事务同时检查实例并发上限和该 DagRun 的 `max_active_tasks`。LocalExecutor 提前计入既有运行中任务及本次待启动进程；有效遗留租约占用容量，任务完成或租约过期后恢复推进，其他运行不占用本运行的限额。
3. 回补开始、结束输入支持日期或带时区 ISO datetime，Web、CLI 预览及实际创建共用边界语义。日期按 DAG 时区包含整日；精确时间按实际时刻比较并包含起止端点。Web 回补弹窗提供精确时间模式，按浏览器本地时区输入并转换为 UTC 请求，切换模式清空旧输入和结果。1000 区间上限保留，高频任务可以使用更细范围分段操作。

## 关键文件及未提交范围

- 依赖加载：`src/easyflow/_internal/source_imports.py`（新增）、`src/easyflow/_internal/dag_parser.py`。
- 运行并发：`src/easyflow/_internal/state_store.py`、`src/easyflow/_internal/persistent_runner.py`、`src/easyflow/_internal/local_executor.py`。
- 回补边界和入口：`src/easyflow/_internal/schedules.py`、`src/easyflow/_internal/scheduler.py`、`src/easyflow/cli.py`、`src/easyflow/web/routers/dags.py`。
- 界面及构建：`web/src/components/BackfillDialog.jsx`、`src/easyflow/web/static/index.html`、`src/easyflow/web/static/assets/index-Cil-L0T6.js`（新增）。保留原有构建资源，未顺带清理旧资源。
- 测试：`tests/test_review_boundary_fixes.py`（新增）、`tests/test_web.py`。
- 文档：`README.md`、`docs/plans/INDEX.md`、本计划及结果文档（新增）。`GOAL.md` 未修改。

## 验证结果

- 首轮相关测试：29 项通过，覆盖依赖加载、执行器、原核心修复与 Web。
- `uv run --locked pytest -q`：172 项通过，含新增 9 项行为测试；一条既有 Starlette/httpx 弃用提示。
- 补充混合日期与精确时间边界断言后，`uv run --locked pytest -q tests/test_review_boundary_fixes.py`：9 项通过。
- `uv run --locked ruff check --select E4,E7,E9,F src tests examples`：通过；修改文件的格式检查和 `git diff --check` 通过。默认 Ruff 基线问题仍见 `docs/known-issues/KI-20261108-2311-001-ruff-locked-baseline.md`。
- `pnpm install --frozen-lockfile`、`pnpm test`（4 项）及 `pnpm build`：通过，依赖锁文件未修改。
- 使用 `playwright-cli` skill 对本机独立测试服务执行浏览器检查：Asia/Shanghai 时区输入 `2026-07-01 00:00` 至 `00:09`，请求正确转换为 `2026-06-30T16:00:00.000Z` 至 `16:09:00.000Z`，返回 10 个一分钟区间；整日超限仍明确报错，切换精度清空旧状态，无页面脚本异常。测试浏览器及临时服务已关闭。
- 自查了传递依赖与 namespace 包缓存、加载期间文件变化、并发领取竞争、有效租约和过期恢复、跨运行容量隔离、非 UTC 午夜、DST 回拨、亚秒 timedelta、无时区输入及旧日期语义。

## 计划偏差与剩余边界

实施范围与计划一致，没有未完成修复。没有新增数据库结构；日期请求和短范围预览保持兼容。源码保证仍限于已追踪的本地 Python 依赖，不冻结运行期动态导入或第三方环境；租约到期也不能撤销失联进程已有的外部副作用。其他已知问题未纳入本次修复，继续由 `docs/KNOWN-ISSUES.md` 跟踪。

本次未执行 commit 或 push。
