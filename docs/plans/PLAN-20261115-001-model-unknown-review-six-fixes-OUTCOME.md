# 六项 Review 缺陷修复结果

- 创建时间：2026-11-15 01:33 Asia/Shanghai。
- 执行模型：Codex（基于 GPT-6，环境未提供完整模型 ID）。
- 范围：WORKTREE，基准提交 `d98476027e7edc68ddc8458003dfb0aafa2db5b3`，尚未提交或推送。
- 关联计划：`docs/plans/PLAN-20261115-001-model-unknown-review-six-fixes.md`。

## 完成情况

1. 定义变更：在事务内结束租约过期的任务和当前尝试，清除执行者及租约信息，保留历史尝试；有效租约存在时保留可扫描运行。依据：`src/easyflow/_internal/state_store.py:1445`、`src/easyflow/_internal/persistent_runner.py:315`。验证了已从新 DAG 删除的旧任务、有效租约随后过期，以及恢复源码后 clear 重跑成功。
2. 周调度：`@weekly` 统一为周日零点，验证 UTC、上海时区、周日之前和之后、历史游标及回补。依据：`src/easyflow/_internal/schedules.py:18`。
3. 斜杠 ID：详情和日志使用已有查询参数接口，新增查询参数清理预览与执行入口，复用旧路由的业务处理和权限；前端统一构造 URL。依据：`web/src/runApi.js:1`、`web/src/pages/DagDetail.jsx:47`、`web/src/components/ClearDialog.jsx:25`、`src/easyflow/web/routers/runs.py:39`、`src/easyflow/web/app.py:95`。验证斜杠、空格、百分号、问号、井号和中文，以及未登录、CSRF、只读权限、404、运行中清理冲突。旧路径接口仍由既有测试覆盖。
4. 日志路径：runner 初始化时固定绝对根目录。依据：`src/easyflow/_internal/persistent_runner.py:96`。验证默认及相对 log-root、sequential/local executor、初始化后切换工作目录及 CLI/Web 读取。
5. 回补边界：先归一化并验证原始输入，再按 DAG 起止边界裁剪，最后枚举并应用预览上限。依据：`src/easyflow/_internal/scheduler.py:68`、`src/easyflow/_internal/schedules.py:156`。验证请求范围跨多日但仅有 3 个有效分钟区间、无交集、输入倒置、精确时间、真实创建枚举次数、有效范围仍超限、timedelta 锚点及 DST fold。
6. 重试时间：新尝试开始时清空实例 ended_at，历史尝试的结束时间不变。依据：`src/easyflow/_internal/state_store.py:826`。

## 验证结果

- `uv run --extra dev pytest -q`：186 项通过，包含新增 14 项回归测试；有一项既有 Starlette/httpx 弃用警告。
- 将新增测试在隔离导出的基准代码上运行：13 项按预期失败，覆盖全部六项缺陷；1 项已有 DST/timedelta 行为保护测试通过。当前修复版本的 14 项全部通过。
- `pnpm test`：5 项通过，包含新增特殊 ID URL 编码测试。
- `pnpm build`：通过；更新 `src/easyflow/web/static/index.html` 和新 JS 资源，保留既有已跟踪静态资源。
- `uv run ruff check --select E4,E7,E9,F src tests examples`、新测试文件默认 Ruff 检查及 `git diff --check`：通过。
- 默认 Ruff 全量检查仍失败：基准 201 项，当前 197 项，无新增诊断；详见既有条目 `KI-20261108-2311-001`（`docs/known-issues/KI-20261108-2311-001-ruff-locked-baseline.md`）。

## 计划偏差和遗留

无实质范围偏差；回补裁剪放在共享区间函数中，以复用日期和精确时间归一化逻辑。六项 Review 均已解决，未创建长期问题或 TODO。既有 Ruff 问题保持未解决。本次未迁移历史数据库记录、未提交或推送。

相关 WORKTREE 文件：上述源码、`web/src/pages/HistoryRun.jsx`、`tests/test_review_six_fixes.py`、`web/tests/runApi.test.mjs`、静态构建产物、计划和结果文件、计划索引、既有 Ruff 条目及 KNOWN-ISSUES 索引。
