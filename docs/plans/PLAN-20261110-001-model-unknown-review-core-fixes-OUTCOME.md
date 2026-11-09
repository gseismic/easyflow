# 核心 Review 问题修复结果

- 创建时间：2026-11-10 01:28 Asia/Shanghai。
- 执行模型：Codex，基于 GPT-6；环境未提供实际模型 ID，沿用计划文件的 `model-unknown`。
- 分析与实施范围：`WORKTREE`；基准提交 `fed44169741820536846a34e32512726705e1008`。本次代码、测试、README、计划及结果文档尚未固定为提交；开始时工作区无其他未提交或未跟踪改动。
- 对应计划：`docs/plans/PLAN-20261110-001-model-unknown-review-core-fixes.md`。

## 完成情况

五项 Review 问题已修复并覆盖行为测试：

1. 定时调度仅使用已创建的 scheduled 运行和实际扫描过的回补日期作为游标。未来回补不会越过未到期区间；已回补日期不重复创建 scheduled 运行；连续 1000 个已回补区间后仍可继续调度。Web 批量工作流信息使用相同游标语义。
2. clear 已完成运行时按普通运行或对应 backfill ID 计量容量。新建 backfill 运行保存其创建时上限；旧运行从 backfill 元数据读取。其他组的活跃运行不再错误阻止清理。
3. `task_ids=[]` 的预览和执行均返回空结果，不改变 DagRun、任务或审计记录；`None` 保持全选语义。
4. 回补预览生成上限为 1000 个区间；超限由 CLI 返回退出码 2、Web 返回 400。范围内运行状态一次读取，不再逐区间初始化数据库；实际创建仍在事务内重新判断资格。
5. 长运行 ID 的日志目录改为固定长度哈希名，短 ID 保留原路径。日志路径无法创建时，任务及 DagRun 仍落下失败状态；本地执行器启动失败的日志写入也不会阻止失败记录。

## 计划偏差与验证

- 自查发现直接调用 `create_dag_run()` 时回补上限未必已写入 `backfill` 表，因此补充 DagRun 回补上限列；`-1` 表示创建时不设上限，`NULL` 表示旧运行。此更改在实施前补入计划。
- Web 已有统一 `ValueError` 处理，无需新增路由层异常分支；通过 Web API 测试确认超限返回 400。
- `uv run pytest -q`：163 项通过，1 条既有 Starlette/httpx 弃用提示。
- `uv run ruff check --select E4,E7,E9,F src tests examples`：通过；涉及本次代码的格式检查及 `git diff --check` 通过。默认 Ruff 全规则的既有基线问题见 `docs/known-issues/KI-20261108-2311-001-ruff-locked-baseline.md`，本次未改变 lint 配置。

## 遗留事项

本次五项缺陷没有未完成修复。新增 SQLite 表和列由 `init_db()` 非破坏性建立；未删除旧数据。结果当前为 `WORKTREE`，若后续提交，以实际 Git 记录为准。
