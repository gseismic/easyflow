# KI-20261108-2311-001：锁定的 Ruff 默认规则导致仓库检查失败

- 创建时间：2026-11-08 23:11 Asia/Shanghai。
- 创建模型：Codex，基于 GPT-6；环境未提供实际模型 ID。
- 初始状态：未解决。
- 来源：搜索与过滤体验修复任务中的开发验证；计划为 `docs/plans/PLAN-20261108-008-model-unknown-search-filter-ux.md`。
- 确认版本：`b11a3a42c4c62e08ba28cbea1d4eabb64de1487b`。同时检查了以该提交为基准的本次 `WORKTREE`；引入提交尚未定位。

## 问题、触发条件和影响

使用 `uv.lock` 中的 Ruff 0.17.4 建立开发环境后，执行 `README.md` 中的 `ruff check src tests examples` 会报告大量已有问题并以非零状态退出。由此无法用该完整命令取得通过的开发验证结果；这不表示本次 Web 运行功能失败。

## 确认依据

- 将基准提交导出到独立目录，并在独立开发环境中使用 Ruff 0.17.4 分别检查基准和本次 `WORKTREE`；两者均报告 195 项，按文件、规则及信息比较，本次没有新增项。
- Ruff 0.17.4 和本机此前被 `uv run ruff` 找到的 Ruff 0.15.6 都显示使用默认配置，但启用的规则集合不同；后者执行同一命令通过。
- 临时验证方式：显式执行 `ruff check --select E4,E7,E9,F src tests examples`，本次检查通过。这只是缩小检查规则，不代表 195 项已解决。

## 后续处理

需要项目决定固定哪些 lint 规则，并处理或明确接受相应的既有检查结果；本次未改变项目 lint 策略。

## 2026-11-08 23:16 Asia/Shanghai：补充工作区范围

- 更新模型：Codex，基于 GPT-6；实际模型 ID 未由环境提供。
- `WORKTREE` 以完整提交 `b11a3a42c4c62e08ba28cbea1d4eabb64de1487b` 为基准；本次 Ruff 对比涉及的相关未提交代码文件为 `src/easyflow/web/catalog.py`、`src/easyflow/web/service.py` 和 `tests/test_web_project_search.py`。检查范围还包括基准中的其他 `src/`、`tests/` 和 `examples/` 文件。
- 依据：对基准与当前工作区的 Ruff JSON 结果按文件、规则和信息计数，均为 195 项、无新增；状态保持未解决。

## 2026-11-15 01:33 Asia/Shanghai：六项 Review 修复后的基线复核

- 更新模型：Codex（基于 GPT-6，环境未提供完整模型 ID）。
- 状态保持未解决；本次未调整 lint 规则或处理范围外基线问题。
- 对比版本：基准 `d98476027e7edc68ddc8458003dfb0aafa2db5b3` 与本次 `WORKTREE`。相关修改为 `src/easyflow/_internal/{persistent_runner,scheduler,schedules,state_store}.py`、`src/easyflow/web/app.py`、`src/easyflow/web/routers/runs.py` 和新增 `tests/test_review_six_fixes.py`。
- 验证：使用同一个 `uv run ruff check --output-format json` 检查两份代码的 `src tests examples`，按相对文件、规则及信息计数，基准 201 项，当前 197 项，新增 0 项、减少 4 项（来自移除回补后置过滤中的重复时区转换）。默认检查仍以非零状态退出；显式 `--select E4,E7,E9,F` 检查通过，新测试文件的默认检查通过。
- 关联结果：`docs/plans/PLAN-20261115-001-model-unknown-review-six-fixes-OUTCOME.md`。修复尚未提交，没有新的修复 commit。
