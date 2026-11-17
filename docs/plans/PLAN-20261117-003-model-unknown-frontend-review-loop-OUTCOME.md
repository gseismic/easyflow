# 前端 Review 修复与循环复审结果

- 创建时间：2026-11-17 10:03 Asia/Shanghai。
- 执行模型：Codex（基于 GPT-6；完整模型 ID 未由环境提供，使用 `model-unknown`）。
- 完成版本：`WORKTREE`，基准完整提交 `e4ca538ddcf97510846dcfdfbd4a839627429c65`，分支 `main`；未提交或推送。下述代码、测试、版本与构建资源尚未固定为提交。
- 计划：[前端 Review 修复与循环复审](PLAN-20261117-003-model-unknown-frontend-review-loop.md)。来源：[已有全量前端报告](../reviews/20261117-0820-REVIEW-model-unknown-e4ca538d-frontend.md)，保留原文，其“未修复”结论对应当时版本。

## 完成内容

| 问题 | 修复与代码依据 | 验收证据 |
| --- | --- | --- |
| R1：合法点号 DAG 错寻址 | `web/src/dagApi.js:1` 统一使用查询参数；`web/src/pages/DagDetail.jsx:28`、`web/src/pages/HistoryRun.jsx:14`、源码/触发/回补均使用 helper。`src/easyflow/web/routers/dags.py:60` 增加同处理函数的五个查询别名，`src/easyflow/web/app.py:93` 注册；旧路径接口保留。 | 新增真实 API 回归覆盖 `.`、`..`、`detail`、`demo` 的详情、源码、特殊 run_id、暂停、触发、容量冲突、回补预览、旧路由，以及认证、角色、CSRF 和跨来源拒绝。浏览器实际完成三个特殊 ID 的主要操作与历史入口，无白屏。 |
| R2：旧触发响应导航 | `web/src/components/TriggerDialog.jsx:10` 在关闭及卸载时使操作失效，响应成功、错误及结束阶段均检查有效性；`:44` 明确关闭不会取消已提交运行。 | 独立浏览器验证关闭窗口、Esc、右上角关闭、直接卸载后延迟响应不改 hash，设置草稿 `06:13` 保留；重开后的参数不被旧响应覆盖，有效请求失败后可重试，正常触发仍选中新运行。 |
| R3：重跑预览无法刷新 | `web/src/components/ClearDialog.jsx:17` 请求失败后清除旧快照，`:25` 刷新前失效，`:77` 始终保留预览入口；服务端冲突保护未改。`web/src/index.css:1014` 允许该弹窗操作区换行。 | blocked 后同范围刷新、409 后必须重预览、刷新失败后不可提交旧快照、请求中范围锁定、切换范围失效及按新快照确认均通过。320/390/768/1440px 所有操作按钮都位于弹窗与视口内。 |

版本 `0.1.2 → 0.1.3`，同步 `pyproject.toml`、`uv.lock`、`web/package.json`；未增加依赖。最终发布入口引用 `index-CL0aI4u-.js`、`index-BsNUirX_.css`，两项文件存在，既有已跟踪哈希资源保留。本轮未引用的中间构建文件已移除。

新增回归：`tests/test_web_workflow_addressing.py`、`web/tests/dagApi.test.mjs`、`web/checks/frontend-review-loop.js`。既有 `navigation-stability.js`、`review-fixes.js`、`monitoring-ui.js` 的详情模拟寻址随新接口同步。

## 实际验证

| 检查 | 结果 |
| --- | --- |
| `uv run pytest -q` | **298 项通过**，170.31 秒；1 项已有 Starlette/httpx 弃用提示。包含 5 项新增工作流寻址与权限回归。 |
| `pnpm test`（`web/`） | **38 项通过**，包含新增浏览器 URL 解析契约检查。 |
| `pnpm build --emptyOutDir false`（`web/`） | 通过，62 个模块；最后一次构建包含弹窗换行修复。 |
| 路由及新测试文件默认 Ruff | 通过。 |
| 三个修改 Python 文件核心 Ruff（`--select E4,E7,E9,F`） | 通过。`app.py` 默认 Ruff 与同一基准均有 1 条 `I001`，新增 0，不能描述为默认检查通过；见 `KI-20261108-2311-001`。 |
| `uv lock --locked`、三处版本一致性、静态入口文件存在性、`git diff --check` | 通过。前端 pnpm 锁文件不包含项目自身版本，无需修改。 |
| 最终 `web/checks/frontend-review-loop.js` | 四个 ID 的全部六标签及管理寻址、过期响应、预览恢复、四种宽度及 viewer 均通过，pageerror 为 0，所有 API 都在独立上下文模拟。 |
| 最终 `web/checks/navigation-stability.js` | 七组回归通过：慢加载/重复点击、快速切换、错误重试、搜索分页、项目归属纠正、移除定义后的历史、viewer 与移动抽屉；57 个采样中最大位移 0px。 |
| 真实隔离 Web 场景 | `.`、`..`、`detail` 的详情、暂停、触发、源码指纹、两日回补预览、blocked 清理预览刷新、历史返回，以及黑夜设置刷新和四宽度均通过，pageerror 为 0。临时库确认 3 次运行均为 queued、参数正确，任务尝试数为 0。 |

真实 Web 使用 `/tmp/easyflow-review-loop-q7jX3r/` 的独立 DAG、数据库、日志与专用测试账户，端口 8794，未启动 scheduler。全部写操作仅作用于临时库。验收后已关闭本轮 `revfix` 浏览器及临时服务，未停止、重启或操作用户原有业务 Web/调度器；临时凭据和浏览器状态不加入 Git。

## 循环复审与范围

1. 第一轮逐条核实 R1–R3，完成修复与自动测试，定向浏览器和既有导航回归通过。
2. 重新检查相邻交互与 320px 弹窗时发现新增第三个按钮使“取消”左边界到达 -26px；添加仅用于清理弹窗的换行规则，增强按钮边界断言并重新构建。修正了真实场景脚本对 pending 任务清理资格的错误假设，以及 Playwright CLI 不提供全局 `URL` 的夹具问题；这些属于验收修正，不修改服务端合法状态规则。
3. 最后一轮只读复审覆盖 `web/src/` 全部 **49 个文件**（含新增 helper 和三份样式）、入口与构建配置、测试/检查及必要后端处理函数。复查会话/CSRF、轮询与过期响应、全部 7 页、导航搜索分页、工作流操作、历史/尝试日志、时间图及 DST、失败原位展开、设置和存储降级。对最终构建重复执行定向和导航浏览器回归，复核差异、版本、静态引用及临时库状态；这一轮没有再修改应用代码，未发现本次范围内待修复的逻辑问题。

## 边界与交付状态

- 已完成本次修复闭环。原报告三项不再是当前 WORKTREE 的待修复问题，不为任务内已解决事项另建长期条目。
- 既有 Ruff 问题仅追加本轮验证证据；其他 KNOWN-ISSUES/TODOS 不在本次范围。没有新增未解决的相关逻辑问题。
- 本轮浏览器内核为 Chromium；没有执行 Firefox/WebKit、真实触屏或 scheduler 驱动的完整业务端到端场景。没有原样执行其他依赖专用演示夹具的旧浏览器脚本，也没有重做真实跨分钟主题切换；不引用其历史通过结果作为本轮证据。
- 新前端需要本轮后端查询路由。**已有 Web 进程需重启后再刷新页面**；仅刷新浏览器不会让旧 Python 进程加载新 API。本轮未部署或重启用户业务服务，不能把临时环境通过描述为业务进程已更新。
- 相关未提交文件：上述源码、三个测试/检查新增文件、三份既有检查、三个版本文件、静态入口及两个新资源、本计划/结果、计划索引、HANDOFF、KNOWN-ISSUES 索引与既有 Ruff 详情。开始时已有未跟踪报告完整保留。`GOAL.md`、业务数据、设计图片均未修改；没有 commit 或 push。

## 收尾核对

2026-11-17 10:05 Asia/Shanghai，Codex（完整模型 ID 未提供）：核对全部 49 个前端源文件及 5 个关键关联文件的 SHA-256，均与最终复审/验收时一致；105 个本地 Markdown 链接有效，差异检查通过，`GOAL.md`、`AGENTS.md` 和 pnpm 锁文件无差异。临时服务会话已正常退出，8794 无监听。当前工作区仅包含上述成果及开始时已有报告，修复闭环无待继续步骤。
