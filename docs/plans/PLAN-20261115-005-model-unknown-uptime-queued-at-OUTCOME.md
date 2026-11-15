# 入队时间修复与半小时 uptime 图结果

- 创建时间：2026-11-15 09:03 Asia/Shanghai。
- 实际模型：Codex（基于 GPT-6；环境未提供可核实的完整模型 ID，沿用计划中的 `model-unknown`）。
- 分析和实施范围：`WORKTREE`，基准提交 `a8708df99a4f6ea8c4c274e86dc5fc577b88fc6c`。本次修改为 `src/easyflow/_internal/{state_store,persistent_runner,local_executor}.py`、`src/easyflow/web/routers/catalog.py`、新增 `src/easyflow/web/uptime.py`、`web/src/` 的 uptime 组件、日期与路由辅助、列表/详情/历史运行页、任务详情、样式，`tests/test_{task_queued_at,web_uptime}.py`、`tests/test_{web,review_boundary_fixes}.py`、`web/tests/uptime.test.mjs`，以及 README、设计、计划、结果、索引、既有 Ruff 问题的验证事件和静态构建入口/新增资源。内容尚未固定为提交。
- 任务开始前的编号 004 计划与结果、Review 和 `docs/plans/INDEX.md` 已有改动均保留，没有提交、推送或修改 `GOAL.md`。

## 完成情况

已完成[计划](PLAN-20261115-005-model-unknown-uptime-queued-at.md)。Review 中的任务入队时间缺失已修复，按用户确认的“列表每行显示，详情展开”交付半小时 uptime 图。

- 任务依赖就绪后进入持久化队列，重复扫描保留原入队时间；最近尝试、重试到期、直接启动与清理重跑的时间语义已统一。依据：`src/easyflow/_internal/state_store.py:732`、`src/easyflow/_internal/state_store.py:873`；两个 executor 的接入位于 `src/easyflow/_internal/persistent_runner.py:425`、`src/easyflow/_internal/persistent_runner.py:557`、`src/easyflow/_internal/local_executor.py:197`。任务详情增加最近尝试说明与排队耗时，见 `web/src/components/TaskDetails.jsx:69`。
- 批量 API 返回按时间格聚合的摘要，最多 100 个工作流、25 小时，固定每格 30 分钟。优先按任务尝试的实际执行区间绘图，失败按发生时刻归格，避免重试或 clear 后将空档涂成连续执行，也不受最近 100 条历史限制。依据：`src/easyflow/web/routers/catalog.py:67`、`src/easyflow/web/uptime.py:16`、`src/easyflow/web/uptime.py:61`、`src/easyflow/web/uptime.py:95`。
- 列表每个工作流下方展示同一天的时间图，详情的运行和历史页复用完整图。支持日期、图例、加载、空、失败保留旧数据、内部横向滚动、键盘定位和触屏。依据：`web/src/pages/DagList.jsx:156`、`web/src/pages/DagList.jsx:217`、`web/src/pages/DagDetail.jsx:198`、`web/src/components/UptimeControls.jsx:4`、`web/src/components/UptimeStrip.jsx:5`。
- 红格可定位示例失败运行、具体任务及那次尝试日志；重试恢复后仍查看原失败日志，并提示任务当前状态来自最近尝试。依据：`web/src/uptime.js:62`、`web/src/components/UptimeStrip.jsx:53`、`web/src/pages/HistoryRun.jsx:21`、`web/src/pages/HistoryRun.jsx:115`。

## 交互取舍和计划调整

- 用户要求从真实场景分析后，补充当前时段黄色等待状态，区分实际执行与调度、依赖或重试等待。不推测过去未保存的等待时段。
- 经过用户明确确认后采用列表逐行图表，保留详情展开；没有新增独立监控导航。界面设计记录为 `docs/design/easyflow-20261115-uptime.md`。
- 为减少首屏占用，搜索与筛选改为可展开区域，已有条件时自动展开；搜索、过滤和地址恢复行为仍保留。依据：`web/src/pages/DagList.jsx:104`。浏览器确认 1280×720 下第一条图表起点约为 694px；更完整图表可向下滚动查看。
- 调整了一项既有回归断言：容量占满时，依赖就绪的新任务由原先 NULL 变成 queued；仍断言没有开始执行，且恢复执行后保留原排队起点。原有并发约束未放宽。

## 验证

- `uv run pytest -q`：**252 项通过**。唯一提示为环境中已有的 Starlette/httpx 弃用提示；没有测试失败。
- `pnpm test`：**9 项通过**，覆盖原有 URL 条件、新增日期与 UTC/DST 转换、特殊 ID、错误说明及失败尝试链接。
- `pnpm build`：通过，入口引用 `index-CUrGh9HF.js` 和 `index-DbANFjk1.css`。保留仓库原有静态资源，避免此次构建顺带删除旧文件。
- 核心 `ruff check --select E4,E7,E9,F src tests examples`、新增 Python 文件的默认 Ruff 和 `git diff --check` 通过。
- 默认全量 Ruff 仍有既有诊断。用同一环境对基准提交与本次 WORKTREE 按相对文件、规则和消息比较：**194 → 194，新增 0，减少 0**。不能称默认全量 Ruff 通过；已在 `KI-20261108-2311-001` 追加本次验证。
- 状态库与 API 测试覆盖容量等待、两个 executor 的依赖就绪、重试、clear、暂停、直接启动、viewer 认证与非法范围、跨午夜、整半小时/微秒边界、零耗时、长任务、多运行去重、1,205 条高频历史、23/25 小时日、原失败任务和当前等待。

## 浏览器与真实执行

使用仓库外 `/var/folders/gh/54w67t6n7_b8pv7t76xw5_qm0000gn/T/easyflow-uptime-ax5mvrw_/` 的独立 DAG、数据库、账户和日志，通过 `127.0.0.1:8791` 验证，没有使用项目业务数据库。

- 列表 4 个工作流分别有 48 格；有执行、错误、等待和无执行状态符合对应记录。`daily_sync` 为真实成功/失败运行；历史长区间和等待重试另使用隔离夹具，未将其表述为真实业务监控。
- 点选 `daily_sync` 红格生成包含 `task_id=publish&attempt=1` 的运行详情地址，自动选中 `publish` 并显示原失败日志；“入队时间（最近尝试）”非空。
- 前一天、今天、键盘 Home/End 正常；模拟 uptime API 503 后显示“当前显示旧数据”，原图保留，恢复请求后提示消失。
- 390px 宽度下页面宽度仍为 390px，图表在内部滚动。真实触屏模拟检测到 coarse pointer，格子约 44.08×44px，没有页面横向溢出。
- 浏览器真实触发 `first_workflow`，scheduler 执行两个任务；期间图表出现蓝格，运行成功后转为绿色。该次运行开始约为 2026-11-15 08:53:58 Asia/Shanghai。
- 搜索 `hourly_pipeline` 后图表减为 1 条、地址保留关键词，刷新后搜索区域自动展开且条件恢复。清除条件后恢复 4 条图表。
- 最后正常加载无新增控制台错误；验收过程中的模拟 503 与服务重启期间的连接错误均为明确测试动作。
- 截图：`/tmp/easyflow-uptime-laptop.png`、`/tmp/easyflow-uptime-desktop-final.png`、`/tmp/easyflow-uptime-failed-log.png`、`/tmp/easyflow-uptime-mobile.png`、`/tmp/easyflow-uptime-touch.png`。临时验收服务保留运行，供复查。

## 边界与遗留

- 灰格仅说明没有实际执行记录，不表示服务故障或已按计划成功；过去缺失的入队时间不推测补写。当前等待状态只标当前格，图表不计算可用率。
- 同格多运行显示计数并提供一个对应示例入口，其他运行可从工作流历史查看。没有删除过去的尝试和错误。
- 已有 Ruff 问题 `KI-20261108-2311-001` 状态仍为未解决；本轮没有新增需跨任务跟踪的已确认缺陷或延期开发事项。
- 本次改动尚未提交、未推送。

## 2026-11-15 09:51 Asia/Shanghai：提交与测试服务复核

- 执行模型：Codex（基于 GPT-6；完整模型 ID 未提供）。用户随后明确要求启动供测试并提交代码。
- 源码、测试、静态资源与本计划文档已提交：`6ac0f571d33a4de9e6c8350530926b97e1047fe7`，分支 `main`，未推送。前文“未提交”描述的是 09:03 结果创建时的快照。
- 复用仍在运行的独立演示服务 `http://127.0.0.1:8791`；页面、登录、catalog 和 uptime 查询均正常，调度器在线，`daily_sync` 返回 48 格。没有启动重复实例或切换业务数据库。
- 用户已获知默认 Ruff 的 194 项既有诊断且无新增，并在该状态下要求提交；已通过的 252 项后端、9 项前端、构建和浏览器验证对应上述代码提交，期间没有再次修改源码。
- 编号 004 的既有验证文档、Review 及其计划索引记录未混入本次提交；这些内容继续保留在工作区。
