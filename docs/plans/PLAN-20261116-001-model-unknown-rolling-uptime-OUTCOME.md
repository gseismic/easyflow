# 最近小时与周月按天展开结果

- 创建时间：2026-11-16 01:54 Asia/Shanghai。
- 模型：Codex（基于 GPT-6；完整模型 ID 未提供，使用 `model-unknown`）。
- 分析与验收范围：WORKTREE，基准提交 `9eece2edc1759cb2c8d96675cb0fae8fd22fa260`；本结果记录尚未提交的实现与验证。
- 计划：`docs/plans/PLAN-20261116-001-model-unknown-rolling-uptime.md`。
- 设计：`docs/design/easyflow-20261116-rolling-uptime.md`。
- 相关文件：`src/easyflow/web/uptime.py`、`src/easyflow/web/routers/catalog.py`；`web/src/uptime.js`、`web/src/components/useUptimeView.js`、`web/src/components/useUptime.js`、`web/src/components/UptimeControls.jsx`、`web/src/components/UptimeStrip.jsx`、`web/src/components/UptimeDayDetail.jsx`、`web/src/components/WorkflowUptime.jsx`、`web/src/pages/DagList.jsx`、`web/src/workspace.css`；对应测试与浏览器 checks、构建资源、README、计划/结果/设计/索引和交接文档。

## 完成情况

两个关联需求均完成：凌晨可直接看到昨晚的执行，周/月可保留上层汇总并展开当天细节。

| 内容 | 完成结果与代码依据 |
| --- | --- |
| 最近 X 小时 | 列表和工作流详情默认最近模式，初始 24 小时，可应用 1–168 的整数；当前浏览器保存小时数，刷新或切换工作流后保留。存储不可用时本页仍应用并提示未保存。见 `web/src/components/useUptimeView.js:6`、`web/src/components/UptimeControls.jsx:28`。 |
| 滚动范围 | API 新增只读 `lookback_hours` 参数，与固定起止时间及每日粒度互斥；服务端一次取 now，精确回溯 X 小时，UTC 半小时取界、首尾裁剪，最多 337 格。现有绝对范围与 DST 契约保留，等待快照显示在最近窗口末格。见 `src/easyflow/web/routers/catalog.py:66`、`src/easyflow/web/uptime.py:43`。 |
| 日期与查看稳定性 | 展示实际跨日起止时间，不再将所有窗口末端固定写成 +1日；查询 URL 保持稳定，格子及焦点使用稳定桶标识，选中格子固定那次范围，轮询不重建正在阅读的失败面板。见 `web/src/uptime.js:97`、`web/src/uptime.js:132`、`web/src/components/UptimeStrip.jsx:13`。 |
| 周/月原位展开 | 点选非未来日期，在同一图下加载当天半小时图，保持父周/月选中状态；20px 格子、2px 间距，可切日、收起及独立重试。统一刷新也更新展开日图，嵌套 Escape 先关闭内层选择。见 `web/src/components/UptimeStrip.jsx:88`、`web/src/components/UptimeDayDetail.jsx:5`、`web/src/workspace.css:162`。 |
| 失败查看 | 展开日图继续沿用失败任务、错误摘要及原始尝试日志；未来日期不请求日图，原“查看当天”仍可进入完整日视图。见 `web/src/components/UptimeStrip.jsx:79`、`web/src/components/UptimeStrip.jsx:89`。 |

没有新增依赖、数据库迁移或业务写接口，没有更改调度与原有业务权限，没有修改 GOAL。

## 测试与真实示例

- `pnpm --dir web test`：37 项通过，新增 4 项覆盖相对范围查询、小时数校验/存储降级、稳定格子标识与跨日/短窗口坐标；原有日周月和 DST 测试通过。
- `uv run pytest -q tests/test_web_recent_uptime.py tests/test_web_uptime.py tests/test_web.py tests/test_web_auth.py tests/test_web_history.py tests/test_web_project_search.py`：63 项通过，其中新增 20 个滚动范围用例；覆盖凌晨前一晚错误、精确左右边界与微秒、等待快照、1/24/168 小时、DST、输入及混用校验、认证与只读 API。仅有既有 Starlette/httpx 弃用提示。
- 针对修改的两个后端实现及两个测试文件运行 `uv run ruff check`：通过。
- `pnpm --dir web build`：61 个模块通过，最终入口为 `assets/index-49jTBR6E.js`、`assets/index-CIm2VWKM.css`。旧的已跟踪资源均保留。
- `web/checks/rolling-uptime.js`：在本机演示服务实际运行前端与 API，不使用模拟保存或 uptime 成功响应替代目标链路。管理员及真实 viewer 均通过；仅错误和慢响应场景注入故障，业务写入被拦截。
- README 示例逐个通过：凌晨默认最近 24 小时显示昨天的红格（实际 49 格），修改为 12 小时并刷新保留；切周/月选择 2026-11-15，在原位展开 48 格当天图，点击红格实际读取 RuntimeError 和第 1 次尝试原日志，再收起回到父视图。
- 真实轮询等待 5.5 秒后，服务端 as_of 推进、原失败面板为同一个 DOM 节点，日志入口仍指向原尝试；最大 168 小时可用，空值/0/169/小数不可应用。
- 日图错误重试、快速换日的慢响应、未来日无请求、顶部刷新同步日图、两层 Escape、列表逐行展开和存储拒绝后的提示/刷新默认值均通过。
- Chrome 320/390/768/1440px 无页面横向溢出；日夜主题通过并实际检查截图。图内滚动保留，日图格子高度实测 20px。
- `web/checks/navigation-stability.js` 七组通过，慢速切换采样 56 帧、最大位移 0px；`web/checks/monitoring-ui.js` 通过，图 y=351、运行选择 y=572；`web/checks/appearance.js` 通过；`web/checks/review-fixes.js` 独立上下文复跑通过。浏览器异常为 0，未触发、暂停、清理或重跑实际业务。
- `git diff --check` 通过。

实际截图保留在临时目录 `/tmp/easyflow-recent-first.png`、`/tmp/easyflow-month-inline-day.png`、`/tmp/easyflow-month-inline-mobile.png`、`/tmp/easyflow-month-inline-dark.png`。

## 复查、修正与运行状态

循环审查修正了微秒贴近半小时边界时的末格划分、滚动后的键盘焦点、统一刷新与展开日图联动，以及缺少范围元信息时的展示防御。既有浏览器脚本按新默认最近模式显式切换日视图并调整图内滚动说明，未改变产品日期行为。

发现长窗口每轮反复创建 Intl 格式器有明显开销后，改为每条图共用格式器、每格只生成一次说明。相同本机 16,850 次说明格式化的单次探针由约 3566ms 降为 127ms；这是局部探针，不等同于完整页面的性能保证。代码见 `web/src/uptime.js:146`。

一次截图写入因磁盘不足失败，清理了已确认属于本轮且结束的 pytest 临时目录后重跑；最终全部必要回归通过。没有删除其他任务目录、业务数据或仓库文件。

已用相同参数重启本机 `8790`（examples）及 `8791`（隔离演示）服务以加载新 API，均保持 `--no-scheduler`；主页面 HTTP 200，当前页面刷新即可加载新资源。服务启动不代表已触发示例运行。

最后一轮覆盖实际源码、API 边界、共享列表/详情、日图加载与收起、错误链路、说明/示例及构建资源，未再修改行为代码；本次审查范围内未发现待修复的逻辑问题。

## 遗留与交付

本轮无未完成的已授权事项，无新增需长期跟踪的已确认问题。小时数限制为 1–168，偏好仅在当前浏览器保存；本轮实测 Chrome，其他内核未单独验收。既有事项仍以 `docs/KNOWN-ISSUES.md`、`docs/TODOS.md` 为准。

结果与交接保存后执行本地 commit，不推送；实际提交号以 Git 记录及最终回复为准。
