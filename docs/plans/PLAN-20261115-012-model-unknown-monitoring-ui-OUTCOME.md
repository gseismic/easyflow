# 监控界面优化与循环复审结果

- 创建时间：2026-11-15 23:30 Asia/Shanghai。
- 执行模型：Codex（基于 GPT-6；完整模型 ID 未提供，使用 `model-unknown`）。
- 完成版本：`WORKTREE`，基准提交 `834d418c7ce4faa78fabbe80cc0450d55bd400bf`；尚未固定为提交。
- 相关未提交文件：`web/src/components/{WorkspaceNavigation,WorkflowHeader,WorkflowUptime,UptimeControls,SchedulerImpact}.jsx`、`web/src/pages/{DagList,DagDetail,HistoryList,HistoryRun}.jsx`、`web/src/workspace.css`、`web/checks/monitoring-ui.js`、构建入口及当前新增 JS/CSS、本轮设计/计划/结果和计划索引；保留计划 011 的源代码修复与导航回归。
- 关联计划：`docs/plans/PLAN-20261115-012-model-unknown-monitoring-ui.md`。
- 关联设计：`docs/design/easyflow-20261115-monitoring-density.md`。
- 状态：已完成；最后一轮完整复审未发现本次范围内待修复的逻辑问题，没有再修改应用代码。未提交、未推送。

## 实际完成

- `web/src/components/WorkflowHeader.jsx:4`：返回按钮、名称、元信息和操作合并为紧凑页头；删除装饰性眉题和独立返回行，保留列表条件和可访问名称。
- `web/src/components/SchedulerImpact.jsx:1`、`web/src/pages/DagDetail.jsx:137`：离线影响改为行内说明，管理员触发保持可见的次级操作；未知/读取失败时不使用旧数据给出确定的离线后果提示。
- `web/src/components/UptimeControls.jsx:14`、`web/src/components/WorkflowUptime.jsx:20`：时间图标题与范围控件合并，图例、单位、时区常驻，较长说明使用原生 details 按需展开；日期与前后箭头保持同组。
- `web/src/pages/DagDetail.jsx:64`、`web/src/components/WorkflowUptime.jsx:11`：页头刷新同步刷新图表；重新挂载以当前刷新版本初始化，不产生额外的重复刷新。错误时仍有局部重试。
- `web/src/components/WorkspaceNavigation.jsx:167`、`web/src/workspace.css:58`：左侧搜索保持可见并明确范围，减少辅助区高度，工作流名称由 12px 增至 13px；桌面省去“工作台”装饰行，窄屏保留标题及关闭按钮。
- `web/src/pages/DagList.jsx:70`、`web/src/pages/HistoryList.jsx:54`、`web/src/pages/HistoryRun.jsx:38`：列表、全局历史和历史运行详情采用一致的紧凑页头与卡片间距。
- `web/src/workspace.css:171`、`web/src/workspace.css:215`：运行标签与选择框减少多余换行，窄屏详情标签保持一行并在组件内滚动；保留 44px 常规操作区域及键盘可达性。

没有新增后端接口、统计口径或业务功能，没有改动 `GOAL.md`。原有错误色、时间粒度、失败尝试、筛选、运行选择与权限语义保持。设计中的监控优先级仍是场景假设，不声称来自真实使用频率统计。

## 首屏效果验收

同一真实隔离演示、Chromium、1440×900、默认日视图，等待工作流元信息完整后读取实际布局：

| 位置 | 优化前 y | 最终 y | 计划标准 | 结果 |
| --- | ---: | ---: | --- | --- |
| 时间格 | 625 | 351 | ≤450 | 通过 |
| 运行选择 | 879 | 572 | ≤640 | 通过 |
| 运行元数据 | 980 | 640 | 首行进入首屏 | 通过 |
| 左树首个工作流 | 378 | 287 | ≤290 | 通过 |

默认态的运行与时间信息进入首屏；展开说明、错误详情或极长名称时允许占用更多空间，不隐藏这些内容以满足位置指标。

## review-fix-loop 与完成标准

1. **初轮审查与实施**：覆盖页头、侧栏、共享日期/图例、列表和历史页面及其相关调用方，识别辅助区堆叠和两处刷新职责的问题；实施紧凑结构和刷新联动，进行真实页面验证。
2. **再审查和修正**：检查新结构与既有 CSS 的组合，修正运行选择器的多余换行、窄屏源码标签单独占行，并确保周期按钮有 44px 操作区域。补充历史返回和非零刷新版本下重新挂载的回归。
3. **最后完整复审**：重新检查本轮全部源文件、相关 App 路由/筛选与 usePolling 逻辑、错误与空状态、导航稳定性，以及设计/计划与验证脚本。最后一轮没有发现待修复逻辑问题，没有再修改应用代码；最终构建的界面回归、导航回归、旧 Review 回归和真实只读账户检查全部通过。

上述结论限定本次约定范围及实际验证，不表示证明整个仓库绝对没有错误。未将其他新功能或既有 KNOWN-ISSUES 纳入本轮修复。

## 验证依据

- `pnpm --dir web test`：22 项通过。
- `uv run pytest -q tests/test_web.py tests/test_web_auth.py tests/test_web_history.py tests/test_web_project_search.py tests/test_web_uptime.py`：43 项通过；有既有 Starlette/httpx 弃用提示。
- `pnpm --dir web build`：成功，55 个模块；最终构建入口为 `index-Ds3Ybu5y.js`、`index-609J5-Wt.css`。既有已跟踪构建资源保留，未借构建清理历史资源。
- `node --check web/checks/monitoring-ui.js`、`node --check web/checks/navigation-stability.js`、`git diff --check`：通过。
- `playwright-cli -s=flash --raw run-code --filename=web/checks/monitoring-ui.js`：全部通过；包括位置标准、六项图例、周期按钮尺寸、两次即时图表刷新、说明键盘操作、日周月与日期切换、红格原始 attempt=1 日志、历史返回、全部详情标签、运行/任务上下文、图表重新挂载仅一次请求、五个断点、列表过滤返回、首次失败重试、无运行、长名称和模拟只读 UI。没有 pageerror 或实际写请求。
- `playwright-cli -s=flash --raw run-code --filename=web/checks/navigation-stability.js`：七组全部通过；最终 56 帧慢速切换采样的最大位移 0px，原导航 DOM、搜索分批、项目纠正、缺失定义历史与抽屉行为保持。
- `web/checks/review-fixes.js` 在独立浏览器上下文复跑：特殊任务名、预览竞态、错误恢复、窄屏依赖图/时间图滚动和键盘边界全部通过。其清理与预览请求由原脚本拦截并返回模拟结果，没有真实清理业务数据。
- 仓库外 `/tmp/easyflow-density-viewer.js`：使用已有真实 viewer 会话确认 role，直接访问源码标签受到限制、无触发/暂停/重跑入口，窄屏日志可读且无页面横向溢出；不是仅靠客户端模拟角色的结果。

主要浏览器复跑脚本已保存在仓库 `web/checks/`。真实基准与旧 Review 包装脚本位于仓库外 `/tmp/easyflow-density-baseline.js`、`/tmp/easyflow-density-review-regression.js`；它们依赖交接文档中的隔离演示数据。

截图：优化前 `/tmp/easyflow-ui-before-desktop.png`；最终桌面 `/tmp/easyflow-monitoring-desktop.png`，最终窄屏 `/tmp/easyflow-monitoring-mobile.png`，均为实际浏览器截图。

## 交付状态与边界

无实质计划偏差。验证覆盖 Chromium 的 320/390/768/1000/1440px；其他浏览器内核未实测。密集时间格保留既有粒度、组件内滚动和键盘操作。

本机隔离服务 `http://127.0.0.1:8791` 已提供最终构建，仍为 `--no-scheduler`。未真实触发、暂停、清理或回补运行，未修改默认业务数据库。

本轮没有新增结束后仍需跟踪的确认缺陷；`docs/KNOWN-ISSUES.md` 和 `docs/TODOS.md` 未改动。工作区保留计划 011 和本轮成果，尚未提交、未推送，原有 10 个未推送提交保持。
