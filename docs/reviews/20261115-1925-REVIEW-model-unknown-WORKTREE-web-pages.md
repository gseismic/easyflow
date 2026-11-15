# easyflow Web 页面浏览器 Review

- 创建时间：2026-11-15 19:25 Asia/Shanghai。
- 创建者：Codex；运行环境未提供可核实的完整模型 ID，标记为 `model-unknown`。
- 分析范围：`WORKTREE`。应用源码基线为完整提交 `3d4b58db81336bf30a930c7d7cc676a2fbba71c5`；本次新生成的截图尚未提交，保存在 `docs/reviews/assets/20261115-web/`。工作区当时没有已修改的应用源码。
- 浏览器环境：Playwright CLI 0.1.13、Chrome；桌面视口 1440×1000，窄屏视口 390×844。
- Web 环境：`http://127.0.0.1:8791`，使用仓库外既有的隔离演示数据库和示例 DAG，启动参数为 `--no-scheduler`。未使用默认业务数据库，未执行触发、暂停、清理或回补等会改变运行数据的操作。

**2026-11-15 19:49 Asia/Shanghai 更新**：按用户授权执行 `review-fix-loop`，四项代码 Review 的 P2 和下述浏览器巡查 P3 均已修复。最新结果仍为以 `3d4b58db81336bf30a930c7d7cc676a2fbba71c5` 为基准的 `WORKTREE`，尚未提交。最后一轮复查未发现本次 Web 页面及关联逻辑范围内待修复的逻辑问题。详细依据见 [修复复核报告](20261115-1925-REVIEW-model-unknown-WORKTREE-web-pages-review-verification-model-unknown.md)。以下保留首次巡查证据和 19 张原始截图，截图反映修复前页面。

## 检查范围与结果

浏览并截图了登录页、工作流列表、工作流概览、任务、工作流运行历史、日志、调度详情、源码、全局运行历史、失败运行详情，以及移动端列表、导航抽屉和任务页。另验证了运行时段日/周/月切换、工作流搜索、失败时段详情和“查看完整日志”跳转。

- 登录成功；登录后页面均正常加载，未发现页面级 `alert` 或非预期浏览器控制台错误。新会话未登录时 `/api/auth/session` 返回一次 `401`，登录成功后未再次出现；这是鉴权探测，不作为缺陷。
- 搜索 `daily_sync` 后只显示匹配的工作流，清除搜索后列表恢复。
- 日、周、月视图分别呈现 48 个半小时格、7 个日格和 30 个日格；错误时段可打开失败详情，并跳转至 `publish` 第 1 次尝试的日志。
- 390px 视口下，文档没有整体水平溢出；时间图和任务依赖图各自在内部横向滚动。
- 演示服务按既有配置关闭了 scheduler，页面显示“调度器离线”。因此本次未验证调度器在线时的真实执行、排队和触发结果；这是测试环境限制，不作为 Web 缺陷。

## 首次巡查发现

本节现象和行号对应上述应用源码基线；当前实现及验证依据见后文修复结果。

### P3：窄屏时间图和任务依赖图没有提示可横向查看

- **触发条件**：在约 390px 宽的屏幕查看工作流列表或工作流任务页。
- **现象与证据**：工作流列表的运行时段区域宽 316px、内容宽 584px；截图中只显示到约 12:00，剩余时间格需要横向滑动。任务页依赖图区域宽 348px、内容宽 496px，第二个任务节点只显示一部分。对应截图：[移动端工作流列表](assets/20261115-web/11-mobile-workflow-list.png)、[移动端任务页](assets/20261115-web/13-mobile-workflow-tasks.png)。
- **代码依据**：运行时段容器在 `web/src/index.css:105-117` 使用 `overflow-x: auto`；任务依赖图在 `web/src/components/Graph.jsx:22-23` 使用固定宽度内容，并在 `web/src/index.css:790-796` 设置内部滚动。触屏样式把每个半小时格的最小宽度设为 48px（`web/src/index.css:310-313`）。
- **影响**：数据仍可通过横向滑动查看，但当前页面没有滑动提示、边缘渐隐或可见滚动条线索，用户可能误以为时间轴只覆盖部分时段，或 DAG 只显示部分节点。该问题降低了移动端核心可视化内容的可发现性，不影响桌面布局。
- **建议**：在窄屏图表旁增加“左右滑动查看全天/完整依赖图”提示，或提供更紧凑的移动布局；同时保留触屏目标尺寸和键盘可访问性。

19:25 首次巡查只报告问题，没有修改应用代码。随后按用户授权修复，处理结果如下；已解决的一次性问题保留在本报告及计划中，不重复建立长期条目。

## 修复结果（2026-11-15 19:49 Asia/Shanghai）

| 优先级 | 问题与影响 | 处理及当前代码依据 |
| --- | --- | --- |
| P2 | 预览全部任务期间改选当前任务，旧响应导致实际清理范围与界面不符 | 请求期间锁定范围，切换后仍清空预览；`web/src/components/ClearDialog.jsx:41`。延迟预览、失败解锁、确认请求体浏览器验证通过。 |
| P2 | 时段聚合逐条转换不重叠旧记录，历史增长后前端超时 | 新增结束时间范围索引，先粗筛再精确计算；由工作流 ID 驱动查询，保留跨日及未结束尝试；`src/easyflow/_internal/state_store.py:158`、`src/easyflow/web/uptime.py:105`。75 万条历史的空日查询从约 32.9 秒降为约 25 毫秒。 |
| P2 | 新定义包含 `__proto__` 而历史运行缺少该任务，依赖图读取继承属性并白屏 | 仅读取自有状态属性，缺失状态显示未知；`web/src/components/Graph.jsx:76`。三种特殊任务名的缺失/自有状态实际渲染均通过。 |
| P2 | 午夜 DST 跳变导致次日多算一小时，周/月可能返回 400 | 独立构造各目标日期边界，后端同时处理日期自身跳过午夜；`web/src/uptime.js:23`、`src/easyflow/web/uptime.py:60`。Santiago 日图 46 格、周/月正常返回；Asuncion 月初跳变也通过。 |
| P3 | 窄屏图表缺少横向滑动线索 | 实际溢出时显示提示并支持容器键盘聚焦，尺寸变化后重新判断；`web/src/components/HorizontalScroll.jsx:7`。390px 内部滚动、键盘到最后一格及桌面隐藏提示均通过。 |

验证：265 项后端测试、22 项前端测试、前端构建及浏览器复查通过。默认全量 Ruff 仍有 194 项既有诊断，与基线逐项比较无新增；核心规则及 uptime/相关测试默认检查通过。调度器在线执行仍不在本次隔离浏览器验收范围内，性能数字为所述夹具测量，不代表所有数据规模的时延保证。

## 截图索引

所有截图均为本次浏览器会话生成；页面数据来自隔离演示环境。

| 页面 / 状态 | 截图 |
| --- | --- |
| 登录页 | [01-login.png](assets/20261115-web/01-login.png) |
| 工作流列表（完整页面） | [02-workflow-list-full.png](assets/20261115-web/02-workflow-list-full.png)；[首屏](assets/20261115-web/02-workflow-list.png) |
| 工作流概览 | [03-workflow-overview.png](assets/20261115-web/03-workflow-overview.png) |
| 工作流任务 | [04-workflow-tasks.png](assets/20261115-web/04-workflow-tasks.png) |
| 工作流运行历史 | [05-workflow-history.png](assets/20261115-web/05-workflow-history.png) |
| 工作流日志 | [06-workflow-logs.png](assets/20261115-web/06-workflow-logs.png) |
| 调度详情 | [07-workflow-schedule.png](assets/20261115-web/07-workflow-schedule.png) |
| 工作流源码 | [08-workflow-source.png](assets/20261115-web/08-workflow-source.png) |
| 全局运行历史（完整页面） | [09-global-history-full.png](assets/20261115-web/09-global-history-full.png)；[首屏](assets/20261115-web/09-global-history.png) |
| 失败运行详情 | [10-run-detail-failed.png](assets/20261115-web/10-run-detail-failed.png) |
| 移动端工作流列表 | [11-mobile-workflow-list.png](assets/20261115-web/11-mobile-workflow-list.png) |
| 移动端导航抽屉 | [12-mobile-navigation-open.png](assets/20261115-web/12-mobile-navigation-open.png) |
| 移动端工作流任务 | [13-mobile-workflow-tasks.png](assets/20261115-web/13-mobile-workflow-tasks.png) |
| 工作流周视图 | [14-week-view.png](assets/20261115-web/14-week-view.png) |
| 工作流月视图 | [15-month-view.png](assets/20261115-web/15-month-view.png) |
| 运行时段失败详情面板 | [16-uptime-failure-panel.png](assets/20261115-web/16-uptime-failure-panel.png) |
| 从失败格跳转到任务日志 | [17-failure-log-drilldown.png](assets/20261115-web/17-failure-log-drilldown.png) |
