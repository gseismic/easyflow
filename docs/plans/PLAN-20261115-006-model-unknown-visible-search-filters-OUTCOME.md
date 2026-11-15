# 工作流基础搜索与筛选常驻修复结果

- 创建时间：2026-11-15 14:44 Asia/Shanghai。
- 实际模型：Codex（基于 GPT-6；完整模型 ID 未提供，沿用 `model-unknown`）。
- 实施与验证范围：`WORKTREE`；基准提交 `3da7839724a921fd9f94b15afec1dd4a897f90e4`。相关修改为 `web/src/pages/DagList.jsx`、`web/src/index.css`、README、`docs/design/easyflow-20261115-uptime.md`、本计划与结果、计划索引，以及 `src/easyflow/web/static/index.html` 和新增的 `index-UcLSk5X3.js`、`index-BDATsz5i.css`。创建结果时内容尚未固定为提交。

## 完成情况

已完成[计划](PLAN-20261115-006-model-unknown-visible-search-filters.md)，没有范围偏差。

- 移除基础工具外层的 `<details>`、展开标题及 `hasFilters` 条件，改成名为“工作流搜索与筛选”的常驻区域，见 `web/src/pages/DagList.jsx:103`。搜索框、项目、标签入口及状态按钮可直接使用，清除条件不会隐藏控件。
- 删除整体折叠容器对应的样式；内部标签面板、过滤组件、统计和请求逻辑保持原实现。没有改动后端、数据库或 A1 图片。
- README 和 uptime 设计说明已修正，撤销“为首屏空间默认折叠基础搜索筛选”的旧假设。
- 前端构建入口已更新，现有隔离测试服务 `http://127.0.0.1:8791` 刷新后可加载新版本；原有静态资源已保留。

## 验证

- `pnpm test`：9 项通过。
- `pnpm build`：通过，生成 `index-UcLSk5X3.js` 和 `index-BDATsz5i.css`。
- `git diff --check` 通过；入口引用的资源全部存在，原有资源未删除，`GOAL.md` 与 `AGENTS.md` 未修改。
- 真实浏览器初始无条件时，搜索框、项目选择、标签入口和四个状态按钮均可见，4 条 uptime 图正常加载。
- 状态切换再“清除过滤”、搜索后刷新恢复、清空搜索、选择再移除项目、选择再移除最后一个标签后，基础控件始终可见，URL 和匹配结果符合原有行为。
- 搜索无匹配时仍可直接操作搜索和过滤；清空后恢复原列表。运行历史页的搜索和项目入口正常。
- 390px 窄屏中页面宽度仍为 390px，基础入口可见，没有页面级横向溢出。
- 浏览器控制台 0 错误、0 警告。截图保存在 `/tmp/easyflow-visible-filters-desktop.png` 与 `/tmp/easyflow-visible-filters-mobile.png`。

本次仅调整前端容器及对应样式，没有新增镜像式单元测试，也未重复执行后端全套测试。演示实例在本次验证时显示调度器离线，列表查询和界面验证正常；本次没有触发任务或将这些检查表述为任务执行验证。

## 工作区与遗留

- 编号 004 的既有计划、结果及索引记录，`docs/reviews/` 和 `docs/ui-design/A1/` 设计图均保留。
- 没有新增需跨任务跟踪的已确认缺陷或延期事项。本次修复对应版本以结果文件所属的 Git 提交为准，未推送。
