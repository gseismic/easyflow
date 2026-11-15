# 三种主题与设置入口交付结果

- 创建时间：2026-11-16 00:32 Asia/Shanghai。
- 执行模型：Codex（基于 GPT-6；完整模型 ID 未提供，使用 `model-unknown`）。
- 分析与验收范围：WORKTREE；基准提交 `1409777e599fd0badd8cc46daf2c1e3558ab0cd5`。本结果对应尚未提交的主题功能，基础导航及界面优化已固定在该基准提交。
- 计划：`docs/plans/PLAN-20261115-013-model-unknown-theme-settings.md`。
- 设计：`docs/design/easyflow-20261115-theme-settings.md`。
- 相关工作区文件：`web/src/appearance.js`、`web/src/useAppearance.js`、`web/src/appearance.css`、`web/src/pages/Settings.jsx`、`web/src/App.jsx`、`web/src/main.jsx`、`web/src/index.css`、`web/src/workspace.css`、`web/src/components/Graph.jsx`、`web/src/components/WorkspaceNavigation.jsx`、`web/vite.config.js`、`web/tests/appearance.test.mjs`、`web/checks/appearance.js`、`web/checks/appearance-real-minute.js`、`web/checks/monitoring-ui.js`、`src/easyflow/web/static/index.html` 及新构建资源、README、本轮计划/结果/设计和 TODO 索引及详情。

## 完成情况

计划范围已完成，未更改业务 API、数据库或权限，也未修改 `GOAL.md`。

| 内容 | 结果与依据 |
| --- | --- |
| 三种主题及切换规则 | 默认白天；黑夜固定深色；自动默认本地 07:00–19:00 为白天，可保存自定义时间，支持跨午夜，拒绝空值、越界和相同时间。`web/src/appearance.js:4`、`web/src/appearance.js:31` |
| 设置入口 | 左栏底部常驻“设置”，窄屏沿用导航抽屉；管理员和只读用户均可访问。`web/src/components/WorkspaceNavigation.jsx:195`、`web/src/App.jsx:17` |
| 设置界面 | 模式即时应用，自动时间显式保存，显示当前生效主题、存储范围、输入错误、保存失败和重试。`web/src/pages/Settings.jsx:7` |
| 持久化和实时同步 | 当前浏览器 localStorage 保存，跨标签页同步；自动模式在分钟边界和页面恢复时重算，切换模式及卸载时清理定时器和监听。`web/src/useAppearance.js:9` |
| 首屏和配色 | 构建时使用同一个核心生成首屏脚本；字符集声明仍在前 1024 字节内，应用脚本未加载时已应用主题及浏览器配色。页面、原生控件、图、日志、源码、弹窗和登录页使用语义色。`web/vite.config.js:6`、`web/src/appearance.js:45`、`web/src/appearance.css:38` |
| 后续告警 | 设置页有“告警通知 / 规划中”位置，明确当前未启用；未来渠道、收件人和真实投递验收记录为 `TD-20261115-2350-001`，见 `docs/todos/TD-20261115-2350-001-alert-notifications.md`。 |

## 验证与实际示例

以下均已实际运行通过；本机隔离演示服务为 `http://127.0.0.1:8791`，浏览器检查使用独立上下文，不更改原会话偏好，不向真实业务提交触发、暂停或清理操作。

- `pnpm --dir web test`：33 项通过，其中新增 11 项外观核心测试，覆盖模式优先级、日夜边界、跨午夜、时间校验、坏存储、写入失败、分钟调度及自包含首屏工厂。
- `uv run pytest -q tests/test_web.py tests/test_web_auth.py tests/test_web_history.py tests/test_web_project_search.py tests/test_web_uptime.py`：43 项通过；仅有既有 Starlette/httpx 弃用提示。
- `pnpm --dir web build`：59 个模块构建成功；最终入口使用 `assets/index-CcAAiWXI.js` 和 `assets/index-CgPxCjkd.css`。保留原有受版本管理的历史资源。
- `web/checks/appearance.js`：三模式、刷新、真实跨标签页同步、阻断应用脚本时的首屏主题、默认与自定义时间边界、跨午夜、焦点/可见恢复、非法草稿不落盘、存储禁用/损坏及恢复重试均通过。设置说明文字的白天对比度 5.48，黑夜说明 8.03、选项正文 8.17；任务标题、日志、源码及选中标记检查也通过。
- 真实示例“黑夜并刷新”：通过实际设置 UI 保存并重载，仍为黑夜；切回白天和另一标签页同步均通过。
- 真实示例“跨分钟自动切换”：通过 `web/checks/appearance-real-minute.js` 在 2026-11-16 本地 00:16 设置白天、00:17 设置黑夜，使用原生 Date 和真实 localStorage，观察到从 light 到 dark，边界延迟 1ms；未使用模拟时钟或模拟保存结果。边界前后截图位于临时目录 `/tmp/easyflow-theme-real-before.png`、`/tmp/easyflow-theme-real-after.png`。
- 真实示例“窄屏设置入口”：320/390/1440px 设置页无横向溢出；390px 抽屉进入设置后关闭并恢复焦点。检查了实际截图，黑夜主题的原生时间控件、说明和规划区均正常。
- 实际只读账号：使用已有真实 viewer 会话进入设置，保存黑夜及 08:15/21:45 自动时间并重载成功，再返回工作流；源码及业务写操作入口仍不可见。无浏览器异常，无业务写请求。
- `web/checks/navigation-stability.js`：七组回归通过，慢速切换采样 55 帧、最大位移 0px，快速切换、搜索/分页、错误重试、项目归属、历史定义缺失及窄屏行为保持正常。
- `web/checks/monitoring-ui.js`：刷新、日周月、原始失败尝试日志、详情标签、列表条件、长名称和 320/390/768/1000/1440px 断点均通过；时间格 y=351、运行选择 y=572、元信息 y=640，保留原有首屏密度。
- 既有 `web/checks/review-fixes.js` 在独立上下文复跑通过：特殊任务名、预览竞态、模拟确认请求范围及窄屏图表滚动均正常，浏览器异常为 0；清理请求由浏览器夹具拦截，没有修改业务数据。
- `git diff --check` 通过。

## 循环审查与计划偏差

实施中修正了首屏脚本放置造成字符集声明延后、存储失败后的重试入口与提示准确性，以及两处被误映射为阴影色的选中标记。最后两处恢复为有足够对比度的语义强调色，并新增实际页面选中标记检查；最终构建和外观浏览器回归通过。

监控回归脚本增加一次显式选择夹具日期：午夜后从历史详情返回概览会默认今天，固定的演示失败发生在前一天，不能继续假定今天有红格。这是测试夹具日期修正，没有改变产品日期逻辑。

最后一轮重新覆盖本轮源码、构建入口、测试、README 示例和关联设计，未再修改行为代码；本次审查范围内未发现待修复的逻辑问题。设置低频入口位于底部，自动时间与告警规划按需进入，沿用哈夫曼主次分配。

## 遗留与交付状态

邮件投递及告警持久化尚未实施，已明确延期并记录上述 TODO；不是本次已启用能力。外观为浏览器偏好，不跨设备同步；本轮实测 Chrome，未单独验证其他浏览器内核。

本轮没有新增需长期跟踪的已确认缺陷。结果及全部必要验证完成后执行本地 commit，不推送；实际提交号以本轮 Git 提交记录及最终回复为准。
