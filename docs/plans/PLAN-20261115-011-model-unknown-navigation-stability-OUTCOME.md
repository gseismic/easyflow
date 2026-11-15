# 左侧导航切换闪烁修复结果

- 创建时间：2026-11-15 21:35 Asia/Shanghai。
- 执行模型：Codex（基于 GPT-6；环境未提供可核实的完整模型 ID，使用 `model-unknown`）。
- 分析与完成版本：`WORKTREE`，基准提交 `834d418c7ce4faa78fabbe80cc0450d55bd400bf`；结果尚未固定为提交。
- 相关未提交文件：`web/src/components/WorkspaceNavigation.jsx`、`web/src/components/WorkflowHeader.jsx`、`web/src/pages/DagDetail.jsx`、`web/src/workspace.css`、`web/checks/navigation-stability.js`、`src/easyflow/web/static/index.html`、当前新增构建资源，以及本计划、结果和 `docs/plans/INDEX.md`。
- 关联计划：`docs/plans/PLAN-20261115-011-model-unknown-navigation-stability.md`。
- 状态：已完成并验证；未执行提交或推送。

## 完成内容

1. `web/src/components/WorkspaceNavigation.jsx:38`、`web/src/components/WorkspaceNavigation.jsx:107`：从已加载分支记录定位当前工作流，详情成功读取的归属优先。尚未确认归属时不插入临时置顶项，避免把列表中已有的工作流再显示一次。
2. `web/src/components/WorkspaceNavigation.jsx:35`：详情已确认项目变化时，暂时隐藏旧分支缓存中的同一入口，防止新旧项目同时高亮同一工作流。搜索、分批或历史定义不可用时需要的当前项入口仍然保留。
3. `web/src/pages/DagDetail.jsx:105`、`web/src/components/WorkflowHeader.jsx:3`：加载、首次失败和成功状态共用工作流标题及标签结构；加载显示本次目标，首次失败提供重试入口。加载或首次失败不显示旧工作流的运行数据和管理按钮。
4. `web/src/workspace.css:123`：工作区保留至少一屏的基本高度，加载卡片采用静态占位，没有添加闪烁动画。
5. `web/checks/navigation-stability.js:1`：新增独立浏览器回归，拦截全部业务 API，不需要登录、不向真实业务服务发送修改请求。

按用户指定 skill 完成哈夫曼式 UI 评审，结论和具体建议在回复中说明；没有另外创建 Review 文件，也没有实施整页布局调整。本次没有修改后端、API、数据库、调度行为或 `GOAL.md`。

## 验证结果

| 验证 | 实际结果 |
| --- | --- |
| 修复前复现 | Chromium 1440×900，详情延迟 700ms；同一工作流出现两个入口，原条目下移 82px 后恢复，导航容器本身没有重建 |
| 修复后真实隔离演示 | 同样延迟 700ms，待加载和完成后入口均只有一个，条目位移 0px，导航 DOM 保持；右侧待加载高度约 881px，修复前为 317px |
| 浏览器慢速与重复点击 | 55 帧全部只有一个目标入口，最大位移 0px、导航滚动变化 0px；重复点击当前链接不重新请求详情 |
| 快速切换 | 两次详情分别延迟 900ms 和 180ms；较早的慢响应不会覆盖后续目标或高亮 |
| 首次错误与重试 | 404 有错误和重试，无旧管理操作；恢复接口后重试成功 |
| 搜索与分批 | 搜索外当前项保持唯一；翻至下一批保留置顶项，返回包含当前项的批次后移除置顶项 |
| 项目归属变化 | 模拟详情已换项目、分支与 facets 尚为旧缓存；只保留新项目中的当前入口 |
| 定义移除后的历史 | 保留当前工作流 ID、不可用说明，并可读取已保存任务日志 |
| 只读与窄屏 | 只读账户加载和完成后均无源码入口；390×844 抽屉导航后关闭并恢复焦点，Esc 可退出，页面无横向溢出 |
| 运行时异常与写操作 | 浏览器回归没有 pageerror，模拟业务 API 没有收到任何写请求；模拟 404 用于错误状态验证 |
| 前端既有测试 | `pnpm --dir web test`：22 项通过 |
| 浏览器脚本语法 | `node --check web/checks/navigation-stability.js`：通过 |
| 相关 Web 后端测试 | `uv run pytest -q tests/test_web.py tests/test_web_auth.py tests/test_web_history.py tests/test_web_project_search.py tests/test_web_uptime.py`：43 项通过；有既有 Starlette/httpx 弃用提示 |
| 最终构建 | `pnpm --dir web build`：成功，54 个模块；当前入口为 `index-WgVWeSA-.js`、`index-CXoqRgOK.css` |
| 差异检查 | `git diff --check`：通过 |

浏览器回归复跑命令：先在 playwright-cli 会话中打开本机 easyflow 页面，再执行 `playwright-cli -s=<会话名> --raw run-code --filename=web/checks/navigation-stability.js`。测试创建独立浏览器上下文并在结束后关闭，不污染原会话。

真实演示复现脚本和截图位于仓库外 `/tmp/easyflow-nav-flash-repro.js`、`/tmp/easyflow-navigation-stability.png`。截图对应加载成功后的原有页面，未实施候选布局调整。

## 范围、偏差与剩余边界

没有实质计划偏差。项目变更与旧分支缓存产生重复项是在计划要求的归属优先级回归中确认并修复的相关边界。保留所有既有已跟踪构建资源，没有借本次构建清理历史产物；本次浏览器产生的仓库内临时输出已清理。

隔离演示服务恢复在 `http://127.0.0.1:8791`，继续使用交接文档中的仓库外状态库和 `--no-scheduler`。仅查询页面及业务数据，没有触发、暂停、清理或回补运行；没有更改默认业务状态库。

验证覆盖 Chromium 桌面和窄屏，以及模拟延迟、失败、缓存不一致等场景；没有验证其他浏览器内核，也未将本次前端加载修复描述为全部异步内容都同时就绪。按监控、排障场景提出的布局建议属于只读评审，尚未实施。

本轮已确认的闪烁及相关重复项均已解决，没有新增任务结束后仍需跟踪的确认缺陷；既有 `docs/KNOWN-ISSUES.md`、`docs/TODOS.md` 条目未改动。没有执行提交或推送，当前分支原有 10 个未推送提交保持不变。
