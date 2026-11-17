# 全量前端代码 Review

- 创建时间：2026-11-17 08:20 Asia/Shanghai。
- 审查者：Codex；运行环境没有提供可核实的完整模型 ID，使用 `model-unknown`，不推测模型名称或版本。
- 审查版本：`e4ca538ddcf97510846dcfdfbd4a839627429c65`，分支 `main`；开始时工作区干净。
- 范围：用户指定的全部前端代码，而非仅最后一次侧栏调整。累计变更基线为前端首次加入前的 `0e402689a9396eeb7d742c2ed39efa064ece9b47`；审查范围为该基线至上述 HEAD 的前端实现及必要的后端契约。
- 规则依据：已读取 `GOAL.md`、根目录 `AGENTS.md`；未发现适用于前端的更具体项目指令文件。
- 本次产物只有本报告；应用源码、发布静态资源、依赖、数据库和项目目标均未修改，没有提交或推送。

## 结论

确认 **3 项 P2 逻辑问题**，分别影响特殊合法工作流的可访问性、慢请求完成后的页面状态，以及重跑预览的错误恢复。基础测试和构建通过不能排除这些交互边界问题。

这些问题存在于当前完整前端中，不应归因于最后一次侧栏提交。本轮执行的是审查、复现和报告，没有生成修复补丁；问题均未解决，**尚未完成用户要求的 `review-fix-loop` 修复闭环**，不能报告“本次范围内没有待修复问题”。

## 覆盖范围

静态阅读覆盖 `web/src/` 的全部 **48 个文件**，并检查入口、构建配置、依赖锁定、现有测试及必要的 API 实现。

| 范围 | 文件或目录 | 关注点 |
| --- | --- | --- |
| 应用入口、会话与请求 | `web/src/App.jsx`、`web/src/main.jsx`、`web/src/api.js`、`web/src/pages/Login.jsx` | 路由、登录/退出、CSRF、轮询、取消及过期响应 |
| 全部 7 个页面 | `web/src/pages/` | 工作流列表与详情、全局/工作流历史、历史详情、登录和设置 |
| 导航、搜索与过滤 | `web/src/navigation.js`、`web/src/listRoute.js` 及 `web/src/components/` 中的导航、搜索和过滤组件 | 条件往返、分页、组合输入、项目变化、移动导航及存储降级 |
| 任务与管理操作 | `web/src/runApi.js` 及 `web/src/components/` 中的依赖图、任务详情、日志、运行元数据、调度、源码和全部对话框 | 当前定义与历史状态、任务/尝试选择、日志、触发/重跑/回补及异常路径 |
| 运行时间图 | `web/src/uptime.js`、`web/src/components/Uptime*.jsx`、`web/src/components/WorkflowUptime.jsx`、`web/src/components/useUptime.js`、`web/src/components/useUptimeView.js` | 最近/日/周/月、时区与 DST、选择稳定性、失败原始尝试及局部展开 |
| 其余共享组件与 Hook | `web/src/components/` 中的状态、调度器、工作流头部、横向滚动组件以及筛选统计、待加载刷新 Hook | 状态语义、错误/加载边界、刷新、滚动与键盘访问 |
| 外观与布局 | `web/src/appearance.js`、`web/src/useAppearance.js`、`web/src/index.css`、`web/src/workspace.css`、`web/src/appearance.css` | 初始化主题、定时切换、跨标签页同步、响应式布局 |
| 工程与发布 | `web/index.html`、`web/vite.config.js`、`web/package.json`、`web/pnpm-lock.yaml`、`web/tests/`、`web/checks/`、`src/easyflow/web/static/index.html` 及其当前引用资源 | 构建边界、测试覆盖、依赖和已发布资源一致性 |

关联核对包括 `src/easyflow/web/routers/`、`src/easyflow/web/service.py`、`src/easyflow/sdk/dag.py`、`src/easyflow/_internal/state_store.py` 及相关测试。未将后端已有独立问题扩大为本次前端发现。历史未引用的哈希构建文件不作为多套独立源码重复审查；当前入口引用的 JS、CSS 和 HTML 已与重新构建结果逐字节比较。

## 已确认问题

### R1 · [P2] 将点号 DAG 标识移出 API 路径段

当合法的 `dag_id` 为 `.` 或 `..` 时，`web/src/pages/DagDetail.jsx:27` 把它直接拼入路径，而 `encodeURIComponent` 不会消除点路径的特殊语义；浏览器将 `/api/dags/.` 归一化为 `/api/dags/`，后端最终返回工作流列表对象，随后 `web/src/pages/DagDetail.jsx:122` 访问不存在的 `tasks.find` 导致整个 React 工作台白屏；`..` 则请求到 `/api/` 并返回 404。它们确实是受支持输入，依据是 `src/easyflow/sdk/dag.py:27`、`:121` 以及 `tests/test_review_runtime_boundaries.py:428-447`，不是人为构造非法 ID。应采用查询参数等不受点路径归一化影响的接口寻址，并统一详情及管理接口的合法 ID 契约；仅再次 URL 编码不能修复。

### R2 · [P2] 忽略已关闭触发对话框的过期界面回调

当触发请求较慢、用户关闭对话框并转到设置或另一个工作流后，`web/src/components/TriggerDialog.jsx:22` 仍无条件执行旧 `onDone`，而 `web/src/pages/DagDetail.jsx:92` 会直接重写全局 hash，强制把用户拉回原工作流；即使原详情组件已经卸载，这个全局副作用仍然发生，也会丢掉新页面未保存的局部输入。浏览器已复现“创建运行 → 请求悬挂 → 取消 → 进入设置 → 放行旧响应 → 自动跳回原详情”，请求期间取消按钮仍可用，见 `web/src/components/TriggerDialog.jsx:53`。应限制进行中操作的关闭行为，或在处理响应时验证对话框/操作仍有效；关闭界面也不能被表述为撤销已发送的运行。

### R3 · [P2] 保留同一范围重新获取重跑预览的入口

当预览包含运行中任务，或者可清理的 `up_for_retry` 任务在确认前被调度器领取而使 `/api/runs/clear` 返回 409 时，`preview` 会一直保留：`web/src/components/ClearDialog.jsx:74-78` 因此不再显示“查看影响”，异常处理 `:16-17` 也只更新错误信息。结果是界面要求“请重新预览”，但同一范围内没有重新预览入口；初次被阻塞的预览在任务结束后仍维持旧的禁用确认，409 后重新点确认则继续使用旧快照。用户只能关闭重开，或切换到其他范围再切回来。应保留显式刷新预览操作，并在状态冲突后使旧预览失效、重新确认最新影响；服务端拒绝冲突是正确行为，不应取消该保护，参见 `src/easyflow/web/routers/runs.py:64-67`。

## 引入版本核对与去重

| 问题 | 已核实的引入提交 | 核对方式 |
| --- | --- | --- |
| R1 | `9d967b659645ee34f3669212b7fd5b906173a3af` | 前端首次加入时已经使用该路径拼接；累计审查基线尚无前端目录 |
| R2 | `e0a6c77fbfa5a5ae4621a867e215b98690018a6a` | 该提交把触发完成后的局部 `setTab` 改为写入全局 `window.location.hash`；结合此前可关闭的异步触发形成导航回归 |
| R3 | `9d967b659645ee34f3669212b7fd5b906173a3af` | 首版即仅在 `preview` 为空时提供检查入口，异常没有使快照失效 |

已对照 `docs/KNOWN-ISSUES.md`、`docs/TODOS.md` 和上次 Web Review 及复核报告。R3 与此前“请求中修改重跑范围导致旧响应覆盖”的问题不同：范围锁定已经实现，本次是状态变化后缺少重新预览路径。R1 也不同于此前已修复的运行/任务特殊 ID 查询参数问题，本次缺口是工作流详情仍使用点路径段。

## 实际验证结果

| 检查 | 结果 |
| --- | --- |
| 在 `web/` 执行 `pnpm test` | **37 项通过** |
| `uv run pytest -q` | **293 项通过**，约 172 秒；1 项已有 Starlette/httpx 弃用警告 |
| `pnpm build --outDir <独立临时目录> --emptyOutDir` | 通过，61 个模块；未清空或重写仓库发布目录 |
| 构建产物与当前发布文件 `cmp` | HTML、`index-BjKCI6IS.js`、`index-DIXidKp1.css` 三项完全一致 |
| `web/checks/navigation-stability.js` | 通过；模拟 API 的独立浏览器上下文，覆盖慢加载、重复点击、快速切换、错误重试、搜索分页、项目纠正、已移除定义的历史、只读角色及移动导航 |
| 本轮额外浏览器检查 | 普通工作流 6 个标签、任务切换、指定尝试日志、全局历史详情、深色主题刷新保留均符合检查预期；在注入点号缺陷前无 `pageerror` |
| 响应式检查 | 列表在 320、375、390、720、1000、1001、1440px 宽度下，文档 `scrollWidth` 均等于视口宽度 |
| R1 浏览器复现 | `.` 导致 `Cannot read properties of undefined (reading 'find')`，工作台主区域数量变为 0；新文档访问 `..` 显示 404 加载错误 |
| R1 真实 API 契约验证 | 独立临时库与 FastAPI `TestClient`：SDK 接受两个 ID；`/api/dags/.` 最终取得列表结构，`/api/dags/..` 返回 404；源码请求同样错寻址 |
| R2 浏览器复现 | 拦截触发 POST 并延迟响应；关闭后进入设置，放行响应即跳回 `#/dags/daily_sync` |
| R3 浏览器复现 | 注入真实接口相同的 409 后，显示“请重新预览”，而“查看影响”按钮数量为 0 |
| R3 真实 API 契约验证 | 临时库中 `up_for_retry` 预览 200 → 改为 `running` 后确认 409 → 阻塞预览 200 → 改为 `success` 后新预览 200；证明 API 允许重新检查，缺口在界面恢复入口 |

浏览器额外检查使用新建临时静态服务和模拟会话，业务写请求全部在浏览器层拦截；真实 API 验证只操作临时数据库和临时 DAG 目录，没有读写默认 `.easyflow` 业务库，也没有触发实际任务或启动调度器。定向检查在复核阶段再次执行，三项观察保持一致。

## 验证空白与完成边界

- 未执行在线调度器驱动的完整业务端到端链路；R3 的任务状态转换在独立临时库中构造，不冒充真实调度执行。
- 未原样执行其余 5 个依赖专用演示夹具的 `web/checks/` 脚本；特别是本轮没有重做真实跨分钟主题切换和真实失败库的长时间滚动检查。旧报告的通过记录不计为本轮验证结果。
- 没有对 Firefox、WebKit、真实触屏设备及长时间浏览器挂起进行本轮验证；浏览器检查使用 Playwright 启动的 Chromium。
- 前端现有自动测试主要覆盖纯函数，缺少本报告三个场景的组件级回归。后续修复应补充：合法点号 ID 的导航、关闭/卸载后延迟触发响应、blocked/409 后同范围重新预览；修复后仍须重新覆盖整个约定范围，不能只重跑三个复现点。
- 未新增计划、修复结果文档或已解决状态；本报告不构成问题已经修复、修复循环已经通过或整个前端绝对无错的声明。
