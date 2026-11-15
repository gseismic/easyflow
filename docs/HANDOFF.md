# easyflow 会话交接

最新补充见第 11 节（2026-11-16 01:54 Asia/Shanghai）：本轮已将默认 uptime 改为最近小时并加入周/月原位展开日图；下文前 10 节保留历史交接，默认日视图等旧描述已由第 11 节更新。

- 创建时间：2026-11-15 16:43 Asia/Shanghai。
- 创建者：Codex（基于 GPT-6；环境未提供可核实的完整模型 ID，标记为 `model-unknown`）。
- 项目根目录：`/Users/mac/pai-studio-fin/library/easyflow`。
- 编写前分支与 HEAD：`main`，`5390558551e0c7af0ec7d69d097d67472e5860b5`。最新功能代码提交为 `7dd1ca82adf1618c864620265e1fe5768a3f051c`；其后只有版本记录文档更新。
- 来源范围：上述固定代码版本，以及下文明确列出的 `WORKTREE` 文档、图片和仓库外运行资料。相关未提交资料不属于 HEAD 的固定内容。
- 用户最新要求：写交接文档，马上新开 session；**不要执行上下文压缩**。本轮仅整理交接，没有执行压缩或新的功能开发。

文中代码路径均相对于项目根目录，行号以编写前 HEAD 为准；文档 Markdown 链接按本文件所在的 `docs/` 目录解析。仓库外路径单独使用绝对路径。

## 1. 接手时先明确的状态

当前没有尚未完成的功能实现。用户要求的入队时间修复、半小时 uptime、搜索筛选常驻、日/周/月视图均已实现、验证并本地提交。用户要求的“先提交，再检查 Airflow dashboard 有什么可以借鉴”也已完成：调研在提交之后进行，只提出建议，没有继续实施建议中的功能。

下一 session 应先读 [GOAL.md](../GOAL.md) 与 [AGENTS.md](../AGENTS.md)，再按用户的新指令继续。不要把下文候选建议自动视为已授权开发任务。

项目目标原文为：做一个 mini、轻量版 Airflow，用户使用友好，支持可视化管理任务。`GOAL.md` 本会话未修改，继续遵循“只有用户明确要求时才能修改目标文件”的限制。

## 2. 用户已确认的产品与交互约束

- 工作流列表中逐个显示 uptime 图，详情页继续展开查看；用户选择的是“列表每行显示，详情展开”，没有新增独立运行监控导航。
- **搜索和基础筛选必须常驻。** 用户明确否定为了首屏时间图而折叠搜索、项目、标签和状态入口的方案。该错误已修复；清空搜索或移除最后一个条件也不会收起工具区域。代码入口：`web/src/pages/DagList.jsx:105`。
- 默认日视图，每格半小时；周、月查看粗粒度信息时，每格一天。用户随后明确补充“天级别也支持，默认显示”。当前实现按日/周/月查看范围切换，不是把每格分别设置成一天/一周/一月。
- 周从周一至周日，月为自然月；日期使用浏览器本地时区，支持 DST，不固定把每天当作 24 小时。日期范围和跨月导航：`web/src/uptime.js:39`、`web/src/uptime.js:52`。
- 红色标记发生过错误的时段，重试成功后仍保留；不把一个失败运行覆盖的整个执行区间全部涂红。灰色仅表示无实际执行记录，不代表故障，也不能证明应该执行的任务已正常执行。
- 沿用浅绿、白色、深绿文字的现有视觉风格。用户明确指定过 `ui-design-zh`；API 设计使用 `api-design-zh`；图片设计稿使用内置 imagegen，不能用代码截图冒充图片生成设计稿。
- 用户曾要求代码完成后先提交再继续其他工作，本会话按此顺序执行。没有执行 push。

## 3. 已完成的实现与代码入口

### 3.1 任务入队时间

原始审查文档是 `docs/reviews/20261115-0758-REVIEW-model-unknown-WORKTREE-task-queued-at.md`，该文件仍在工作区、未跟踪。问题是 `task_instance.queued_at` 有列、有 API 和页面展示，但没有正常写入路径。

已定义为“最近一次尝试满足依赖并进入执行队列的时间”，不包含上游依赖等待和重试间隔：

- 持久化入队、重复扫描保留时间、暂停和 scheduler fence 校验：`src/easyflow/_internal/state_store.py:732`。
- 有队列阶段时保留原入队时间；直接领取时以启动时刻兜底：`src/easyflow/_internal/state_store.py:873`。
- clear 清空该时间：`src/easyflow/_internal/state_store.py:1710`。
- Sequential 与 Local executor 先为就绪任务排队，容量不足时保留等待起点：`src/easyflow/_internal/persistent_runner.py:425`、`src/easyflow/_internal/persistent_runner.py:557`、`src/easyflow/_internal/local_executor.py:197`。
- 页面展示“入队时间（最近尝试）”和“排队耗时”：`web/src/components/TaskDetails.jsx:69`、`web/src/components/TaskDetails.jsx:73`。

旧记录未保存的入队时间没有推测补写。测试位于 `tests/test_task_queued_at.py`；既有恢复并发测试也已调整为依赖就绪任务处于 queued，而不是 NULL，并继续验证未提前启动及排队时间保留。

### 3.2 uptime 聚合和日/周/月

- 批量接口：`GET /api/workflows/uptime`，入口为 `src/easyflow/web/routers/catalog.py:67`。沿用登录保护，viewer 可读，支持重复 `dag_id`，最多 100 个工作流。
- 聚合实现：`src/easyflow/web/uptime.py:19`。优先根据 `task_try` 的实际执行区间统计，没有 try 的旧运行才使用运行时间兜底，避免 clear/重试等待变成虚假的连续执行。失败按实际结束时刻归格，按 run ID 去重，不受最近 100 条运行选择器的限制。
- `granularity=half_hour` 为默认值：保留旧 API 行为，范围为正半小时倍数，最多 25 小时。普通日为 48 格，DST 日可为 46/50 格，见 `src/easyflow/web/uptime.py:37`。
- `granularity=day&timezone=Asia/Shanghai` 等每日聚合：起止时间转入指定 IANA 时区后必须为午夜，范围为 1–31 个本地自然日；日边界分别转换成 UTC。每天直接去重聚合，不相加半小时计数，见 `src/easyflow/web/uptime.py:44`。
- 当前等待落入实际边界，通过二分查找定位，见 `src/easyflow/web/uptime.py:185`。历史未保存的等待时段没有回填。
- 每日聚合 `slot_minutes` 为 null，半小时仍为 30；响应另有 `granularity`、`timezone`，见 `src/easyflow/web/uptime.py:205`。

前端入口：

| 文件 | 作用 |
| --- | --- |
| `web/src/uptime.js:39` | 日/周/月范围、跨月导航、请求 URL、格子说明和错误链接 |
| `web/src/components/UptimeControls.jsx:4` | 常驻日/周/月按钮、日期、前后周期、今天/本周/本月、图例 |
| `web/src/components/UptimeStrip.jsx:5` | 半小时或每日格子、日期/星期刻度、选中说明和键盘操作 |
| `web/src/components/UptimeStrip.jsx:57` | 周/月选中日期后的“查看当天”，切回该日半小时明细 |
| `web/src/components/WorkflowUptime.jsx:7` | 详情页三种视图，默认日 |
| `web/src/pages/DagList.jsx:154` | 列表共享范围控件，按当前页批量查询 |
| `web/src/pages/DagList.jsx:215` | 每个工作流的时间图及查看当天回调 |

状态优先级是：发生错误、当前执行中、当前等待、有执行、无执行/未来。绿色表示该格有实际执行，不是“服务可用率”。切换查看范围保留所选日期，重新进入列表/详情默认今天的日视图，没有新增范围 URL 持久化。

### 3.3 错误定位

格子统计包含示例 `sample_run_id`；出错格优先附带 `sample_task_id` 和 `sample_try_number`。点选后可以进入该次失败任务及原始尝试日志，成功重试后也不会误看最新成功日志。

- 链接构造：`web/src/uptime.js:107`。
- 路由读取任务和尝试参数：`web/src/App.jsx:24`；组件实例 key 包含这些参数：`web/src/App.jsx:274`。
- 历史运行页选中指定任务/尝试：`web/src/pages/HistoryRun.jsx:21`；页面提示当前任务状态/时间是最近一次尝试的结果：`web/src/pages/HistoryRun.jsx:115`。

## 4. A1 图片设计稿：已保存，未整体实现

文件：[docs/ui-design/A1/easyflow-workflow-overview.png](ui-design/A1/easyflow-workflow-overview.png)。这是用户要求使用 image 生成的方法、基于当前 Web 截图生成的图片，随后按用户指定路径复制保存。原生成文件也保留在 `/Users/mac/.codex-3/generated_images/01a1dc94-834a-7382-8580-fc0cddc43f96/exec-8c15a7eb-3932-45a9-9869-b66356fd3d62.png`。

A1 描绘紧凑的工作流表格、对齐的时间图、选中红格下方原位展开失败信息和日志。**当前代码尚未完整实现 A1 的紧凑布局及行内错误面板**；当前点击错误仍进入历史运行详情。不要把图片中的设计效果描述为已实现功能。

该图片目前未跟踪，用户要求保存的成果应继续保留；新设计稿不要直接覆盖 A1。

## 5. 本地提交与工作区

| 提交 | 内容 |
| --- | --- |
| `6ac0f571d33a4de9e6c8350530926b97e1047fe7` | 入队时间、半小时 uptime、失败任务与尝试日志入口 |
| `3da7839724a921fd9f94b15afec1dd4a897f90e4` | 上述版本及测试服务记录 |
| `c99e5da6b6e925db21fced9dbf700b2c06d46edd` | 修复基础搜索与筛选常驻 |
| `7dd1ca82adf1618c864620265e1fe5768a3f051c` | 日/周/月视图、每日聚合、DST 与相关测试 |
| `5390558551e0c7af0ec7d69d097d67472e5860b5` | 日/周/月版本记录；编写交接前的 HEAD |

本会话没有执行推送。上表代码、测试和设计说明已提交。

编写交接前，工作区没有未提交的应用源码，保留下列既有资料：

```text
 M docs/plans/INDEX.md
?? docs/plans/PLAN-20261115-004-model-unknown-live-web-monitor-check.md
?? docs/plans/PLAN-20261115-004-model-unknown-live-web-monitor-check-OUTCOME.md
?? docs/reviews/
?? docs/ui-design/
```

`docs/plans/INDEX.md` 剩余差异只有编号 004 的两行索引，是本会话开始前已有的内容；编号 005–007 的记录已经提交。之前提交采用显式文件列表和索引局部暂存，没有把这些既有资料混进代码提交。后续不要随手 `git add .`，也不要清理或覆盖这些文件。

## 6. 测试服务与本机资料

### 当前服务

- 地址：`http://127.0.0.1:8791`，本机独立演示环境，不是默认项目业务数据库。
- 2026-11-15 16:40–16:43 复核：Web PID `40549`，父进程 `40541`。PID 仅为快照，使用前重新检查端口和命令行。
- 使用 `--no-scheduler`，**当前调度器离线**。离线在日/周/月修改前已被观察到，本轮未诊断其原始原因；重新加载 Web 时确认没有实际运行中的任务，并保留这一状态。创建新运行会排队，不要误以为打开 Web 就会执行任务。
- 交接时实际复核：页面 HTTP 200，演示账户登录 200，catalog 成功且 `scheduler_running=false`，当天 uptime 返回 48 格。
- 用户名：`uptime-check`。临时演示凭据保存在下方本机文件中，不把口令写入 Git 文档。

本机运行目录（可能随系统清理而消失）：

```text
/var/folders/gh/54w67t6n7_b8pv7t76xw5_qm0000gn/T/easyflow-uptime-ax5mvrw_/
├── dags/
├── state.db
├── logs/
├── fixture-note.txt
└── handoff-access.json
```

`handoff-access.json` 保存 base_url、username、password，权限已确认是 `0600`，仅用于该隔离演示库。不要将它加入版本管理或输出到无关日志。`/tmp/easyflow-uptime-current-dir` 也记录了该目录。

如现有服务已经退出，可在项目根目录恢复同一 Web：

```bash
EASYFLOW_DEMO_DIR=/var/folders/gh/54w67t6n7_b8pv7t76xw5_qm0000gn/T/easyflow-uptime-ax5mvrw_
uv run easyflow web \
  --dags-folder "$EASYFLOW_DEMO_DIR/dags" \
  --database-url "$EASYFLOW_DEMO_DIR/state.db" \
  --log-root "$EASYFLOW_DEMO_DIR/logs" \
  --port 8791 --no-scheduler
```

需要恢复任务执行时，应针对这个隔离库明确启动 scheduler；不要混用默认 `.easyflow/easyflow.db`。本次交接没有启动、停止 scheduler 或触发任务。

演示工作流有 `daily_sync`、`first_workflow`、`hourly_pipeline`、`retrying_pipeline`。前两者有真实运行记录；后两者包含用于长区间/等待显示的合成夹具。不要把夹具当成业务执行证据。

### 浏览器、截图和外部脚本

已有 Playwright CLI 会话名为 `easyflow-uptime`，可先 `playwright-cli list`，再用 `playwright-cli -s=easyflow-uptime snapshot` 检查；旧 tool session ID 不应作为新 session 的依赖。

- 当前视图截图：`/tmp/easyflow-uptime-week.png`、`/tmp/easyflow-uptime-month.png`、`/tmp/easyflow-uptime-periods-mobile.png`。
- 搜索常驻验证：`/tmp/easyflow-visible-filters-desktop.png`、`/tmp/easyflow-visible-filters-mobile.png`。
- 验收脚本：`/tmp/easyflow-uptime-periods-browser-check.js`、`/tmp/easyflow-visible-filters-check.js`。这些脚本带有本次演示库和日期假设，不是仓库的通用自动测试。

## 7. 已通过的验证与工具注意事项

最新功能代码 `7dd1ca82adf1618c864620265e1fe5768a3f051c` 的验证：

- `uv run pytest -q`：**259 项通过**；存在已有 Starlette/httpx 弃用提示。
- 在 `web/` 执行 `pnpm test`：**12 项通过**；`pnpm build` 通过。
- 修改 Python 文件的默认 Ruff、全仓核心 `--select E4,E7,E9,F`、`git diff --check` 通过。
- 默认全量 Ruff 仍有 **194 项既有诊断**，不能称全量 Ruff 通过；本次修改文件没有新增诊断，详情见 `KI-20261108-2311-001`。
- 浏览器验证了默认日、周/月、查看当天、短月前后导航、390px 窄屏，以及纽约时区 2026-11-01 的 50 格日视图与跨该日的周/月。最终页面 0 个 alert、控制台 0 错误/警告。
- 后续交接文档没有改应用源码，不需要为文档重复全套测试。

主要测试：`tests/test_task_queued_at.py`、`tests/test_web_uptime.py`、`tests/test_web.py`、`tests/test_review_boundary_fixes.py`、`web/tests/uptime.test.mjs`。

开发注意：

- Python 使用 `uv run`，前端使用 pnpm，优先沿用既有技术栈。
- `web/vite.config.js:6` 的构建会清空 `src/easyflow/web/static`。本会话保留旧的已跟踪资源，仅更新入口并增加新哈希文件；后续构建先确认已有资源改动，不要覆盖用户已有修改。
- 当前构建入口为 `index-CuoyGlRT.js`、`index-BOqk6M7I.css`。普通 Web 用户不需要 Node。
- Vite 开发代理指向 `127.0.0.1:8790`，而当前演示后端是 `8791`；直接用当前构建后的 `8791` 验证最简单。运行 `pnpm dev` 时注意代理目标差异，见 `web/vite.config.js:7`。
- 当前修复没有增加新依赖、数据库迁移或改变调度语义。

## 8. 已知问题和文档入口

主索引：[docs/KNOWN-ISSUES.md](KNOWN-ISSUES.md)。以下均为仍未解决的既有事项，详情以对应条目为准，不在这里另维护处理日志：

- `KI-20261108-1025-001`：异步发现重复 DAG ID 前可能已经触发运行。
- `KI-20261108-1637-001`：测试日期早于 DAG 开始时间仍执行任务。
- `KI-20261108-1718-001`：Web 收到 SIGTERM 可能遗留 scheduler。
- `KI-20261108-2311-001`：默认 Ruff 基线检查失败。

当前不存在 `docs/TODOS.md`，没有为只读 Airflow 调研自动建立待办。

本轮相关文档：

- [入队时间与 uptime 设计](design/easyflow-20261115-uptime.md)。
- [日/周/月设计](design/easyflow-20261115-uptime-periods.md)。
- [编号 005 结果](plans/PLAN-20261115-005-model-unknown-uptime-queued-at-OUTCOME.md)。
- [编号 006 搜索常驻修复结果](plans/PLAN-20261115-006-model-unknown-visible-search-filters-OUTCOME.md)。
- [编号 007 日/周/月结果](plans/PLAN-20261115-007-model-unknown-uptime-periods-OUTCOME.md)。
- [计划索引](plans/INDEX.md)。

## 9. 已完成的 Airflow UI 调研与候选下一步

代码提交后，查看了 [Airflow 3.4.0 官方 UI 图解](https://airflow.apache.org/docs/apache-airflow/3.4.0/ui.html)，并实际检查官方首页、DAG 列表、Grid、DAG 概览和任务日志截图。以下是已向用户汇报的建议，**没有实施，也不代表用户已选择下一项开发**：

| 优先级建议 | 借鉴点 | easyflow 的候选方向 |
| --- | --- | --- |
| 高 | 失败概览与日志联动 | 点击红格后在原位展开失败任务、错误摘要和该次日志，与 A1 方向一致 |
| 高 | 所选周期统计 | 展示所选日/周/月的运行数和错误数，点击数字进入相应筛选 |
| 中 | 任务 × 最近运行矩阵 | 任务为行、最近多次运行为列，定位反复失败和上游阻塞 |
| 中 | 耗时对比 | 用小柱图比较最近运行，区分排队耗时与实际执行耗时 |

建议优先“失败详情原位展开”，以缩短从红格到错误原因的路径。当前全局统计条仍表示工作流当前/最近状态，不是所选 uptime 周期的执行总数；不要向用户声称周期统计已经完成。

来源定位：[首页](https://airflow.apache.org/docs/apache-airflow/3.4.0/ui.html#home-page)、[DAG 概览与 Grid](https://airflow.apache.org/docs/apache-airflow/3.4.0/ui.html#dag-details-page)、[任务日志](https://airflow.apache.org/docs/apache-airflow/3.4.0/ui.html#task-instance-view)。

官方截图保存在本机 `/tmp/airflow-3.4-home_light.png`、`/tmp/airflow-3.4-dag_list1.png`、`/tmp/airflow-3.4-dag_overview_grid1.png`、`/tmp/airflow-3.4-dag_overview_dashboard1.png`、`/tmp/airflow-3.4-dag_task_instance_logs1.png`，未加入项目。

## 10. 新 session 的接续顺序

1. 读取目标、规范、本交接；运行 `git status --short`，保留既有文档与 A1 图片。
2. 根据用户的新指令确定下一步。若是继续 UI 设计，先看 A1 和当前真实 Web；若用户采纳 Airflow 建议，先明确要实现哪项并按规范建立计划。
3. 若需要浏览器检查，先确认 `8791` 与现有 Playwright 会话；需要登录时读取仓库外的演示凭据文件。
4. 保持基础搜索筛选常驻、日视图默认、错误历史保留、时区与 DST 语义，不重新引入已被用户否定的折叠设计。

不需要继续本轮实现，也不需要重新生成 A1 或重跑所有验证来恢复上下文。

## 11. 2026-11-16 01:54 最近小时与周月展开交接

- 更新者：Codex（基于 GPT-6；完整模型 ID 未提供，使用 model-unknown）。
- 本轮来源基准：`9eece2edc1759cb2c8d96675cb0fae8fd22fa260`，主题与设置已在该提交完成；本节记录本轮 WORKTREE，相关文件及验收版本见下方结果文档，最终提交号查 Git 记录。
- 用户新要求已完成：凌晨能看前一晚的最近 X 小时；周/月选一天，在原汇总下展开更紧凑的当天 uptime。

默认现在是最近 24 小时，允许 1–168 小时并保存在当前浏览器，列表与详情共用；API 使用服务端 now 的相对范围，首尾裁剪，实际跨日时间可见。代码：`web/src/components/useUptimeView.js:6`、`src/easyflow/web/uptime.py:43`。固定日/周/月仍可选择，DST 规则保留。这个新默认覆盖第 2、10 节的默认日视图描述。

周/月选非未来日期会原位展开当天图，20px 格子，可收起、切日、独立重试，顶部刷新同步日图；红格仍读取原始失败尝试。代码：`web/src/components/UptimeStrip.jsx:88`、`web/src/components/UptimeDayDetail.jsx:5`、`web/src/workspace.css:167`。未来日不请求展开，父周/月保持。

验证：37 项前端、63 项相关 Web 后端、针对性 Ruff、构建、管理员及真实 viewer 的实际示例均通过；导航、监控布局、主题和原有预览/日志浏览器回归通过。最后一轮无待修复逻辑问题。本机服务 8790（examples）和 8791（既有隔离演示）已重启加载新 API，均无 scheduler，未触发业务运行。最终资源为 `index-49jTBR6E.js`、`index-CIm2VWKM.css`，旧资源保留。

一次磁盘不足导致截图失败已通过清理本轮已结束测试的临时目录解决，随后必要回归全部跑通。没有清理其他任务目录；日夜和 320/390/768/1440px 已检查，其他浏览器内核未单独验证。

本轮没有待继续实现的授权事项，也没有新增长期问题；既有问题和后续事项仍以 [KNOWN-ISSUES](KNOWN-ISSUES.md)、[TODOS](TODOS.md) 为准。邮件告警尚属后续范围。

关联文档：[计划](plans/PLAN-20261116-001-model-unknown-rolling-uptime.md)、[结果](plans/PLAN-20261116-001-model-unknown-rolling-uptime-OUTCOME.md)、[设计](design/easyflow-20261116-rolling-uptime.md)。本轮本地提交，不推送。
