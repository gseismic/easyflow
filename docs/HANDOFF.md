# easyflow 会话交接

最新交接见第 13 节（2026-11-17 03:53 Asia/Shanghai）：已修复刷新时反复误报工作流不完整，代码与验证完成、尚未提交，现有业务 Web 须重启加载。第 12 节的图片设计及代码微调仍保留原有暂停状态，本轮没有推进。第 11 节记录最近小时与周/月展开功能；前 10 节为历史信息，不能代替当前代码和最新交接。

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

## 12. 2026-11-16 05:41 界面微调设计与代码实施交接

- 更新者：Codex（基于 GPT-6；环境未提供完整模型 ID，使用 `model-unknown`）。
- 当前分支：`main`；固定 HEAD：`9e3f1cd5b4803d0e02a5bc0c269c62d611a7544b`。本节的设计图片和文档是 WORKTREE，未固定为提交。
- 当前要求：**先写交接文档，暂停代码实施。** 目标“差不多可以实施代码微调了”当前状态为 `paused`，没有标记完成。
- 本会话没有修改应用源码、配置、静态资源、测试或 `GOAL.md`，没有新 commit，也没有 push。

### 12.1 用户已明确的方向

用户先调用其提供的 `ui-redesign-zh`，要求“为我微调界面”；随后明确“不用所有的都生成，我觉得可以了”，再明确“差不多可以实施代码微调了”，最后要求先交接。这些授权持续有效。恢复后应实施代码微调，不用重新确认是否允许实施，也不要继续补齐图片或开启下一轮图片迭代。

微调保留当前布局、左侧树形导航、顶部栏、详情标签和操作路径，延续浅绿、白色、深绿文字风格。重点是间距、字体层级、控件一致性、按钮主次、卡片边框和阴影，以及日夜主题和窄屏细节。没有授权增加功能或重组页面。搜索与基础筛选仍常驻；最近 24 小时、日周月、当天展开和历史错误语义沿用当前实现。

### 12.2 已保存成果及复核状态

- [本次设计说明](design/easyflow-20261116-ui-polish.md)：真实截图中的问题、页面范围、配色和微调方向。
- [图片设计计划](plans/PLAN-20261116-002-model-unknown-ui-polish.md) 与 [结果](plans/PLAN-20261116-002-model-unknown-ui-polish-OUTCOME.md)：原计划已按用户停止补图的指令收口，不能描述为全部完成。
- [原始截图记录](ui-design/source/20261116/notes.md)：`docs/ui-design/source/20261116/` 共 19 张原图；桌面视口 1440×1000，手机 390×844。长页采用完整页面截图，弹窗和抽屉为视口截图，已逐张打开检查。
- [A2 状态及逐页索引](ui-design/A2/notes.md)：`docs/ui-design/A2/` 保存 01–16 共 16 张 PNG，以及 01–19 的逐页 `*-prompt.txt`。17 黑夜概览、18 手机概览、19 手机抽屉只有提示词，没有归档 PNG；不再补生成。
- 旧 `docs/ui-design/A1/` 保留，没有覆盖。A2 采用内置 imagegen，以实际截图为输入，后续页面使用 A2 概览作样式参考。生成图的画布尺寸存在差异，不等同于 CSS 精确尺寸。

**已用图片查看工具重新打开的 A2 稿件只有 7 张：02 列表、03 概览、04 任务、05 日志、06 工作流历史、07 调度详情、08 源码。** 01 登录与 09–16 共 9 张已保存，但中断前尚未完成保存后的逐图复核。不要宣称 16 张全部通过复核。

已发现两处设计稿偏差，后续实施直接按真实数据和统一样式处理，无须为它们再生成图片：

1. `docs/ui-design/A2/03-overview.png` 的 22:00 刻度误写为 `11/16 22:00`，正确应为 `11/15 22:00`；真实截图 `docs/ui-design/source/20261116/03-overview.png` 正确。不能把图片误字写入时间计算逻辑。
2. `docs/ui-design/A2/06-workflow-history.png` 的“触发运行”仍为白底，未与其他已检查详情稿的深绿主按钮统一。实现时统一主操作，保留刷新和暂停为次级操作。

A2 是未完成的图片轮次，因用户认为已有成果足够而停止，不是“无问题”的定稿。未检查稿件中的长 ID、代码、日志、中文和日期，应在真正使用时结合原图核对。用户已接受视觉方向，不代表逐字接受图像生成误差。

### 12.3 建议的实施起点与约束

尚未建立代码实施计划，也没有开始编辑 CSS/JSX。恢复后先按项目规范创建当天新编号的实施计划，不把编号 002 图片计划伪装成已实施代码的计划；使用 `feature-dev-workflow`，沿用既有 React、FastAPI、pnpm、uv，不新增依赖。

可优先阅读这些已核实入口，行号对应上述 HEAD：

| 路径与行号 | 本次相关性 |
| --- | --- |
| `web/src/main.jsx:3` | 样式加载顺序为 index.css → workspace.css → appearance.css；调整覆盖关系前先确认来源。 |
| `web/src/index.css:476` | 共享按钮；primary 阴影在 `web/src/index.css:489`，卡片在 `web/src/index.css:544`，登录样式在 `web/src/index.css:1104`。 |
| `web/src/workspace.css:1` | 工作台导航、页头及紧凑控件；当天小格的 20px 高度在 `web/src/workspace.css:167`，窄屏规则从 `web/src/workspace.css:194` 开始，编辑前重读。 |
| `web/src/appearance.css:1`、`web/src/appearance.css:38` | 浅色/深色语义变量；设置控件也在此文件。 |
| `web/src/pages/DagDetail.jsx:149` | 当前“＋ 触发运行”按钮仍用 `button subtle`，可按稿统一为 primary；只调整呈现，不改触发逻辑与权限。 |
| `web/vite.config.js:13` | 构建输出到已跟踪的 `src/easyflow/web/static`，`emptyOutDir: true`。构建前检查已有资源，避免无关删除；代理在第 14 行，当前指向 8790。 |

视觉参数参考：浅色底 `#F5F7F5`、白面板、边框 `#DFE8E2`、主文 `#183B2D`、辅文 `#53695E`、主色 `#216B4F`；卡片约 12px 圆角、控件约 8px，8px 间距体系。深色以 `#101914` 背景、`#19251F` 面板、`#344A3D` 边框、`#E4ECE6` 主文为参考，减少整体绿雾。正文目标约 14px、辅助字约 12px；保留明显焦点状态及手机触摸面积，不能机械把全部 44px 控件缩小。列表每条工作流适当收紧留白，保留原表格字段和下方独立时间条，不改成新布局。

后续应运行 `web/` 的 `pnpm test`、`pnpm build`，并在真实隔离页面验证列表/详情/日志/设置、红格原位失败、周月当天展开、登录/弹窗、日夜及 390/1440px；必要时补 320/768px。已有 `web/checks/appearance.js`、`rolling-uptime.js`、`navigation-stability.js`、`monitoring-ui.js`、`review-fixes.js` 可参考。旧脚本中可能包含像素断言，调整视觉后要按有意义的行为边界复核，不能为通过检查恢复旧间距。本轮没有运行这些代码测试，历史通过结果不能冒充新微调验证。

### 12.4 当前环境与工作区

本轮开始时工作区干净。交接前工作区为：

```text
 M docs/HANDOFF.md
 M docs/plans/INDEX.md
?? .playwright-cli/
?? docs/design/easyflow-20261116-ui-polish.md
?? docs/plans/PLAN-20261116-002-model-unknown-ui-polish.md
?? docs/plans/PLAN-20261116-002-model-unknown-ui-polish-OUTCOME.md
?? docs/ui-design/A2/
?? docs/ui-design/source/
```

`.playwright-cli/` 是本轮浏览器命令产生的快照/日志，不作为交付源码；不要随手 `git add .`。A2、原图、提示词和新增记录属于用户成果，应保留。没有应用代码改动，没有需回滚的代码。

隔离演示 Web 已恢复：`http://127.0.0.1:8791`，本轮读取时监听进程 PID `25780`，父 uv PID `25779`；均为快照，使用前重新核实。保持 `--no-scheduler`，未触发/暂停/清理/回补业务。数据、日志及 `handoff-access.json` 仍在第 6 节的隔离目录。新截图会话 `polish` 已关闭；原 `easyflow-uptime` 会话仍列为 open，可自行新建独立上下文，避免改变已有浏览器偏好。长会话名曾超过 macOS Unix socket 长度而启动失败，短名称正常。

本轮页面截图等待字体/图片加载；主体浏览 pageerror 为 0、业务写请求为 0。首开未登录时出现的 401 属登录前状态，不是已确认应用缺陷。没有在仓库写入口令；临时认证状态文件 `/tmp/easyflow-ui-polish-auth.json` 为本机临时资料，旧会话可能已失效，不加入版本管理。

曾因磁盘不足保存提示词失败；用户随后明确“我已经重新分配了空间”，保存已恢复。没有删除其他任务缓存、浏览器或业务目录。由于用户中断，旧函数执行单元已不可查询，最后 3 张 PNG 未归档；不重新发起生成。

既有问题和后续工作见 [KNOWN-ISSUES](KNOWN-ISSUES.md)、[TODOS](TODOS.md)，本轮未发现需要新增长期条目的应用问题。恢复后的顺序是：读 GOAL/AGENTS 和本节 → 检查 Git → 建立代码微调计划 → 基于已接受方向实施和验证。当前先完成交接并停止实施。

交接收尾检查：`git diff --check` 通过；7 份相关 Markdown 的 101 个本地链接全部存在；已确认 19 张原图、16 张生成图、19 份提示词；`web/`、`src/`、`tests/`、`GOAL.md` 没有本轮差异。未运行应用测试或构建，因为本轮没有代码修改。

## 13. 工作流刷新完整性误报修复（2026-11-17 03:53 Asia/Shanghai）

- 执行模型：Codex（基于 GPT-6；完整模型 ID 未由环境提供，使用 `model-unknown`）。
- 来源范围：`WORKTREE`，基准完整提交 `64597a920303d75f8625c868b7f377b519443c66`；本次相关代码尚未固定为提交。开始时 `main` 与 `origin/main` 同步且工作区干净；本轮未提交或推送。
- 用户报告 Web 总提示工作流不完整，怀疑与刷新有关。已通过回归和真实浏览器确认：已有同签名快照的 5 秒后台重解析被误标为 `pending`，使多个列表和 facets 周期性退回不完整状态。
- `src/easyflow/_internal/dag_parser.py:116`、`src/easyflow/_internal/dag_parser.py:144` 现在只等待/标记没有当前快照的文件；首次、新增、修改仍加载，真实错误仍显示。`tests/test_web_discovery_refresh.py` 覆盖失效快照、旧 Future、增删、空文件、错误及恢复，并核对五类 API 状态。
- 版本 `0.1.0 → 0.1.1`，同步 `pyproject.toml`、`uv.lock`、`web/package.json`；没有新增依赖或修改前端源码、构建资源、数据库、调度逻辑、设计图片与 `GOAL.md`。

验证：8 项新增后端、100 项相关 Web/发现/调度/运行边界后端、37 项前端通过；实际发现 12 个示例 DAG，无错误且未执行任务。真实 1 秒慢解析 DAG 的隔离浏览器完成自动刷新、3 次手动刷新和重载，初始化收敛后不完整提示 0；新增错误文件后实际提示仍在，修复为空辅助文件后恢复。浏览器无 pageerror、无业务写操作。新测试默认 Ruff、修改文件核心规则、锁文件检查和差异检查通过；解析器默认 Ruff 仍有与基准相同的两条既有诊断，见 [KI-20261108-2311-001](known-issues/KI-20261108-2311-001-ruff-locked-baseline.md)，本轮未提交。结果及验证边界见 [修复结果](plans/PLAN-20261117-001-model-unknown-discovery-refresh-OUTCOME.md)，计划见 [修复计划](plans/PLAN-20261117-001-model-unknown-discovery-refresh.md)。

本次未提交的文件为：解析器、新测试、版本声明与锁文件（`pyproject.toml`、`uv.lock`、`web/package.json`）、`docs/plans/PLAN-20261117-001-model-unknown-discovery-refresh{,-OUTCOME}.md`、`docs/plans/INDEX.md`、`docs/HANDOFF.md`、`docs/KNOWN-ISSUES.md` 和既有 Ruff 条目详情。仅这些文件属于本轮成果，不应混入其他任务改动。

隔离服务使用仓库外目录 `/var/folders/gh/54w67t6n7_b8pv7t76xw5_qm0000gn/T/easyflow-refresh-check-ro5s4s7f/`，端口 `8792`、`--no-scheduler`；本轮 `refresh` 浏览器会话和该隔离进程已关闭，临时凭据不入 Git，仓库内本轮浏览器快照已移除。用户原有 `8790` 进程未重启，启动参数为 `easyflow web --dags-folder examples`，须重启 Web 才会加载后端修复；不得误称浏览器刷新已使旧 Python 进程更新。其他问题及待办仍以主索引为准，没有新增未解决的刷新相关问题。收尾核对 94 个本地 Markdown 链接均存在，三处版本一致，差异检查通过。
