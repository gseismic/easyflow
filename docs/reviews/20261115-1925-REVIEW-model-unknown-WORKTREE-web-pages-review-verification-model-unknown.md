# Web 页面 Review 修复复核

- 创建时间：2026-11-15 19:49 Asia/Shanghai。
- 执行模型：Codex，基于 GPT-6；运行环境未提供可核实的完整模型 ID，使用 `model-unknown`。
- 原报告：`docs/reviews/20261115-1925-REVIEW-model-unknown-WORKTREE-web-pages.md`，保留 19 张首次浏览器巡查截图。
- 复核范围：`WORKTREE`；基准完整提交 `3d4b58db81336bf30a930c7d7cc676a2fbba71c5`。用户提供的四条代码 Review 对应累计实现截至 `339c0dc10a80367570fadf9a49b0379145e27971`；本轮以当前工作区复现和验证，不把基准提交描述成最终固定版本。
- 相关未提交内容：`src/easyflow/_internal/state_store.py`、`src/easyflow/web/uptime.py`、`tests/test_web_uptime.py`、`web/src/uptime.js`、`web/tests/uptime.test.mjs`、`web/src/components/{ClearDialog,Graph,UptimeStrip,HorizontalScroll}.jsx`、`web/src/index.css`、`web/checks/review-fixes.js`、`src/easyflow/web/static/index.html` 及对应新构建资源，以及计划、报告和截图。原始未跟踪报告和截图均保留。
- 关联计划：`docs/plans/PLAN-20261115-010-model-unknown-web-review-fixes.md`。

## 逐条复核

### P2：预览范围与确认提交不一致——已解决

全部任务预览未返回时改选当前任务，旧响应仍能覆盖预览，确认随后提交全部任务，可能重复产生外部写入。原实现的请求和确认分别位于 `web/src/components/ClearDialog.jsx:24`、`web/src/components/ClearDialog.jsx:32`；当前在 `web/src/components/ClearDialog.jsx:41` 对请求期间的范围选择施加 `disabled={busy}`，范围变动仍在 `web/src/components/ClearDialog.jsx:44` 清空旧预览。

浏览器使用可手动释放的请求拦截验证：全部预览期间范围锁定；返回后可切换且确认按钮消失；重新预览当前任务后，唯一确认请求体为 `{"task_ids":["extract"]}`。模拟预览 500 后显示错误并解锁。确认请求完全由测试拦截，没有对演示库执行清理。

### P2：聚合扫描无重叠历史——已解决

原查询对所选日期前的全部已结束任务尝试调用 Python 时间转换，历史较多时超过 `web/src/api.js:56` 的 15 秒超时。新增表达式索引位于 `src/easyflow/_internal/state_store.py:158`、`src/easyflow/_internal/state_store.py:161`。当前活动查询在 `src/easyflow/web/uptime.py:105` 从少量工作流 ID 驱动范围索引，使用结束时间下界保留跨日执行；失败聚合和失败详情也在 `src/easyflow/web/uptime.py:142`、`src/easyflow/web/uptime.py:274` 先筛选候选记录。

SQLite `julianday` 只作保守粗筛，参数在 `src/easyflow/web/uptime.py:19` 向两端保留一秒余量；无法解析的 Python ISO 格式继续保留，最终仍由原 Python 转换判断微秒和时区。没有结束时间的记录继续走既有运行结束时间/当前时间回退。回归在 `tests/test_web_uptime.py:705` 同时验证：旧结束时间不会进入 Python 转换、跨日及未结束尝试不丢失、左边界瞬时事件与偏移时区及微秒不误删、已有数据库可重复初始化、实际查询计划使用范围索引且不扫描任务/运行事实表。

隔离性能夹具：25 个工作流，每个 30,000 次旧运行及已结束失败尝试，共 750,000 条 `dag_run`、`task_instance` 和 `task_try` 记录；查询无执行的 2026-01-02。基准和新实现读取同一数据库。

| 测量 | 时间 |
| --- | --- |
| 基准实现，空日 | 32.9175 秒 |
| 修复实现，空日 | 0.0254 秒 |
| 修复实现，空周 / 空月 | 0.0050 / 0.0072 秒 |
| 失败详情空页 | 0.0048 秒 |
| 在已有 750,000 条历史上首次建立两个索引 | 8.3619 秒（一次性初始化成本） |

初版只加范围谓词仍被优化器选为事实表扫描，约 1.8 秒；复审查询计划后用 `CROSS JOIN` 固定 ID 驱动顺序，再得到上述结果。最终计划包括 `SEARCH t USING INDEX ix_task_try_uptime_end (dag_id=? AND <expr>>?)` 和相同范围形式的 `ix_dag_run_uptime_end`；等待查询使用既有 `(dag_id, state)` 索引。数字是本机夹具结果，不是生产 SLA。

### P2：依赖图读取继承属性——已解决

当前定义新增合法任务名 `__proto__`、所选历史运行没有该任务时，普通下标读取到原型对象，Status 把它作为 React 子节点会崩溃。`web/src/components/Graph.jsx:76` 现用 `Object.hasOwn`，仅把自有状态或 `undefined` 传给 Status。浏览器在真实工作台中注入 `__proto__`、`constructor`、`toString` 三种定义，分别验证缺失状态显示“未知”、自有成功状态显示“成功”，并逐个选中任务检查详情；没有渲染错误。

### P2：归一化午夜的小时被继承——已解决

Santiago 的 `2026-09-06` 从 01:00 开始，复制 start 再 setDate 会把次日终点错误延伸到 01:00。`web/src/uptime.js:23`、`web/src/uptime.js:34` 和 `web/src/uptime.js:50` 现独立构造目标日期的本地边界。进一步发现月初自身发生跳变时，后端原午夜校验也会拒绝合法起点；`src/easyflow/web/uptime.py:60` 现按每个日期独立构造午夜并比较其 UTC 时刻，允许该日午夜归一化结果，仍拒绝普通日期的非午夜起点。

新增的前后端测试先在原实现失败，修复后通过；真实 Chrome 时区上下文的 UI/API 联合验证结果：

| 时区与日期 | 日 | 周 | 月 |
| --- | --- | --- | --- |
| `America/Santiago`，2026-09-06 | 46 格，04:00Z 至次日 03:00Z | 7 格，8 月 31 日至 9 月 7 日 | 30 格，9 月 1 日至 10 月 1 日 |
| `America/Asuncion`，2017-10-01 | 46 格，04:00Z 至次日 03:00Z | 本夹具未单独验证 | 31 格，首日 23 小时、后续日期均 24 小时 |

表中所有已测请求均返回 200；原纽约 23/25 小时及周/月测试继续通过。后端回归位于 `tests/test_web_uptime.py:686`，前端回归位于 `web/tests/uptime.test.mjs:40` 和 `web/tests/uptime.test.mjs:69`。

### P3：窄屏滚动提示——已解决

原始截图 11、13 确认时间轴和依赖图确实超出各自容器，页面缺少可继续查看的线索。`web/src/components/HorizontalScroll.jsx:7` 使用 ResizeObserver 检测实际横向溢出；`web/src/components/HorizontalScroll.jsx:17` 提供描述关联、可聚焦容器，溢出时才显示提示。图表内容尺寸及容器尺寸变化都会重新测量。接入依赖图的位置是 `web/src/components/Graph.jsx:23`，时间图使用同一组件。

390×844 下验证内部滚动、依赖图方向键、时间轴 End 到最后一格、无文档整体水平溢出；恢复 1440×1000 后无溢出的时间图自动隐藏提示。另打开新截屏人工复核提示位置和任务详情布局；原有 19 张首次截图不覆盖、不冒充修复后的截图。

## 验证与循环审查结论

- `uv run pytest -q`：265 项通过；仅已有 Starlette/httpx 弃用提示。
- 在 `web/` 执行 `pnpm test`：22 项通过；`pnpm build` 通过。新构建 JS/CSS 已由真实 Web 服务加载。构建自动删除的 21 个既有受控静态资源已恢复，保留历史资源，不夹带资源清理。
- `uv run ruff check src/easyflow/web/uptime.py tests/test_web_uptime.py` 以及 `uv run ruff check --select E4,E7,E9,F src tests examples` 通过；`git diff --check` 通过。
- 同一 Ruff 环境按相对文件、规则及信息比较基准与 WORKTREE：默认全量检查均为 194 项，新增 0、减少 0。它仍失败，属于已有 `KI-20261108-2311-001`，没有通过缩小规则将其写成已解决。
- 可重复的交互回归脚本为 `web/checks/review-fixes.js`，运行方式：`playwright-cli -s=easyflow-uptime --raw run-code --filename=web/checks/review-fixes.js`；需要已登录且含 `daily_sync` 夹具的本机隔离环境。
- 最后一轮重新覆盖未登录入口、工作流搜索/清除、日周月、六个详情标签、全局历史、失败详情和原始尝试日志、特殊任务名、重跑预览以及窄屏导航和滚动；未出现非预期 API 错误或 pageerror。该轮未再修改应用代码。
- 19 张原始 PNG 均通过文件签名、完整数据块 CRC 和报告链接检查。

结论：四项 P2 和一项 P3 均已修复并验证，本次 Web 页面及关联逻辑审查范围内没有发现待修复的逻辑问题。全部改动仍为 WORKTREE，未提交或推送。隔离服务保持 `--no-scheduler`；未验证在线调度器真实执行，也没有改写默认业务数据。既有 Ruff 基线问题保留跟踪，不阻止本次已完成的行为验证。
