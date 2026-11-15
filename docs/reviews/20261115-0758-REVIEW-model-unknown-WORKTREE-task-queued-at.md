# 任务详情“入队时间”监控 Review

- 创建时间：2026-11-15 07:58 Asia/Shanghai
- 实际模型：model-unknown（当前会话未提供更具体的模型 ID）
- 分析范围：`WORKTREE`；基准提交 `a8708df99a4f6ea8c4c274e86dc5fc577b88fc6c`。本次审查相关代码为该提交内容；工作区另有未提交文件 `docs/plans/INDEX.md`、`docs/plans/PLAN-20261115-004-model-unknown-live-web-monitor-check.md` 和 `docs/plans/PLAN-20261115-004-model-unknown-live-web-monitor-check-OUTCOME.md`。实际运行证据来自仓库外 `/tmp/easyflow-web-livecheck.wyaojD/state.db`。

## 评审范围

核对真实 Web 运行中任务详情的“入队时间”字段是否由运行状态正确写入并展示。审查范围：

- `src/easyflow/_internal/state_store.py`
- `src/easyflow/web/service.py`
- `web/src/components/TaskDetails.jsx`
- 隔离 Web 实例 `live_monitor_demo` 的实际运行记录。

这是针对任务时间字段的定向 Review，不代表并发、失败恢复或其他 Web 页面的全面审查。

## 发现

### [P2] 任务入队时间没有写入，详情页持续显示空值

- **触发条件**：新建任务运行并在工作流或运行详情页查看任务详情。
- **现象**：详情页的“入队时间”显示 `—`，无法看到任务进入待执行队列的时间。
- **影响**：用户无法根据该字段判断任务开始前等待了多久，也无法用它排查调度或并发容量造成的排队延迟。任务运行状态、开始/结束时间、日志和历史状态仍可正常查看。
- **代码依据**：`task_instance.queued_at` 在表结构中可空（`src/easyflow/_internal/state_store.py:120-134`）；创建任务实例时只写入状态 `NULL`（`src/easyflow/_internal/state_store.py:428-435`）；`start_task_try` 写入 `state`、`try_number`、`started_at` 和租约字段，却没有设置 `queued_at`（`src/easyflow/_internal/state_store.py:819-849`）。代码中仅在清理任务时将 `queued_at` 置空（`src/easyflow/_internal/state_store.py:1653-1666`）。服务层把该列原样返回（`src/easyflow/web/service.py:739-746`），前端将其渲染为时间（`web/src/components/TaskDetails.jsx:69-70`）。
- **运行证据**：真实 Web 手动运行 `manual__2026-11-14T22:52:24.835757+00:00` 成功结束后，`extract`、`transform`、`finish` 三条 `task_instance` 记录的 `queued_at` 均为 `NULL`；三个任务的开始、结束时间和最终状态均已记录。页面与数据库一致地将入队时间显示为空。
- **建议**：明确 `queued_at` 表示首次排队还是最近一次尝试排队，并在任务进入持久化待执行状态时写入该时间；重试和清理时按定义更新或清空。若当前执行器没有可观察的入队阶段，则应隐藏该字段或说明其不可用。补充状态库、API 和页面验证，覆盖 queued 到 running 的时间传递及清理/重跑行为。

## 其他观察与验证空白

- 本次真实运行中，scheduler 在线状态、运行状态、任务依赖顺序、任务日志和工作流内/全局历史均与状态库一致；除上述入队时间外，本次检查范围没有发现其他不一致。
- 浏览器未登录时 `/api/auth/session` 返回 `401` 并显示登录页，属于预期认证流程；登录、触发及监控请求成功。
- 真实验证为一次单机串行成功路径，没有运行并发压力场景，也没有验证排队延迟的实际数值。即使该场景未形成可见队列等待，代码中缺少 `queued_at` 写入路径及数据库空值仍可确认该字段当前不可用。

## 结论

需要修复或明确该字段的产品语义后再对外展示。此次只形成 Review 文档，没有修改应用代码或自动测试。
