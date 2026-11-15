# Web 实际运行监控验证结果

- 创建时间：2026-11-15 06:58 Asia/Shanghai
- 实际模型：model-unknown（当前会话未提供更具体的模型 ID）
- 分析范围：`WORKTREE`；基准提交 `a8708df99a4f6ea8c4c274e86dc5fc577b88fc6c`。本次提交未修改应用源码；工作区文档改动为计划、结果和 `docs/plans/INDEX.md`。运行数据、截图和临时 DAG 位于仓库外的 `/tmp/easyflow-web-livecheck.wyaojD/`。

## 完成情况

按[计划](PLAN-20261115-004-model-unknown-live-web-monitor-check.md)完成一次真实 Web 登录、页面触发、scheduler 执行和运行历史核验。未发现 Web 监控状态、任务依赖或日志与实际执行不一致的问题。Web 与 scheduler 保持运行，供用户查看。

## 实际过程与验证结果

- 临时 DAG `live_monitor_demo` 在独立目录中包含 `extract → transform → finish` 三个顺序任务。首两段分别运行 25 秒和 20 秒，并输出阶段日志；整个过程没有使用项目默认数据库、日志目录或现有示例。
- `uv run easyflow dags list --dags-folder /tmp/easyflow-web-livecheck.wyaojD/dags` 发现了该 DAG。使用隔离状态库和临时管理员登录 Web；未登录时 session 检查返回 `401` 并显示登录页，登录及后续认证 API 请求成功。
- 浏览器通过 `POST /api/dags/live_monitor_demo/trigger` 创建运行，HTTP `200`。运行 ID：`manual__2026-11-14T22:52:24.835757+00:00`。
- 运行期间 Web 显示 scheduler 在线及更新中的心跳。`extract` 运行时，两个下游仍显示未运行；随后页面自动更新为 `extract` 成功、`transform` 运行中、`finish` 未运行。状态库中的同一时刻任务状态相符。
- 日志面板按任务显示了对应输出：`LIVE_CHECK:extract:start/done`、`LIVE_CHECK:transform:start input=ready` 与 `done`、`LIVE_CHECK:finish input=ready-processed`。浏览器的 DAG 详情、运行详情、任务日志及历史查询 API 均返回 `200`。
- 约 46 秒后 DAG 运行成功；状态库记录的结束时间为 `2026-11-14T22:53:13.039861+00:00`。三个任务各执行一次且均成功，XCom 依次保存 `ready`、`ready-processed`、`complete`。工作流内历史页和全局运行历史都展示了同一 run ID、成功状态及 46 秒耗时。
- 最终确认 Web 根页面返回 `200`，监听地址为 `127.0.0.1:8790`；Web PID `12685` 与其 scheduler PID `12688` 均仍运行。截图保存在 `/tmp/easyflow-web-livecheck.wyaojD/live-success-full.png`。

## 观察说明

任务仍在运行时，详情页显示最近心跳时间；任务成功后显示“暂无心跳”。这与当前状态字段语义一致：`src/easyflow/_internal/state_store.py` 的 `commit_task_terminal` 在终态时清空活动租约和 `last_heartbeat_at`，而最后一次心跳仍保留在 `task_heartbeat` 表中。因此本次未将终态的“暂无心跳”判定为缺陷。

浏览器日志中唯一的 `401` 是登录前的 `/api/auth/session` 检查；实际登录、触发和监控数据请求均成功。没有观察到其他浏览器控制台错误或警告。

## 计划偏差与遗留

- 计划内步骤均完成。验证限于一个本机实例、一个手动运行和成功路径，不代表并发、失败恢复或长时间负载验证。
- 临时 Web 实例、scheduler、数据库、任务日志和 DAG 文件继续保留在上述 `/tmp` 目录，供用户打开查看；不涉及仓库业务数据。
- 没有发现需要新增 KNOWN-ISSUES 或 TODOS 的事项，也没有修改应用代码。
