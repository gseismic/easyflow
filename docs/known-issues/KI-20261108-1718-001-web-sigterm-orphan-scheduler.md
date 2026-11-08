# KI-20261108-1718-001：Web 收到 SIGTERM 后可能遗留调度器

- 创建时间：2026-11-08 17:18 Asia/Shanghai。
- 创建模型：Codex，基于 GPT-6；环境未提供实际模型 ID。
- 初始状态：未解决。
- 来源：示例审查修复期间，为加载调度修复而重启 Web；关联计划 `docs/plans/PLAN-20261108-003-model-unknown-example-review-fixes.md`。
- 确认版本：`WORKTREE`，基准提交 `b29d2bfa6eddc4ef698530d0f9805c294230285a`；`src/easyflow/web/app.py` 在该工作区未修改，运行环境使用修改后的 `pyproject.toml` 和 `src/easyflow/_internal/schedules.py`。工作区整体尚未固定为提交。
- 引入提交：未定位；修复版本：无。

## 问题、触发条件与影响

当 `easyflow web` 自行启动 scheduler 后，向实际 Web Python 进程发送 `SIGTERM`，Web 可能直接退出而未停止 scheduler。遗留的 scheduler 仍持有租约并可继续调度任务；随后启动的 Web 会将它识别为已有 scheduler，导致旧进程继续使用原先加载的代码。

这与 `README.md` 原有“停止 Web 会同时停止由它启动的 scheduler”的表述不完全一致，可能使用户误以为调度已经停止，或误以为重启 Web 已加载新的调度代码。此处只确认 `SIGTERM` 行为，不推断其他信号或进程管理器的全部行为。

## 确认依据

- `src/easyflow/web/app.py:103` 在没有现存 scheduler 时创建子进程，`src/easyflow/web/app.py:128` 至 `src/easyflow/web/app.py:137` 意图在 Web 结束时通知并等待该子进程。
- 2026-11-08 17:17 Asia/Shanghai 在临时 DAG 目录、独立 SQLite 状态库和空闲端口运行 Web；确认 scheduler 是 Web 子进程后，向 Web 发送 `SIGTERM`。Web 以 `-15` 退出，等待 1 秒后 scheduler 仍在运行。随后向该测试 scheduler 发送 `SIGINT` 并确认退出；未使用用户数据。
- 相同隔离条件下向 Web 发送 `SIGINT`，Web 以 `0` 退出且其 scheduler 退出。当前用户服务已清理旧进程，重新启动的 Web 正在使用其新建的 scheduler。

## 临时绕过

交互运行时可使用 `Ctrl+C`（`SIGINT`），并确认 Web 自启的 scheduler 进程已退出。若使用 `SIGTERM`，需确认原 scheduler 是否仍在运行；在确认没有需要保留的执行中任务后，可对该 scheduler 正常发送 `SIGINT`。独立启动、由 Web 复用的 scheduler 本来就不受 Web 停止控制。
