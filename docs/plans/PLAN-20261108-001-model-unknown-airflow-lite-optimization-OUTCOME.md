# 轻量 Airflow 高频功能优化结果

- 完成记录时间：2026-11-08 10:31 Asia/Shanghai
- 执行模型：Codex，基于 GPT-6；环境未提供实际模型 ID。
- 实施范围：`WORKTREE`，基准提交 `0e402689a9396eeb7d742c2ed39efa064ece9b47`。用户已有未跟踪 `GOAL.md` 未修改。相关未提交内容包括 SDK、调度与状态存储、CLI、`src/easyflow/web/`、`web/`、测试、README 和本计划及设计文档，尚未固定为提交。
- 关联计划：`docs/plans/PLAN-20261108-001-model-unknown-airflow-lite-optimization.md`。

## 实施结果

- 对齐所支持范围内的 TaskFlow、传统 Operator、XCom 引用、任务默认参数、常用 Context 及 Bash 返回值和跳过行为；不支持的默认参数明确报错。
- 支持 DAG 自动注册；通过解析子进程与调度元数据快照隔离退出和超时。对 DAG 文件及可追踪的本地 Python 依赖进行运行前校验，覆盖范围在 README 中明确。
- 按 Airflow 3.2 默认 trigger timetable 调整定时与回补的数据时间语义；量化示例显式计算业务窗口。支持多个 DAG 和同一 DAG 的运行并发，使用共享 SQLite 状态约束任务容量；过期任务租约不永久占用容量。
- 暂停/恢复持久化；支持回补预览，回补并发与普通 DAG 运行容量分开，任务仍共享实例并发。失败重试等待不阻断其他就绪任务。
- 完成本机 Web 界面及 FastAPI 管理接口，支持 DAG 列表、依赖图、运行与任务日志、触发、暂停/恢复、重跑预览和回补日期预览。确认重跑时只清理预览中的任务，并在状态库事务中重新检查。
- 按用户追加要求将 CLI 整体改为 Typer 命令组，补齐内存运行的 `dags test`，并更新使用说明和历史设计文档的引用。

## 计划偏差与边界

- 用户明确当前开发阶段不考虑旧版兼容，未实现旧数据库或旧时间语义的迁移层；未删除已有用户数据。
- 手动运行当前默认使用当前 UTC `logical_date`，未实现 Airflow 允许空 `logical_date` 的完整语义；分布式执行、权限体系、动态任务映射和完整 Provider/CLI/REST 不在范围内。
- 已确认异步发现时重复 DAG ID 的窗口，详见 `KI-20261108-1025-001` 和 `docs/known-issues/KI-20261108-1025-001-duplicate-dag-discovery.md`。部署前应检查所有文件的解析结果及 DAG ID 唯一性。
- 进程租约不能保证外部副作用 exactly-once；停止 Web 自动启动的 scheduler 可能中断本地任务，文档已说明。

## 验证

- 全量 Python 回归：107 项通过；并发回补、暂停恢复、过期容量、解析隔离、Web API 和 CLI 均有针对性测试。
- `ruff check src tests examples` 与 `git diff --check` 通过；前端 `pnpm build` 通过。
- 本机浏览器验证 DAG 列表、解析错误、依赖图、暂停后触发排队、恢复执行、日志、预览并重跑，以及桌面和窄屏布局；最终重跑请求只包含预览展示的任务，控制台无错误。
- `uv build` 生成 source distribution 和 wheel；隔离环境中验证 wheel 的界面入口与 API，并检查最终 wheel 中的静态资源引用完整。
- 本次未提交或推送。

## 2026-11-08 10:33：干净环境复核

执行模型仍为 Codex，基于 GPT-6，实际模型 ID 未提供。在临时副本中执行 `uv sync --extra dev` 和 `uv run pytest -q -p no:cacheprovider`，使用新解析的依赖再次通过 107 项测试。新版 Starlette 的测试客户端对 `httpx` 发出一条弃用提示；该提示仅出现在测试依赖中，本次没有为消除提示额外增加运行时依赖。
