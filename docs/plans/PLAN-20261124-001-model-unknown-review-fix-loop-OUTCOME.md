# EasyFlow 全量 Review 修复与复审结果

- 创建于 2026-11-24 18:59（Asia/Shanghai）。执行工具为 Codex，已知模型系列为 GPT-6，环境未提供精确模型 ID。
- 复审覆盖从 `d2a1585296ef2d0dd62e9888e77ee3c7b03daf4e` 至本次 `WORKTREE` 的累计代码和用户入口。修复基线为 `c900362e04944fd8c0a483c8958f1899aae795d1`。工作区改动涉及 `README.md`、本计划及报告、执行与存储代码、Web API 与界面、回归测试，以及前端入口引用的新静态包 `src/easyflow/web/static/assets/index-C7HaSvvp.js`。[计划](PLAN-20261124-001-model-unknown-review-fix-loop.md) · [原审查报告](../reviews/20261124-1823-REVIEW-model-unknown-c900362e-all-code.md)

## 发现与验收

| 编号 | 修复和验收证据 |
| --- | --- |
| R01 | Web 预览与执行拒绝定义变更，执行事务内再次确认。主文件、本地依赖和预览后变化三种情形均保留原状态及 XCom，恢复原定义后可重跑。[Web 回归](../../tests/test_web.py#L54) |
| R02 | 回收按尝试号和进程身份终止旧 worker。真实子进程断租后，第 1 次记为租约丢失，第 2 次成功，未追加错误的启动失败记录。[执行器回归](../../tests/test_local_executor.py#L18) |
| R03 | 依赖改变时废弃发现缓存，建单事务内重新核对加载指纹。手动触发执行新依赖，定时扫描在写入前发现变化时不占用逻辑日期，刷新后该日期成功执行。[Web 触发回归](../../tests/test_web.py#L100) · [定时回归](../../tests/test_review_runtime_consistency.py#L32) |
| R04 | 终态运行的旧队列不拦截可清理任务。定义改变导致运行失败后，恢复源文件，清理上游并成功向下游重新提供 XCom。[终态队列回归](../../tests/test_web.py#L113) |
| R05 | 等待同批前序完成后按日期重新推进，三天 `depends_on_past` 回补在并发上限为 2 时全部成功。[回补回归](../../tests/test_scheduler_backfill.py#L16) |
| R06 | worker 和 `dags test` 在实际任务期间保留已加载的本地模块，结束后恢复缓存。两条入口均能解析导入类引用的类型别名。[导入作用域回归](../../tests/test_local_executor.py#L111) |
| R07 | 启动超时禁止继续领取任务，退出时统一限制等待并在必要时强制终止。忽略 `SIGTERM` 的真实子进程在启动和执行期间都能及时停止。[超时及关闭回归](../../tests/test_local_executor.py#L62) |
| R08 | 普通运行占满容量时直接返回，单独的回补运行不占自动调度容量，建单事务仍检查上限。[容量回归](../../tests/test_review_runtime_consistency.py#L80) |
| R09 | 先读出可能过期的尝试再申请写锁。500 个任务在没有过期时触发 0 次恢复写事务，1 个过期时触发 1 次，活跃租约保持有效。[恢复回归](../../tests/test_review_runtime_consistency.py#L106) |
| R10 | 根据执行区间定位时间格后再去重聚合。25 个工作流各每分钟 1 次运行，每次尝试持续 1 秒。对同一内存 SQLite 中的 252,000 次运行查询最近 168 小时，基线耗时 23.377 秒，修复后为 4.724 秒，336 个格子的逐工作流结果完全一致。[时段查询](../../src/easyflow/web/uptime.py#L102) |
| R11 | 尚无任务尝试的现代运行显示等待。缺少任务记录的旧运行仍可使用运行区间回退。[等待状态回归](../../tests/test_web_uptime.py#L61) |
| R12 | 保留可识别的 `.zone` 或 `.key`，无法无损恢复的动态时区报告独立的 DAG 解析错误。纽约的当地 09:00 在冬夏季均按正确时钟调度。[时区回归](../../tests/test_schedule_regressions.py#L20) |
| R13 | `PythonOperator` 和 `@task` 明确拒绝异步函数或异步 callable。同步包装器返回异步对象时，`dag.test()` 与持久化执行均报告失败。[Python 任务回归](../../tests/test_dag_test.py#L27) |
| R14 | 有限调度按 `(dag_id, run_id)` 的最后状态决定退出码。真实调度同一运行先报告 `running`，重试后报告 `success` 并退出 0，另有跨 DAG 同名运行的失败用例。[退出码回归](../../tests/test_state_store_cli.py#L26) |
| R15 | 清理不存在的运行时，预览与执行均退出 1 并报告运行不存在。[清理命令回归](../../tests/test_state_store_cli.py#L15) |
| R16 | 按读取起点和已采样长度限制最多 1,000,000 字节。对低于和超过上限的日志模拟实时追加，返回长度和截断标志均符合采样时的内容。[日志回归](../../tests/test_web_history.py#L38) |
| R17 | 已选的失败格移出滚动窗口后，面板、原时段和第 2 次尝试均保留，直到主动收起。[浏览器回归](../../web/checks/frontend-review-loop.js) |
| R18 | 历史任务详情核对两个响应的定义指纹。先收到新工作流定义时隐藏未经匹配的上下游，日志继续可读。[浏览器回归](../../web/checks/frontend-review-loop.js) |

## 全量验证与终轮复审

- 后端 `pytest` 全量 342 项通过，包含 23 个新增回归用例。宿主磁盘不足造成基线 SQLite I/O 错误，正式全量验证在独立内存卷中完成。测试输出有 1 条 Starlette 对 `httpx` TestClient 的弃用提示。
- `pnpm test` 共 38 项通过，`pnpm build` 通过。浏览器的前端修复与导航脚本、真实隔离 FastAPI 实例的失败日志及最近小时和周月视图均通过。离线构建的 wheel 包含实际 HTML 入口引用的静态资源及保留的旧构建资源。[前端脚本](../../web/checks/) · [打包入口](../../pyproject.toml)
- `git diff --check` 和 Ruff `--select E4,E7,E9,F` 通过。默认 Ruff 0.17.4 基线和工作区均为 192 条诊断，按相对路径、规则和消息对比无新增，既有检查问题仍保持登记。[Ruff 已知问题](../known-issues/KI-20261108-2311-001-ruff-locked-baseline.md)
- 复审补查了建单写入边界、worker 关闭、容量检查与前端独立轮询。最终一轮重新检查累计代码的调度、任务执行、状态迁移、CLI、Web API、前端读取及打包入口，期间未修改代码，未发现本次 Review 范围内仍需修复的逻辑问题。修复与报告保存在当前工作区。
