# 执行边界六项 Review 循环修复结果

- 创建时间：2026-11-15 04:10 Asia/Shanghai。
- 执行模型：Codex（基于 GPT-6；环境未提供可核实的完整模型 ID）。
- 分析与实现版本：WORKTREE，基准提交 `e0b1fa4aa03d8a40b66b55a221b5e6e796b02822`；开始时工作区干净，结果尚未提交或推送。
- 关联计划：`docs/plans/PLAN-20261115-003-model-unknown-review-runtime-boundaries.md`。
- 范围：用户本轮六条 Review，以及验证中发现的必要关联执行边界；没有全仓新增功能或无关重构。

## 修复与验证依据

| 项目 | 实现 | 行为验证 |
| --- | --- | --- |
| 独立任务续租 | `src/easyflow/_internal/task_heartbeat.py:43` 启动独立解释器并等待握手；`src/easyflow/_internal/task_heartbeat.py:103` 用管道和父 PID 感知 worker 退出。`src/easyflow/_internal/persistent_runner.py:563` 在执行尝试时启用，`src/easyflow/_internal/persistent_runner.py:635` 在终态提交后 finally 回收。`src/easyflow/_internal/state_store.py:892` 按 owner、worker PID、try 校验身份，保留真实 worker PID。 | sequential/local 在 1 秒租约下执行 2 秒 ctypes.PyDLL 原生调用，外部恢复扫描不误判；心跳持续更新且 PID 等于任务实际返回值。真实 os._exit 崩溃后续租停止，租约过期后恢复并重试成功。延迟成功、失败和跳过提交时均持续续租；子进程、管道被回收；启动失败会结束尝试且不执行任务；错误 owner/PID/旧 try 不能续租。 |
| 数据库绝对位置 | `src/easyflow/_internal/state_store.py:228` 固定 path 与传递给子进程的规范化 database_url。 | 默认 URL、相对 SQLite URL 和裸路径，在初始化前或任务执行中 chdir 后仍连接原文件；重新构造 store 和 local worker 一致，不产生第二份数据库。 |
| 入队前校验 executor | `src/easyflow/_internal/persistent_runner.py:60` 定义 CLI 枚举及统一校验；`src/easyflow/_internal/persistent_runner.py:227` 在 runner 副作用前校验。`src/easyflow/cli.py:207`、`src/easyflow/cli.py:345`、`src/easyflow/cli.py:438` 约束三类命令，`src/easyflow/cli.py:526` 处理当前 Typer 的参数异常。`src/easyflow/_internal/scheduler.py:136`、`src/easyflow/_internal/scheduler.py:170`、`src/easyflow/_internal/scheduler.py:240`、`src/easyflow/_internal/scheduler.py:318`、`src/easyflow/_internal/scheduler.py:419`、`src/easyflow/_internal/scheduler.py:463` 覆盖调度入口与回补。 | CLI trigger、backfill、scheduler --once/常驻拒绝 typo，退出码 2；对应内部入口抛 ValueError。对数据库所有表的前后快照比较无 run、backfill、cursor、lease 或 config 写入，空 sources 也不能绕过。有效执行器由全量回归覆盖。 |
| 安全日志路径 | `src/easyflow/_internal/persistent_runner.py:676` 对 DAG/run/task 各组件编码；`src/easyflow/_internal/persistent_runner.py:689` 使用小写十六进制，长值回退 SHA-256。 | 支持的点名称、超长 DAG ID，以及绝对路径、..、斜杠、特殊字符、超长任务 ID 均在日志根目录下且 CLI/Web 可读。路径跳转不能合并两个任务日志；大小写不敏感文件系统的编码碰撞也被覆盖。旧记录继续按数据库中已存路径读取。 |
| XCom 快照 | `src/easyflow/_internal/persistent_runner.py:77` 在 push 时保存 JSON，pull 与提交值各自反序列化；`src/easyflow/_internal/state_store.py:1911` 在同一异常边界处理 JSON 与 UTF-8 编码。 | DAG.test/sequential/local 均保存 push 当时的嵌套可变对象；每次 pull 隔离，整数键和 tuple 按 JSON 归一化。非 JSON、超字节限制和非法代理字符在 push 当时拒绝，异常可捕获且不覆盖此前的值。失败与重试不泄漏由既有回归验证。 |
| chain 按位置连接 | `src/easyflow/sdk/operators/base.py:169` 预先物化参数并校验相邻列表长度，然后逐项配对，保留单项、扇出、扇入及 XComArg。 | 两条分支仅 a→c、b→d，一个分支失败不影响另一分支；长度错误在修改依赖之前报告；中间生成器只消费一次，tuple/XComArg 与首尾单任务行为正确。 |

新增回归集中在 `tests/test_review_runtime_boundaries.py:61`，共 53 项参数化用例。无数据库 schema 迁移；GOAL.md 未修改。

## 循环复审及必要关联修改

- 真实驱动验证发现本地源码刷新会清除正在运行的入口模块，导致 spawn 在任务执行前失败。在 `src/easyflow/_internal/source_imports.py:64` 保留 __main__/__mp_main__，独立模块身份用例及同目录真实驱动验证通过；其余本地依赖刷新逻辑保留。
- 首轮 Base64 方案存在只差大小写的路径组件，如 aaA/aa[ 编成 YWFB/YWFb；复审增加反例后改用小写十六进制和哈希，碰撞保护通过。
- 补齐 UTF-8 序列化异常类型一致性；三种执行方式验证通过。
- 首次全量为 235 项通过、2 项失败；失败来自既有 Web 测试假设首次异步发现必定在 0.5 秒内完成，隔离两项通过。`tests/test_web.py:12` 改为按 loading 状态在 5 秒内等待，保留真实错误断言，不改变 Web 行为。
- 最后一轮从实际代码重新覆盖六项、续租资源生命周期、入口模块、CLI 错误传播、历史日志读取、失败重试及测试等待逻辑，没有再修改代码，未发现本次范围内待修复的逻辑问题；必要验证通过。这不表示仓库不存在其他已知问题。

## 实际验证

- `uv run --extra dev pytest -q`：最终 **240 项通过**，耗时 172.48 秒；有一条既有 Starlette/httpx 弃用警告。
- `uv run --extra dev pytest -q tests/test_review_runtime_boundaries.py tests/test_web.py`：**55 项通过**。
- 将基准提交导出到隔离目录，对 47 项兼容的新回归用例运行：**36 项按预期失败、11 项保护用例通过**，覆盖全部六项原始缺陷和入口模块问题。基准不存在新增续租模块，因此仅在隔离测试副本去掉对该模块的顶层导入，6 项新增监督进程/身份契约用例仅在修复版本验证；未修改基准源码。
- `pnpm test`（web）：**5 项通过**；`pnpm build`：通过。没有前端源码变化，构建删除的既有已跟踪历史静态资源已恢复，最终无前端产物差异。
- `uv run ruff check --select E4,E7,E9,F src tests examples`：通过；新增续租模块和新增测试文件的默认 Ruff 检查通过；`git diff --check` 通过。
- 使用同一环境按文件、规则和信息计数对比默认 Ruff JSON：基准 195 项，当前 **194 项，新增 0 项**；减少一条 chain 的 RUF007。默认全量检查仍以非零状态退出，沿用 `KI-20261108-2311-001`，没有调整 lint 策略。

## 工作区、偏差和遗留

本轮的必要关联扩展与设计调整已记录在计划中：入口模块保留、大小写安全编码、UTF-8 异常和异步测试等待。原始六项均已解决，没有新增仍待修复的长期事项；现有已知问题保持原范围，Ruff 条目追加验证记录。

相关 WORKTREE 文件：`src/easyflow/_internal/{persistent_runner,scheduler,source_imports,state_store,task_heartbeat}.py`、`src/easyflow/cli.py`、`src/easyflow/sdk/operators/base.py`、`tests/test_review_runtime_boundaries.py`、`tests/test_web.py`、本计划及结果、`docs/plans/INDEX.md`、`docs/KNOWN-ISSUES.md` 和既有 Ruff 条目。尚未提交或推送。
