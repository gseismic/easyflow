# 工作流刷新完整性提示修复结果

- 创建时间：2026-11-17 03:53 Asia/Shanghai。
- 执行模型：Codex（基于 GPT-6；环境未提供可核实的完整模型 ID，使用 `model-unknown`）。
- 完成版本：`WORKTREE`，基准完整提交 `64597a920303d75f8625c868b7f377b519443c66`；改动尚未固定为提交，未提交或推送。
- 相关未提交文件：`src/easyflow/_internal/dag_parser.py`、`tests/test_web_discovery_refresh.py`、`pyproject.toml`、`uv.lock`、`web/package.json`，以及本计划、结果、计划索引、交接和既有 Ruff 问题记录。
- 关联计划：`docs/plans/PLAN-20261117-001-model-unknown-discovery-refresh.md`。
- 状态：实施与验证已完成；现有业务 Web 进程未重启，运行中的旧后端须重启才会加载修复。

## 原因与完成内容

异步发现每 5 秒重解析文件，同时保留未变更文件的快照。原来的 `pending=bool(self.pending)` 把仍有后台解析等同于工作流缺失，导致 Web 的列表、导航和统计在完整数据仍可用时反复显示不完整提示。旧版隔离浏览器已实际观察到 `loading=true`、facets `complete=false` 和对应页面提示。

- `src/easyflow/_internal/dag_parser.py:45`、`src/easyflow/_internal/dag_parser.py:144`：明确 `pending` 表示存在尚无当前文件快照的文件。已有同签名快照的后台刷新保持完整，成功解析但没有 DAG 的辅助文件也视为已完成。
- `src/easyflow/_internal/dag_parser.py:116`：有限等待仅针对没有可用快照的文件；正常后台更新直接返回已有快照，后台解析继续运行。
- `src/easyflow/_internal/dag_parser.py:88`、`src/easyflow/_internal/dag_parser.py:104`：保留签名核对、变更失效和解析错误的既有处理。首次、新增、修改文件仍提示加载；旧签名结果不能替代新文件结果；真实 issues 仍令 facets 不完整。
- `tests/test_web_discovery_refresh.py:21`：新增受控时间和 Future 回归，覆盖首次发现、后台刷新、变更期间旧结果返回、新增/移除、空文件、解析失败/异常及恢复；同一集成用例核对 catalog、工作流/历史列表和两类 facets。
- 版本按 `feature-versioning` 从 `0.1.0` 更新至 `0.1.1`，同步 `pyproject.toml`、`uv.lock` 与 `web/package.json`。pnpm 锁文件没有项目版本字段，无须改动；没有更换依赖。

## 验证

| 检查 | 结果 |
| --- | --- |
| 修复前新增回归 | 7 项失败、1 项通过；失败明确发生在已有完整快照的后台刷新或错误重试被误标 pending 的分支 |
| `uv run pytest -q tests/test_web_discovery_refresh.py` | 修复后 8 项通过 |
| `uv run pytest -q tests/test_web.py tests/test_web_auth.py tests/test_web_history.py tests/test_web_project_search.py tests/test_cron_dag_discovery.py tests/test_scheduler_reliability.py tests/test_review_runtime_boundaries.py` | 100 项通过，覆盖相关 Web、发现、调度和运行边界；仅有既有 Starlette/httpx 弃用提示 |
| `pnpm --dir web test` | 37 项通过 |
| `uv run easyflow dags list --dags-folder examples` | 实际发现 12 个示例 DAG，没有解析错误；仅发现，没有触发任务 |
| 新测试文件默认 Ruff | 通过 |
| 修改解析器与测试的 `ruff check --select E4,E7,E9,F` | 通过 |
| 解析器默认 Ruff 与基准对比 | 基准与当前均有既有 `BLE001`、`S102` 各一条，新增 0；默认检查仍未通过，关联 `KI-20261108-2311-001`，不描述为全部 lint 通过 |
| `uv lock --check` | 通过；锁文件仅项目自身版本发生变化 |
| `git diff --check` | 通过 |

版本同步时首次 `uv lock --offline` 因跨 Python 版本解析所需的缓存不全而失败，随后正常 `uv run` 完成同步并通过 `uv lock --check`，没有修改支持的 Python 范围或依赖版本。

真实浏览器使用仓库外隔离状态库、只读账号和导入时延迟 1 秒的 DAG，服务为 `http://127.0.0.1:8792`、`--no-scheduler`。先在旧进程复现，再只重启该隔离进程加载修复：

- 初始化收敛后监测约 20 秒，跨多个解析周期、3 次手动刷新及页面重载；catalog/列表保持 `loading=false`，facets 保持 `complete=true`，没有再次出现不完整提示。
- 隔离目录新增抛出异常的文件后，页面显示实际错误和导航不完整提示，facets 为 `complete=false, pending=false`。
- 将错误文件改为正常辅助文件后，错误消失、匹配数量恢复为 1，facets 为 `complete=true, pending=false`，catalog 无 issues。
- 以上浏览器上下文没有 pageerror，没有触发、暂停、回补或清理等业务写请求；测试只使用隔离登录。

首次浏览器观察曾在工作流列表初始加载尚未收敛时开始记录，捕获一次真实的初次加载提示；补充等待列表及导航全部就绪后重新观察，稳定期提示为 0。这不是周期刷新回归。隔离服务重启后有一次未就绪连接被拒绝，确认 HTTP 200 后重跑成功。

## 自查、偏差和边界

实施后复核了文件签名失效、过期 Future 丢弃、空结果、后台错误与恢复、facets 缓存键变化，以及 CLI 调度循环的使用方式；没有发现本次仍需修复的问题。无实质范围偏差，没有修改前端实现、构建产物、数据库、调度规则或 `GOAL.md`。前端源码未变，因此没有重复构建。

本次确认的刷新误报已解决，没有新增需长期跟踪的事项。既有默认 Ruff 问题追加了本轮证据，主索引同步更新时间，状态仍为未解决；其他既有问题与待办见 `docs/KNOWN-ISSUES.md`、`docs/TODOS.md`。

测试用 `refresh` 浏览器会话和 `8792` 隔离服务已关闭，本轮生成的仓库内浏览器快照已移除；仓库外夹具位于 `/var/folders/gh/54w67t6n7_b8pv7t76xw5_qm0000gn/T/easyflow-refresh-check-ro5s4s7f/`，其中凭据仅供该隔离状态库使用，不纳入 Git。现有 `8790` Web 进程没有重启或改变其 scheduler；需要按原启动参数重启 Web 才能应用后端修复。收尾核对 94 个 Markdown 本地链接均存在，三处版本均为 `0.1.1`，差异检查通过。
