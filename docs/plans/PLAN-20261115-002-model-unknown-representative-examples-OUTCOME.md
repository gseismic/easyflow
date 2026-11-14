# 代表性示例优化结果

- 完成时间：2026-11-15 02:05 Asia/Shanghai。
- 执行模型：Codex，基于 GPT-6；环境未提供可核实的完整模型 ID，沿用计划中的 `model-unknown`。
- 实施范围：`WORKTREE`，基准提交为 `d0aaeaffe72063b23c60dcc1655d8e3f8aa26295`。相关变更为 `examples/002_basic_taskflow_etl.py`、`examples/301_operator_bash.py`、`examples/402_runtime_retry.py`、`examples/501_quant_daily_prices.py`、删除的两个项目标签示例、`examples/README.md`、`README.md`、`tests/test_examples.py`、本次计划和结果、计划索引，以及既有 Ruff 问题的追加验证记录与索引。内容尚未固定为新提交。
- 对应计划：`docs/plans/PLAN-20261115-002-model-unknown-representative-examples.md`。
- 完成状态：示例优化与 easyflow 侧验证完成；Airflow 侧本次实测因依赖下载超时未完成。

## 完成内容

- 移除流程相同的 `003_basic_project_quant.py`、`004_basic_project_analytics.py`，其余文件编号及 DAG ID 保持不变，示例总数由 14 收敛为 12。项目筛选由真实流程承载：ETL 使用 `project:analytics`（`examples/002_basic_taskflow_etl.py:17`），行情分区使用 `project:quant`（`examples/501_quant_daily_prices.py:21`）。
- Bash 示例通过 `params` 和逻辑日期模板生成环境变量，在固定命令中用带引号的变量展开，再将最后一行交给 TaskFlow；缺少逻辑日期时显示“未提供”。入口与关键配置见 `examples/301_operator_bash.py:26`。
- 重试示例新增准备任务，为每次运行生成独立临时标记；调用任务删除标记并模拟首次失败，第二次恢复成功。`fail=true` 保持三次尝试后失败，两条正常执行路径均无标记残留，也没有成功清理叶任务掩盖失败。实现见 `examples/402_runtime_retry.py:27`。
- 示例索引补充阅读顺序、参数命令、预期输出、临时资源和本机执行前置条件；根 README 的项目链接同步改为 ETL、行情示例。当前使用说明无指向已删除文件的链接，测试也已移除对应执行入口。
- 示例执行测试改为先复制单个文件到独立目录；新增 shell 特殊字符作为数据传入的检查，以及 LocalExecutor 跨任务进程的重试恢复、耗尽和标记清理验证。未修改库 API、调度器或执行器。

## 验证结果

- `uv run --no-sync pytest -q -p no:cacheprovider`：187 项通过，用时 100.05 秒。包含全部示例测试和 LocalExecutor 实际重试验证；另有一条既有的 Starlette/httpx 弃用提示。
- 12 个 DAG 分别复制到只包含自身示例文件的临时目录，通过公开 `easyflow dags test` 执行；另验证参数覆盖、重试耗尽、显式业务日期和 shell 特殊字符输入，共 16 次 CLI 验证均符合预期。量化文件内容和临时文件清理同时核对，没有使用项目原有的状态库或数据目录。
- `uv run --no-sync ruff check --select E4,E7,E9,F src tests examples`：通过。默认 Ruff 0.17.4 检查仍失败；同版本按相对文件、规则和信息对比，基准 197 项、当前 195 项、新增 0 项，减少的两项来自删除重复示例。既有问题 `KI-20261108-2311-001` 已追加本次验证，仍为未解决。
- `git diff --check`：通过。根 README 与示例 README 的本地文件链接检查通过，没有指向被移除示例的残留链接。
- 代码自查确认：示例仅使用公开接口和已声明依赖；重试状态不依赖进程内全局变量，持续失败没有被下游成功状态掩盖；Bash 参数未拼入命令源码；未改动 `GOAL.md`、用户状态库、业务数据或正在运行的服务。

## 计划偏差与剩余限制

- Airflow 3.2.0 未在本机环境或 uv 缓存中安装。本次成功取得官方 Python 3.12 约束文件，但隔离安装先后在 `cron-descriptor==2.0.8`、`apache-airflow-providers-standard==1.12.1` 的 wheel 下载时超时；第二次使用官方 PyPI 并将 HTTP 超时从 30 秒增至 90 秒仍失败。因此本次不能声明优化后的版本已在 Airflow 实际运行，需网络恢复后按 `examples/README.md` 的隔离方式复核 Bash 和重试场景。
- 重试模拟要求准备和调用任务共享本机临时目录。若在准备任务完成后、首次调用前中断，需按准备任务返回路径清理标记；这个前置条件和清理方式已写入示例说明。
- 已删除 DAG 不再参与新运行发现；本次没有迁移或删除既有运行。源码变更后的旧运行仍受现有版本一致性检查约束，应使用更新后的 DAG 创建新运行。
- 无新增长期缺陷或待办；既有 Ruff 基线问题仍按原条目跟踪。未提交或推送本次工作。
