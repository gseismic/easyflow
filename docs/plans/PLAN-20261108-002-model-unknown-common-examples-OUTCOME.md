# Airflow 常见场景示例结果

- 完成时间：2026-11-08 16:40 Asia/Shanghai
- 执行模型：Codex，基于 GPT-6；环境未提供实际模型 ID。
- 实施范围：`WORKTREE`，基准提交 `9d967b659645ee34f3669212b7fd5b906173a3af`；新增和修改的示例、测试、文档及问题记录尚未固定为提交。任务开始时已有的 `GOAL.md`、`.easyflow/` 和 `data/` 均未删除或覆盖。
- 对应计划：`docs/plans/PLAN-20261108-002-model-unknown-common-examples.md`。

## 完成情况

- 新增并整理 12 个分级编号示例，覆盖手动、TaskFlow ETL、每日和 Cron、一次调度、依赖分发汇合、触发规则、Bash/Python Operator、运行参数、重试和量化分区。
- 原 `examples/quant_data_dag.py` 更名为 `examples/501_quant_daily_prices.py`，保留 `quant_daily_prices` DAG ID；设置固定的带时区开始时间与 `catchup=False`，支持显式 `trade_date`，并处理 Airflow 手动运行可能没有逻辑日期的情况。
- 每个示例默认使用 easyflow，可设置 `EASYFLOW_EXAMPLE_ENGINE=airflow` 切换到 Airflow 3.2 公共导入。`examples/README.md` 记录编号、运行方法、场景、时间语义、调度副作用和已确认差异；根目录 `README.md` 的命令已指向新路径。
- 已确认并记录 `KI-20261108-1637-001`：逻辑日期早于开始时间的 DAG 测试在两个引擎表现不同；本次没有修改调度或执行核心。

## 计划偏差

- Airflow 的 DAG 测试在逻辑日期早于开始时间时可能报告成功但不执行任务。为避免空运行误判，量化示例的开始时间由原计划的下一刻度调整为当日 UTC 08:00；该时间仍在下一个每日刻度之前，不会因本次更改而立即自动写入已有数据。Airflow 验证增加了实际任务状态与输出检查。
- 初次未使用官方依赖约束的 Airflow 环境出现依赖冲突；随后使用 Airflow 3.2.0、Python 3.12 及官方约束文件重建隔离环境并完成验证。

## 验证结果

- easyflow：`uv run --offline --no-project --with-editable . --with 'pytest==9.0.2' --with 'httpx==0.28.1' pytest -q`，122 项通过；`ruff check src tests examples` 通过。
- Airflow 3.2.0：12 个文件均通过 safe mode 筛选和 DAG 解析；使用独立临时状态库对 12 个 DAG 执行 `airflow dags test`，确认实际任务状态；触发规则示例的上游及 `all_success` 下游为 skipped，`all_done` 下游为 success。ETL、Bash、PythonOperator、参数覆盖和量化示例的输出与预期一致，量化文件写入临时目录。
- 无逻辑日期的两个示例分支通过模拟任务上下文验证；上述 Airflow 端到端测试使用了明确的逻辑日期，未实际启动 REST API 进行无逻辑日期触发。
- 在无开始时间、无自动运行历史且 `catchup=False` 的条件下，使用固定时钟分别对比 Airflow 3.2 CronTriggerTimetable 和 easyflow：`@daily` 在 UTC 00:01 选择当天 00:00，UTC 12:00 等待次日；此验证不等同于覆盖所有 timetable 配置。
- Web API 返回 12 个 DAG、零解析错误且调度器在线；示例中的每日、工作日和一次性 DAG 已被当前调度器发现并执行演示任务。量化 DAG 的旧运行仍可查看，原 `data/prices/symbol=DEMO/date=2026-11-07/data.json` 未被覆盖。
- 验证 README 本地链接、回补预览命令及 `git diff --check` 均通过。

## 遗留事项

- `KI-20261108-1637-001` 未解决；该问题的持久化手动运行及其他 Airflow 版本尚未核实。现阶段对照测试应使用不早于 `start_date` 的逻辑日期，并核对实际任务状态或输出。
- Airflow 手动触发无逻辑日期、time zone 及不同 timetable 的完整组合没有全量运行验证；示例文档限定了已验证范围。旧量化运行重跑可能受源码版本保护影响。
