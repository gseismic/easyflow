# KI-20261108-1637-001：DAG 测试日期早于开始时间时仍执行任务

- 创建时间：2026-11-08 16:37 Asia/Shanghai
- 创建模型：Codex，基于 GPT-6；环境未提供实际模型 ID。
- 初始状态：未解决。
- 来源：用户要求核对 Airflow 3.2 常见用法和可在两个引擎运行的示例；见 `docs/plans/PLAN-20261108-002-model-unknown-common-examples.md`。
- 确认版本：`WORKTREE`，基准提交 `9d967b659645ee34f3669212b7fd5b906173a3af`；代码依据为该基准的 `src/easyflow/sdk/dag.py`、`src/easyflow/sdk/testing.py`，复现使用本次未提交的 `examples/501_quant_daily_prices.py`。工作区内容尚未固定为提交。
- 引入提交：未定位；修复版本：无。

## 问题、触发条件与影响

对设置了 DAG `start_date` 的工作流，使用 `DAG.test()` 或 `easyflow dags test --logical-date` 指定早于 `start_date` 的逻辑日期时，easyflow 的内存测试仍执行任务。与之对照，在 Airflow 3.2.0 的相同示例和逻辑日期下，`airflow dags test` 返回成功，但没有执行任务。

因此，同一个示例在两个引擎中的测试结果及外部副作用可能不同：easyflow 可能写入文件或调用外部服务，而 Airflow 这次对照没有执行任务。此记录仅覆盖已复现的 DAG 测试入口；持久化手动运行、任务级开始时间和其他 Airflow 版本尚未核实。

## 确认依据

- 示例 `examples/501_quant_daily_prices.py:14` 设置了 `2026-11-08 08:00 UTC` 的 `start_date`；测试逻辑日期为 `2026-07-04 00:00 UTC`，两个引擎均传入 `trade_date=2026-07-02` 和独立临时输出目录。
- `easyflow dags test` 报告成功，且在临时目录产生 `symbol=DEMO/date=2026-07-02/data.json`，确认任务实际执行。
- 按 Airflow 3.2.0 官方约束安装的隔离环境中，`airflow dags test` 报告 `state=success`，日志没有 `[DAG TEST] running task`，临时输出目录也没有创建；同一 Airflow 环境使用晚于 `start_date` 的逻辑日期时，任务确实执行并生成预期文件。
- `src/easyflow/sdk/dag.py:218` 的 `DAG.test()` 将逻辑日期直接传给 `run_dag_in_process`；`src/easyflow/sdk/testing.py:94` 的测试执行循环按依赖状态选择任务，没有使用 DAG `start_date` 过滤。

## 临时绕过

对照测试时，选择不早于 DAG `start_date` 的逻辑日期，并检查任务状态或输出文件，不只检查 DAG 成功状态。需要处理更早的业务日期时，可在示例的运行参数中单独指定 `trade_date`。
