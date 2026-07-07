# easyflow

easyflow 是一个单机、持久化、可调度的 Airflow 兼容子集，目标是支撑本地 Python 环境中的
DAG 编排、量化数据抓取和历史回补。

当前实现状态：

- 已实现 `easyflow.sdk` 公共入口；
- 已实现 DAG context、`@dag`、`@task`、`XComArg`、`dag.test()`；
- 已实现 `PythonOperator`、`BashOperator`、`EmptyOperator`；
- 已实现严格模板渲染、JSON XCom 限制和基础 trigger rule；
- 已实现 SQLite 状态库初始化：`easyflow db init`；
- 已实现持久化手动运行：`easyflow dags trigger DAG_ID --file path/to/dag.py`；
- 已实现 DAG 目录发现：`easyflow dags list --dags-folder dags`；
- 已实现状态和日志查询：`easyflow dags state`、`easyflow tasks state`、`easyflow tasks logs`；
- 已实现一次性 scheduler：`easyflow scheduler --once --dag-id DAG_ID --file path/to/dag.py`；
- 已实现目录级 scheduler：`easyflow scheduler --once --dags-folder dags`；
- 已实现同步 backfill：`easyflow backfill create --dag-id DAG_ID --file path/to/dag.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD`。
- 已实现基于 `croniter` 的 5 字段 cron schedule，如 `DAG("market_job", schedule="30 21 * * 1-5")`；
- 已实现 scheduler lease，避免同一 SQLite 状态库误启动多个 scheduler；
- 已实现 Task lease、heartbeat 和 orphaned running task 恢复；
- 已实现 LocalExecutor 多进程执行：`--executor local --parallelism N`；
- 已实现单个运行内的 `parallelism`、`max_active_tasks` 和 `max_active_runs` 基础限流；
- 已实现 DAG definition hash 恢复拦截，历史 run 默认不会悄悄切换到新代码；
- 已实现审计型 `tasks clear`，支持 `--dry-run` 和 `--yes`。

本地开发：

```bash
python -m pip install -e .
python -m pytest
```

不安装时可以使用：

```bash
PYTHONPATH=src python -m pytest
PYTHONPATH=src python -m easyflow.cli db init
```

触发示例 DAG：

```bash
PYTHONPATH=src:. python -m easyflow.cli dags trigger quant_daily_prices \
  --file examples/quant_data_dag.py \
  --logical-date 2026-07-06T00:00:00+00:00 \
  --executor local \
  --parallelism 4
```

执行一次 scheduler 扫描：

```bash
PYTHONPATH=src:. python -m easyflow.cli scheduler --once \
  --dag-id quant_daily_prices \
  --file examples/quant_data_dag.py \
  --now 2026-07-08T12:00:00+00:00
```

扫描 DAG 目录并调度所有 DAG：

```bash
PYTHONPATH=src:. python -m easyflow.cli dags list --dags-folder examples

PYTHONPATH=src:. python -m easyflow.cli scheduler --once \
  --dags-folder examples \
  --now 2026-07-08T12:00:00+00:00
```

执行历史回补：

```bash
PYTHONPATH=src:. python -m easyflow.cli backfill create \
  --dag-id quant_daily_prices \
  --file examples/quant_data_dag.py \
  --start-date 2026-07-01 \
  --end-date 2026-07-05
```

清理已完成任务并允许重跑：

```bash
PYTHONPATH=src:. python -m easyflow.cli tasks clear quant_daily_prices \
  --run-id manual__2026-07-06T00:00:00+00:00 \
  --task-id fetch_daily_prices \
  --dry-run

PYTHONPATH=src:. python -m easyflow.cli tasks clear quant_daily_prices \
  --run-id manual__2026-07-06T00:00:00+00:00 \
  --task-id fetch_daily_prices \
  --yes \
  --reason "修复上游数据后重跑"
```

当前限制：

- scheduler 支持 `--once` 和常驻循环，可按单文件或 DAG 目录扫描；
- schedule 当前支持 `@once`、`@hourly`、`@daily`、`@weekly`、`@monthly`、`@yearly`、`timedelta` 和 `croniter` 驱动的 5 字段 cron；
- cron 当前故意不接受秒字段和年份字段；当前 scheduler 是分钟级调度，秒级 schedule 需要单独设计；
- LocalExecutor 仅限单机本地进程，不支持远程 Worker；
- 强制终止 OS 进程仍未实现。
