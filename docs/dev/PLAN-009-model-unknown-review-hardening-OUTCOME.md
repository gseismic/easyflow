# PLAN-009 调度与执行审查修复结果

完成时间：2026-11-08 07:40 Asia/Shanghai；执行模型：Codex，环境未提供具体模型 ID（model-unknown）。验证版本：WORKTREE，基准提交 `6d0bded933b5b7fc63291fdd588b54d3ce770a9b`；验证和编写本记录时，本次代码、测试和文档尚未提交，后续提交信息以 Git 历史为准。工作区原有的 `AGENTS.md` 修改未纳入本次变更。

## 实施结果

30 条审查意见均已处理。运行创建、容量判断和回补重处理判断使用 SQLite 写事务；scheduler 先推进已有活跃运行，常驻期间由独立子进程续租，创建运行和领取任务时进行租约栅栏检查。时区和重试基线持久化，旧数据库根据审计恢复已清除任务的重试预算。同步回补在容量不足或运行等待时显式报告未完成。任务上下文、终态、日志、Worker、DAG 解析和 SDK 行为已对齐。

设计与接口边界见 `docs/design/easyflow-20261108-reliability.md`，用户操作和时间语义已补充至 `README.md`。实施期间找到并阅读 `api-design-zh` 的实际副本；原失效符号链接未修改。

## 审查意见逐项核对

下表中的 `S`、`R`、`E` 分别表示 `tests/test_schedule_regressions.py`、`tests/test_scheduler_reliability.py`、`tests/test_execution_regressions.py`；测试函数名称均省略前缀 `test_`。

| 序号 | 审查问题 | 主要回归证据 |
|---|---|---|
| 1 | 恢复未完成 DagRun | R `scheduler_resumes_cleared_and_orphaned_runs` |
| 2 | 执行和空闲期间续租、丢租约停调度 | R `scheduler_renewal_survives_task_holding_gil`、`long_task_keeps_scheduler_lease_and_owner_is_exclusive`、`daemon_keeps_lease_during_idle_interval`、`transaction_fence_rejects_claim_after_lease_replaced`、`local_executor_stops_launching_tasks_after_lease_loss`、`renewal_process_stops_when_scheduler_parent_dies` |
| 3 | timedelta 稳定锚点 | S `timedelta_scheduler_does_not_create_a_run_each_scan`、`timedelta_uses_stable_anchor_and_start_end_constraints` |
| 4 | 无 start_date 的 @once 去重 | S `once_is_deduplicated_and_backfill_terminates` |
| 5 | @once backfill 有界迭代 | S `once_is_deduplicated_and_backfill_terminates` |
| 6 | DAG 开始、结束边界 | S `boundaries_are_not_floored_before_start_and_stop_at_end`、`timedelta_uses_stable_anchor_and_start_end_constraints` |
| 7 | DAG 时区调度及回补 | S `non_utc_scheduler_and_backfill_context`、`backfill_intervals_follow_local_day_across_dst` |
| 8 | 持久化与测试的业务日期一致 | S `manual_context_preserves_timezone_and_saved_interval`、`local_executor_preserves_named_timezone_across_dst` |
| 9 | 使用持久化数据区间 | S `manual_context_preserves_timezone_and_saved_interval`、`non_utc_scheduler_and_backfill_context` |
| 10 | 回补容量不足明确未完成 | R `backfill_capacity_and_cli_trigger_report_incomplete` |
| 11 | depends_on_past 等待保持非终态 | R `depends_on_past_waits_nonterminal_until_previous_succeeds` |
| 12 | 容量判断与运行创建原子化 | R `capacity_is_atomic_for_simultaneous_manual_creations`、`backfill_and_manual_capacity_compete_across_processes`、`two_backfills_do_not_race_reprocessing_none` |
| 13 | 手动触发遵守 DAG 上限 | R `backfill_capacity_and_cli_trigger_report_incomplete`、`backfill_and_manual_capacity_compete_across_processes` |
| 14 | 每个 Local Worker 校验定义版本 | E `local_worker_rejects_changed_source_between_tasks` |
| 15 | Worker 领取任务前异常退出 | E `local_worker_startup_error_does_not_restart_forever` |
| 16 | clear 在写事务中重新验证状态 | E `clear_rechecks_under_write_lock_and_preserves_xcom` |
| 17 | 同时应用 DAG 和回补上限 | R `backfill_limit_counts_its_own_runs_and_dag_limit_counts_all`、`backfill_capacity_and_cli_trigger_report_incomplete` |
| 18 | 持久化延迟、指数退避及上限 | E `retry_delay_backoff_and_clear_resets_budget`、`retry_at_is_persisted_and_enforced`、`explicit_no_backoff_is_not_overridden_by_default_args` |
| 19 | clear 后重新分配重试预算 | E `retry_delay_backoff_and_clear_resets_budget`、`upgrade_recovers_retry_baseline_from_clear_audit` |
| 20 | 尊重 do_xcom_push | E `leaf_outcome_and_xcom_disabled_match_test_runner` |
| 21 | 按叶子节点计算运行终态 | E `leaf_outcome_and_xcom_disabled_match_test_runner`、`clear_reactivation_respects_capacity_and_finish_rechecks_state` |
| 22 | 测试 XCom 读取隔离 | E `xcom_objects_are_isolated_and_sets_resolve_in_both_runners` |
| 23 | 异常写入可查询日志 | E `error_and_template_traceback_in_logs_and_run_id_paths_unique`、`retry_delay_backoff_and_clear_resets_budget` |
| 24 | run_id 日志路径无碰撞 | E `error_and_template_traceback_in_logs_and_run_id_paths_unique`、`existing_database_upgrades_with_utc_fallback` |
| 25 | 隔离同文件无效 DAG | E `file_with_invalid_dag_keeps_valid_one` |
| 26 | 显式空 Bash 环境隔离 | E `bash_explicit_empty_env_and_bash_syntax` |
| 27 | 显式使用 Bash | E `bash_explicit_empty_env_and_bash_syntax` |
| 28 | 拒绝显式 DAG 与 context 冲突 | E `explicit_dag_must_match_context` |
| 29 | 解析集合中的 XComArg | E `xcom_objects_are_isolated_and_sets_resolve_in_both_runners` |
| 30 | cron 避免夏令时无效时刻及非单调区间 | S `cron_skips_spring_gap_and_orders_both_fall_folds`、`backfill_intervals_follow_local_day_across_dst` |

## 验证与复查

- `pytest -q`：87 项通过；包含进程间容量竞争、独立续租进程、持有 GIL 的长任务、父进程意外退出、夏令时、旧库迁移和日志回读。
- `ruff check src/easyflow tests`：通过；本次修改的代码和新增测试通过 `ruff format --check`。
- `python -m compileall -q src`、`git diff --check`：通过。
- 已进行代码复查并修复发现的收尾与 clear 竞争、闲置期间租约释放、命名时区在 Local Worker 中丢失、旧库 retry 基线及回补策略竞争。
- 既有 `test_scheduler_respects_max_active_runs` 已调整为由其他执行进程持有运行锁的情形；此前将普通 queued 运行视为永久不可恢复，与本次要求冲突。

## 计划偏差与剩余边界

- 追加了运行锁、完成态事务复核、旧库审计基线恢复和独立续租子进程，以闭合修复期间确认的竞争窗口；计划及设计已同步更新。
- POSIX 文件锁和 Bash 在当前 macOS 环境验证；Windows 文件锁分支通过模拟测试，尚未在 Windows 实机验证。
- 任务已有外部副作用不能因租约丢失而撤销，需由业务任务提供幂等性；同步执行会等待配置的重试延迟。本项目仍为单机 SQLite 设计，不承诺跨主机文件锁。
