# PLAN-004 恢复与审计操作实现计划

> 日期：2026-07-06
>
> 设计依据：`docs/design/easyflow-20260706-airflow-lite.md`
>
> 前置结果：`docs/dev/PLAN-003-scheduler-backfill-OUTCOME.md`

## 1. 目标

补齐影响生产稳定性的核心保护：

- scheduler lease，防止同一状态库误启动多个 scheduler；
- DAG definition hash 不一致默认停止历史 run；
- `tasks clear` 危险操作的 dry-run、确认、状态迁移和审计记录。

## 2. 范围

本计划覆盖：

- StateStore scheduler lease 获取、续租和释放；
- `easyflow scheduler --once` 默认获取 lease；
- 非过期 lease 存在时拒绝启动；
- 持久化 run 恢复时校验当前 definition hash；
- hash 不一致时把 DagRun 标记为 `failed` 并写入审计；
- `easyflow tasks clear DAG_ID --run-id RUN_ID`；
- `tasks clear --task-id TASK_ID`；
- `tasks clear --dry-run`；
- `tasks clear --yes` 非交互执行；
- clear 删除当前 run 的 XCom，保留 TaskTry 和日志；
- clear 终态 DagRun 时把 DagRun 恢复为 `queued`；
- clear 写入 audit_log。

本计划暂不覆盖：

- queued/running 任务强制终止；
- Web UI 审计展示；
- DAG source bundle 版本化；
- 多 scheduler HA。

## 3. 验收标准

- `python -m pytest` 通过；
- 有效 scheduler lease 存在时第二个 scheduler 获取失败；
- lease 过期后可以重新获取；
- definition hash 不一致的历史 run 不执行新代码；
- `tasks clear --dry-run` 不修改状态；
- `tasks clear --yes` 清空选中任务状态和 XCom，保留 TaskTry；
- clear 后 DagRun 变为 `queued` 并可重新执行；
- audit_log 记录 clear 和 definition hash 拦截。
