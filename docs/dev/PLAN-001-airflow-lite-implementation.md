# PLAN-001 Airflow Lite 实现计划

> 日期：2026-07-06
>
> 设计依据：`docs/design/easyflow-20260706-airflow-lite.md`
>
> 目标：实现 easyflow v0.1 的首个可验证版本，优先交付 M0，并建立 M1 所需的持久化状态基础。

## 1. 范围

本计划覆盖：

- Python 包结构和安装元数据；
- `easyflow.sdk` 公共入口；
- DAG 三种声明方式；
- `BaseOperator`、`PythonOperator`、`BashOperator`、`EmptyOperator`；
- TaskFlow `@task`、`XComArg` 自动依赖和 `.override()`；
- `>>`、`<<`、`chain()` 依赖构建；
- DAG 验证和 `dag.test()` 顺序执行；
- Context、严格模板渲染、JSON XCom 限制；
- SQLite 状态库基础 schema；
- `easyflow db init` 和最小 CLI 骨架；
- 覆盖核心行为的 pytest 测试。

本计划暂不完整实现：

- 常驻 scheduler；
- LocalExecutor 多进程；
- cron/timedelta 自动调度；
- backfill 执行器；
- `tasks clear` 审计操作；
- DAG 文件隔离解析。

这些能力依赖 M0 的定义模型和 M1 的状态库契约，后续计划继续推进。

## 2. 实施步骤

1. 创建包结构：`src/easyflow`、`tests`、`pyproject.toml`。
2. 实现公共异常、枚举、Context 和 DAG 上下文栈。
3. 实现 DAG / BaseOperator / 标准 Operator / TaskFlow。
4. 实现 `dag.test()` 的顺序执行器、XCom 存取、模板渲染和 trigger rule 子集。
5. 实现 SQLite `StateStore` 初始化和基础表结构，先作为 M1 的可迁移骨架。
6. 实现 CLI：`easyflow db init`、`easyflow dags test` 的基础入口。
7. 添加测试：TaskFlow、依赖、模板、XCom、多输出、失败重试、BashOperator、SQLite 初始化。
8. 复审实现与设计稿偏差，生成结果文档。

## 3. 验收标准

- `python -m pytest` 通过；
- 示例 DAG 可通过 `dag.test()` 顺序执行；
- TaskFlow 调用在解析期不执行函数，运行期通过 XCom 传参；
- JSON XCom 拒绝不可序列化对象和超限值；
- 模板引用不存在变量时失败；
- `easyflow db init` 能创建 SQLite 状态库基础表；
- 未实现能力以明确错误或缺省 CLI 帮助呈现，不做静默兼容。

## 4. 设计取舍

- 本轮不实现完整 scheduler，是为了先把用户接口、Task 定义模型和进程内执行语义做稳。
- SQLite schema 先落地核心表和约束，允许后续迁移扩展字段。
- `dag.test()` 使用隔离内存运行状态，不写生产状态库，符合设计稿本地测试语义。
- 外部副作用仍按 at-least-once 边界处理；本轮测试用幂等文件/内存示例验证用户侧写法。
