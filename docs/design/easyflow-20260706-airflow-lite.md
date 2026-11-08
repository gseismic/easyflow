# easyflow 轻量 Airflow 设计稿

> 历史设计：与 [2026-11-08 优化设计](easyflow-20261108-optimization.md) 和当前实现冲突的部分以新设计为准，尤其是 Web 界面、时间语义和回补容量。

> 状态：修订后定稿候选
>
> 日期：2026-07-06
>
> 目标版本：easyflow v0.1
>
> 兼容基线：Apache Airflow 3.2 公共 Task SDK
> 文档语言：中文

## 1. 文档关系

本设计替代以下旧方向，但不删除旧设计：

- [旧版 Dataset/Slice 设计](archive/20260702/easyflow-20260702-v1.md)
- [旧版工作流接口对比](archive/20260702/workflow-api-examples-20260702-comparison.md)
- 原始 draft 代码位于 `docs/ref/v1`

旧设计以 Dataset、Slice、缓存和数据版本为核心；本设计改为以 DAG、Task、DagRun 和
TaskInstance 为核心。旧设计保留用于追溯假设和后续设计数据资产扩展层，不再作为
easyflow v0.1 的实现依据。

## 2. 背景

原始 draft 已经具备轻量工作流引擎的基本结构：

- `DAG`
- `Operator`
- `PythonOperator` 和 `BashOperator`
- `>>`、`<<` 任务依赖
- `SequentialExecutor` 和 `LocalExecutor`
- 简单任务状态和 DagRun ID

这些概念与 Airflow 的核心模型接近。继续在这一模型上增加 Dataset、Slice、业务日历、
缓存命中和数据版本，会使 easyflow 逐渐成为数据资产框架，而不是轻量 Airflow。

本设计重新确定产品边界：

> easyflow 是一个单机、持久化、可调度的 Airflow 兼容子集。

它优先兼容 Airflow DAG 作者的心智模型和常用源码结构，不兼容 Airflow 的分布式部署、
管理平台和完整 provider 生态。

## 3. 设计目标

### 3.1 用户目标

1. 已了解 Airflow 的用户无需重新学习 DAG、Task、Operator、DagRun 等概念。
2. 属于 v0.1 兼容子集的 Airflow 3 DAG，只修改 import 前缀即可迁移到 easyflow。
3. 支持经典 Operator 和 TaskFlow 两种编写方式。
4. 支持手动运行、定时运行、失败重试、进程重启恢复和历史回填。
5. 单机安装和运维足够简单，默认只依赖本地 Python 环境和 SQLite。
6. 不支持的 Airflow 能力在 DAG 解析阶段明确报错，禁止静默忽略。

### 3.2 工程目标

1. 用户定义与运行状态严格分离。
2. 调度器、执行器和状态存储之间有明确内部契约。
3. 所有状态迁移可持久化、可审计、可恢复。
4. 时间语义从第一版开始稳定，后续增加 scheduler 和 backfill 时不推翻数据模型。
5. 公共 API 遵循语义化版本；内部接口允许迭代。
6. 运行时依赖可注入，便于使用 fake clock、内存状态库和同步执行器测试。

### 3.3 量化数据工作负载目标

easyflow v0.1 必须能支撑单机量化数据流水线稳定运行，典型场景包括：

1. 按交易日或分钟级时间窗口自动抓取行情、财报、公告、宏观和因子原始数据。
2. 对已抓取数据执行清洗、标准化、特征计算和落地。
3. 对历史区间执行 backfill，并能在失败后从已提交成功的位置恢复。
4. 用 `logical_date` 和 `data_interval` 明确表达业务数据区间，禁止任务直接用 wall clock 猜测应处理哪天数据。
5. 大型业务数据写入外部存储，easyflow 只保存编排状态、日志、XCom 小型控制数据和审计信息。
6. 外部副作用必须按业务分区实现幂等，例如用交易日、标的、数据源版本或数据区间作为唯一键。

这意味着 easyflow 的核心价值是“可靠触发、恢复、审计和重试”，不是替代行情库、对象存储、时序数据库或数据质量平台。

## 4. 非目标

easyflow v0.1 不提供：

- Web UI
- REST API 服务
- 用户、角色和权限系统
- 多 scheduler 高可用
- Celery、Kubernetes 等分布式 Executor
- Airflow provider/plugin 生态兼容
- Dynamic Task Mapping
- deferrable operator 和 triggerer
- Asset/Dataset 调度
- Dataset、Slice、缓存、数据版本和数据质量协议
- 跨 DAG 依赖
- SLA、Deadline Alert、邮件告警
- 完整 Airflow CLI 兼容

上述能力没有被永久禁止，但不得提前污染 v0.1 核心模型。

## 5. 外部兼容基线

本设计只以 Airflow 3 的公共接口为兼容基线：

- [Airflow 3 公共接口](https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html)
- [Airflow Task SDK API](https://airflow.apache.org/docs/task-sdk/stable/api.html)
- [Airflow DAG 核心概念](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
- [Airflow TaskFlow](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Airflow XCom](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)

不以 `airflow.models.*`、`airflow.decorators.*` 等 Airflow 2 旧路径作为正式基线。

兼容的含义是：

- 概念名称一致；
- 支持范围内的参数名称一致；
- 支持范围内的返回值和副作用一致；
- 任务依赖、重试、调度和状态语义一致；
- import 根包从 `airflow` 改为 `easyflow`。
- easyflow 明确标注为“严格兼容子集”，不是 Airflow 的替代安装包。

兼容不意味着：

- 可以把 easyflow 安装成名为 `airflow` 的包；
- 任意 Airflow DAG 都能运行；
- 只改 import 后仍带有未支持参数的 DAG 可以继续运行；
- Airflow 内部类、数据库表或 Executor 插件可以直接复用；
- easyflow 必须复制 Airflow 的全部参数。

## 6. 总体方案比较

### 6.1 方案 A：延续原始 draft 的经典 Operator 接口

示例：

```python
dag = DAG("example", executor=LocalExecutor())

a = BashOperator(task_id="a", bash_command="echo a", dag=dag)
b = PythonOperator(task_id="b", python_callable=work, dag=dag)

a >> b
dag.run()
```

优点：

- 与原始代码最接近；
- 实现量最小；
- DAG 依赖清楚。

缺点：

- 不支持 Airflow 3 推荐的 TaskFlow；
- `executor` 泄漏到 DAG 用户接口；
- `dag.run()` 与 Airflow 正式运行方式不一致；
- 难以自然表达 Task 返回值和自动数据依赖。

结论：不采用为正式公共接口，仅保留部分兼容别名。

### 6.2 方案 B：Airflow 3 Task SDK 兼容子集

示例：

```python
from easyflow.sdk import DAG, task

with DAG("example", schedule="@daily") as dag:
    @task
    def extract():
        return {"value": 1}

    @task
    def load(data):
        print(data)

    load(extract())
```

优点：

- 面向稳定的 Airflow 3 公共接口；
- 同时支持经典 Operator 和 TaskFlow；
- 用户迁移成本低；
- 可以明确维护兼容矩阵。

缺点：

- 需要实现 XComArg、Context、模板和更完整状态机；
- 相同名字会提高用户对行为一致性的要求；
- 必须严格拒绝不支持参数，不能做宽松模拟。

结论：采用。

### 6.3 方案 C：Dataset/Slice 数据编排接口

优点：

- 适合金融时序数据处理；
- 容易表达分区、窗口、缓存和数据血缘；
- 比 Airflow 更贴近部分数据业务。

缺点：

- 与“轻量 Airflow”的目标冲突；
- Dataset 同时承担依赖、存储和版本语义，运行时契约较重；
- 用户无法直接迁移 Airflow DAG；
- 第一版工作量和正确性风险过高。

结论：从核心移除。未来如有需要，以独立扩展包构建在 Task `inlets/outlets` 之上。

## 7. 产品形态比较

### 7.1 方案 A：仅提供进程内 DAG Runner

只支持 `dag.test()`，不提供状态数据库和常驻 scheduler。

优点是极轻；缺点是没有可靠恢复、定时调度和历史运行，因此不能称为 Airflow 类编排器。

### 7.2 方案 B：单机持久化编排器

提供：

- DAG 文件发现；
- SQLite 状态库；
- 单 scheduler；
- 本地 Executor；
- CLI；
- 手动触发、定时调度和 backfill。

它保留 Airflow 最有价值的运行语义，同时避免分布式平台复杂度。

### 7.3 方案 C：分布式 Airflow 替代品

需要消息队列、远程 Worker、HA scheduler、API Server、权限系统和分布式锁。

这会直接失去“轻量”目标，也会与成熟 Airflow 正面重复建设。

### 7.4 定稿

采用方案 B：单机持久化编排器。

第一实现里程碑可以先交付手动 trigger、`dag.test()` 和 backfill，但数据模型必须按
常驻 scheduler 设计，不允许先使用不可恢复的内存状态再整体重写。

### 7.5 量化数据工作负载方案比较

量化交易场景对“稳定干活”的要求高于普通脚本调度：任务经常访问外部 API、写入分区数据、
执行历史回补，并且失败重试可能重复产生外部副作用。因此需要单独确定边界。

#### 方案 A：把 Dataset/Slice 重新放回核心

优点：

- 可以直接表达标的、交易日、窗口、缓存和数据版本；
- 对金融数据语义最贴近；
- 更容易做数据血缘和质量检查。

缺点：

- 与“轻量 Airflow 兼容子集”目标冲突；
- 第一版同时设计编排器和数据资产层，正确性风险过高；
- 会让普通 Airflow DAG 作者必须理解金融数据概念；
- 外部存储、分区格式和质量协议会污染核心状态机。

#### 方案 B：核心只做可靠编排，量化数据以幂等 Task 表达

优点：

- 保持 DAG、Task、DagRun、TaskInstance 这组 Airflow 心智模型；
- `logical_date` 和 `data_interval` 已足够表达“本次处理哪个业务区间”；
- 数据写入方式由业务选择，可以是 SQLite、Parquet、DuckDB、ClickHouse、对象存储或自研库；
- v0.1 可以先把恢复、重试、日志和审计做扎实。

缺点：

- DAG 作者必须自己实现外部数据幂等写入；
- 不内置数据质量、血缘、版本和缓存命中；
- 同一团队需要制定数据落地约定。

#### 方案 C：提供独立量化扩展包

优点：

- 不污染 easyflow 核心；
- 可以在核心稳定后提供 `easyflow_quant`，封装交易日、数据分区、行情源适配和质量检查；
- 扩展包可以复用 DAG 的 `inlets/outlets` 或 Task params。

缺点：

- v0.1 不能直接提供完整量化数据资产体验；
- 需要等核心调度、恢复和 backfill 语义稳定后再做。

#### 定稿

采用方案 B，并为方案 C 预留但不实现扩展点。

v0.1 的量化数据最佳实践是：

- Task 输入使用 `data_interval_start`、`data_interval_end`、`logical_date` 和 `params`；
- 大数据结果写入外部存储，XCom 只返回 manifest、路径、checksum、row_count 等小型元数据；
- 写入先进入临时位置，校验成功后用原子 rename、事务 upsert 或唯一键提交；
- 重试和 backfill 必须以同一业务分区覆盖或跳过，禁止追加不可去重的半成品；
- API 抓取任务使用 `max_active_runs`、`max_active_tasks`、重试退避和外部限流保护数据源。

## 8. 用户接口与内部接口边界

### 8.1 稳定用户接口

正式稳定入口为：

```python
from easyflow.sdk import (
    DAG,
    BaseOperator,
    Context,
    DagRunState,
    TaskInstanceState,
    TriggerRule,
    XComArg,
    chain,
    dag,
    get_current_context,
    task,
)
```

标准 Operator：

```python
from easyflow.providers.standard.operators.bash import BashOperator
from easyflow.providers.standard.operators.empty import EmptyOperator
from easyflow.providers.standard.operators.python import PythonOperator
```

为降低原始 draft 的迁移成本，可以提供便利重导出：

```python
from easyflow import DAG, dag, task
from easyflow.operators import BashOperator, EmptyOperator, PythonOperator
```

便利入口与正式入口行为必须完全相同，不允许维护两套实现。

### 8.2 非稳定内部接口

以下对象属于 `_internal`，DAG 作者不应直接依赖：

- `DagBag`
- `DagDefinition`
- `Scheduler`
- `TaskRunner`
- `StateStore`
- `Executor`
- `TaskWorkload`
- `ExecutorEvent`
- `XComBackend`
- `LogStore`
- 数据库模型
- lease 和 heartbeat 对象

内部对象不得因为 Airflow 存在同名内部类，就承诺结构兼容。

### 8.3 兼容失败原则

对于 Airflow 已知但 easyflow 暂不支持的参数：

```text
UnsupportedFeatureError:
参数 "pool" 属于 Airflow 公共接口，但 easyflow v0.1 尚不支持。
处理方式：移除该参数，或改用全局 max_parallelism。
```

对于未知参数，遵循 Python 正常 `TypeError`。

严禁：

- 接收后忽略；
- 只记录 warning 后继续；
- 将多个不同语义的 Airflow 参数映射到同一个布尔开关。

## 9. DAG 公共接口

### 9.1 构造函数

v0.1 支持以下参数：

```python
class DAG:
    def __init__(
        self,
        dag_id: str,
        *,
        description: str | None = None,
        schedule: str | timedelta | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        catchup: bool | None = None,
        default_args: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        tags: Collection[str] | None = None,
        max_active_tasks: int | None = None,
        max_active_runs: int | None = None,
        **kwargs: Any,
    ) -> None: ...
```

约束：

- `dag_id` 在一个 easyflow 实例内唯一；
- `dag_id` 只允许字母、数字、点、下划线和连字符；
- `start_date`、`end_date` 必须带时区；
- `start_date < end_date`；
- `schedule=None` 表示只允许手动触发；
- `catchup=None` 表示读取全局配置；
- `max_active_tasks=None` 表示读取全局配置，用于限制单个 DagRun 内并发 TaskInstance 数；
- `max_active_runs=None` 表示读取全局配置；
- `default_args` 在构造时复制，禁止后续修改影响既有 Task。
- `kwargs` 只用于识别 Airflow 已知但暂不支持的参数；这些参数抛出
  `UnsupportedFeatureError`，其他未知参数抛出 `TypeError`，任何值都不会被保存或忽略。

### 9.2 三种 DAG 声明方式

显式传入：

```python
dag = DAG("example", schedule="@daily")
task_a = EmptyOperator(task_id="a", dag=dag)
```

上下文：

```python
with DAG("example", schedule="@daily") as dag:
    task_a = EmptyOperator(task_id="a")
```

装饰器：

```python
@dag(dag_id="example", schedule="@daily")
def build():
    EmptyOperator(task_id="a")

dag = build()
```

三种方式生成同一种 `DAG` 对象和同一种内部 `DagDefinition`。

### 9.3 DAG 上下文规则

- Operator 显式传入 `dag=` 时使用该 DAG；
- 未传 `dag=` 时绑定当前最内层 DAG context；
- 两者同时存在但不是同一 DAG 时立即报错；
- 不在 DAG context 且未传 `dag=` 时，Task 可以暂时未绑定；
- 未绑定 Task 与已绑定 Task 建立依赖时，继承对方 DAG；
- 两个不同 DAG 的 Task 建立依赖时立即报错；
- DAG 解析结束后仍未绑定的 Task 不能执行。

### 9.4 DAG 验证

解析完成后至少检查：

- `dag_id` 合法且无重复；
- `task_id` 在 DAG 中唯一；
- DAG 无环；
- 所有 Task 已绑定；
- 所有 XComArg 引用有效；
- 所有 trigger rule 在支持范围内；
- schedule 和时间边界有效；
- 不存在不支持参数。

验证失败时，该 DAG 不进入 scheduler，但不阻止其他 DAG 被加载。

### 9.5 本地测试

不提供正式的 `dag.run()`。

提供与 Airflow 心智一致的：

```python
result = dag.test(
    logical_date=None,
    run_conf=None,
    use_executor=False,
)
```

语义：

- 创建隔离的测试 DagRun；
- 默认在当前进程顺序执行；
- 日志输出到终端；
- 返回结构化 `DagTestResult`；
- 默认不写入生产状态库；
- `use_executor=True` 时使用配置的本地 Executor 和临时状态库。

## 10. BaseOperator 公共接口

### 10.1 支持参数

```python
class BaseOperator:
    def __init__(
        self,
        *,
        task_id: str,
        dag: DAG | None = None,
        owner: str = "airflow",
        retries: int | None = None,
        retry_delay: timedelta | None = None,
        retry_exponential_backoff: bool | float = False,
        max_retry_delay: timedelta | None = None,
        trigger_rule: TriggerRule | str = TriggerRule.ALL_SUCCESS,
        depends_on_past: bool = False,
        params: dict[str, Any] | None = None,
        do_xcom_push: bool = True,
        multiple_outputs: bool = False,
        **kwargs: Any,
    ) -> None: ...

    def execute(self, context: Context) -> Any:
        raise NotImplementedError
```

`retries=None`、`retry_delay=None` 表示依次继承 DAG `default_args` 和全局默认值。

`kwargs` 与 DAG 构造函数采用相同的严格兼容失败规则，不是任意扩展参数容器。

### 10.2 任务依赖

支持：

```python
a >> b
a << b
a.set_downstream(b)
a.set_upstream(b)
chain(a, b, c)

a >> [b, c]
[a, b] >> c
```

`>>` 和 `<<` 运算符必须返回可继续连接的对象，因此以下写法有效：

```python
a >> b >> c
```

禁止把 `>>` 和 `<<` 写成无返回值的纯副作用。

`chain(a, b, c)` 是兼容便利函数，可以返回 `None`；不得依赖 `chain()` 返回值继续拼接。

### 10.3 Operator 生命周期

用户 Operator 只实现 `execute(context)`。

执行顺序：

```text
构造 Operator
→ 解析并冻结 TaskDefinition
→ 调度 TaskInstance
→ 渲染 template_fields
→ execute(context)
→ 序列化返回值到 XCom
→ 提交终态
```

用户 Operator 对象是定义，不保存某次运行的状态。

### 10.4 标准 Operator

#### EmptyOperator

无副作用，返回 `None`，用于表达控制依赖。

#### PythonOperator

```python
PythonOperator(
    task_id="work",
    python_callable=work,
    op_args=[1],
    op_kwargs={"name": "demo"},
)
```

执行 `python_callable(*op_args, **op_kwargs)`。

#### BashOperator

```python
BashOperator(
    task_id="shell",
    bash_command="echo {{ ds }}",
    env={"MODE": "prod"},
    append_env=True,
    cwd=None,
)
```

约束：

- 使用独立 subprocess；
- 默认捕获 stdout/stderr 到任务日志；
- 非零退出码使任务失败；
- shell 命令属于显式外部副作用；
- DAG 作者负责命令幂等性；
- 不自动重试不可幂等命令，除非用户显式设置 `retries`。

## 11. TaskFlow 公共接口

### 11.1 基本行为

```python
@task
def extract() -> dict[str, int]:
    return {"value": 1}

@task(retries=2)
def transform(data: dict[str, int]) -> int:
    return data["value"] + 1

result = transform(extract())
```

DAG 解析期间：

- 装饰后的函数不执行；
- 每次调用生成一个 Task；
- 调用返回 `XComArg`；
- 输入参数中的 XComArg 自动建立上游依赖；
- 普通 Python 值作为 Task 参数冻结在定义中。

任务运行期间：

- XComArg 被解析为对应上游 TaskInstance 的 XCom 值；
- 再调用原 Python 函数；
- 返回值写入当前 TaskInstance 的 XCom。

### 11.2 task_id

- 默认使用函数名；
- `@task(task_id="name")` 显式指定；
- 同一装饰函数重复调用时，按 Airflow 心智生成 `name`、`name__1`、`name__2`；
- 可以使用 `.override(task_id="other", retries=3)` 覆盖 Operator 参数；
- 自动后缀只处理 TaskFlow 重复调用，不掩盖显式 Operator 的重复 ID。

### 11.3 多输出

```python
@task(multiple_outputs=True)
def extract():
    return {"rows": 10, "path": "/tmp/data"}

result = extract()
consume(result["rows"])
```

要求返回值为 `Mapping[str, Any]`。不是 Mapping 时任务失败，错误码为
`XCOM_MULTIPLE_OUTPUTS_TYPE_ERROR`。

### 11.4 Context

推荐在需要时显式获取：

```python
@task
def work():
    context = get_current_context()
    print(context["run_id"])
```

`get_current_context()` 只能在 Task 执行线程或进程内调用。在 DAG 解析期调用时抛出
`ContextNotAvailableError`。

## 12. XCom 设计

### 12.1 定位

XCom 只用于同一 DagRun 内 Task 之间的小型控制数据，不用于传递 DataFrame、大文件或
数据库结果集。

### 12.2 标识

```text
dag_id
+ run_id
+ task_id
+ map_index
+ key
```

默认返回值使用 key `return_value`。

### 12.3 默认序列化

默认使用安全的 JSON XCom backend，支持：

- `None`
- `bool`
- `int`、`float`、`str`
- JSON list
- key 为字符串的 JSON dict

默认单值上限为 1 MiB，可通过配置降低。超过限制时任务失败，错误码为
`XCOM_VALUE_TOO_LARGE`。

### 12.4 Python 对象兼容

为兼容可信单机场景，可选提供 Pickle backend，但必须显式开启：

```toml
[xcom]
backend = "pickle"
allow_unsafe_deserialization = true
```

原因：反序列化不可信 pickle 可以执行任意代码，不能隐藏在默认行为中。

### 12.5 提交原子性

任务成功时，XCom 写入与 TaskInstance `success` 状态必须在同一状态库事务中提交。

如果 XCom 序列化或持久化失败：

- TaskInstance 不得进入 `success`；
- 按普通任务异常进入 retry 或 failed；
- 不得留下可被下游读取的半成品 XCom。

## 13. 时间模型

### 13.1 核心字段

每个 DagRun 从第一版开始保存：

- `run_id`
- `run_type`
- `logical_date`
- `data_interval_start`
- `data_interval_end`
- `queued_at`
- `start_date`
- `end_date`

时间统一保存为 UTC，用户接口返回带时区的 datetime。

### 13.2 数据区间

区间采用左闭右开：

```text
[data_interval_start, data_interval_end)
```

日调度示例：

```text
logical_date = 2026-07-05 00:00:00+08:00
data_interval = [2026-07-05 00:00:00, 2026-07-06 00:00:00)
实际创建时间不早于 2026-07-06 00:00:00
```

不得把 wall clock 当前时间直接当成业务数据日期。

### 13.3 schedule 支持范围

v0.1 支持：

- `None`
- cron 字符串
- `timedelta`
- `@once`
- `@hourly`
- `@daily`
- `@weekly`
- `@monthly`
- `@yearly`

暂不支持自定义 Timetable 和 Asset schedule。

### 13.4 catchup

- `catchup=True`：scheduler 为所有尚未创建且已结束的数据区间创建 DagRun；
- `catchup=False`：启用 DAG 时只创建最新一个符合条件的数据区间；
- scheduled DagRun 已存在相同 `(dag_id, logical_date)` 时不得重复创建；
- `max_active_runs` 同时约束 scheduler 和 backfill。

DagRun 持久化唯一性：

- `(dag_id, run_id)` 全局唯一；
- scheduled run 额外唯一约束 `(dag_id, logical_date, run_type="scheduled")`；
- backfill run 额外唯一约束 `(dag_id, backfill_id, logical_date)`；
- manual run 只要求 `run_id` 唯一，允许用户为同一 `logical_date` 创建多次手动运行。

### 13.5 手动运行

手动 trigger 必须创建新的唯一 `run_id`，默认格式：

```text
manual__2026-07-06T10:20:30.123456+00:00
```

用户可以传：

- `logical_date`
- `conf`
- 自定义 `run_id`

重复 `run_id` 立即报错。无 schedule 的 DAG 默认使用
`data_interval_start == data_interval_end == logical_date`。

### 13.6 backfill

CLI：

```bash
easyflow backfill create \
  --dag-id daily_etl \
  --start-date 2026-07-01 \
  --end-date 2026-07-05 \
  --reprocessing-behavior failed \
  --max-active-runs 1
```

语义：

- `start-date` 和 `end-date` 选择 logical date；
- 先生成 dry-run 计划，再创建 DagRun；
- `reprocessing-behavior` 支持 `none`、`failed`、`completed`；
- `none`：只为完全不存在历史 DagRun 的 logical date 创建 backfill run；
- `failed`：只为最近一次相关 DagRun 为 `failed` 的 logical date 创建新的 backfill run；
- `completed`：为最近一次相关 DagRun 已终态的 logical date 创建新的 backfill run；
- 新 backfill run 使用新的 `run_id` 和同一个 `backfill_id` 下的唯一 logical date，不覆盖历史记录；
- DAG 内 `depends_on_past=True` 时，默认按 logical date 正序执行；
- 每个 backfill 创建独立 `backfill_id`，便于审计。

## 14. 运行状态模型

### 14.1 DagRunState

```text
queued
running
success
failed
```

合法迁移：

| 当前状态 | 下一状态 | 条件 |
|---|---|---|
| queued | running | scheduler 开始调度该 run |
| queued | failed | DAG 定义已不可执行 |
| running | success | 所有叶子 Task 满足成功条件 |
| running | failed | 至少一个叶子 Task 失败或 upstream_failed |
| success / failed | queued | 用户执行已确认且已审计的 tasks clear |

正常调度不得把 DagRun 终态原地改回 running。唯一例外是显式 `tasks clear`：先把 DagRun
恢复为 queued，再由 scheduler 进入 running，并记录操作者、选择范围和原因。批量重处理
优先创建新 DagRun，不复用历史 run。

### 14.2 TaskInstanceState

`TaskInstanceState` 枚举包含：

```text
scheduled
queued
running
up_for_retry
success
failed
upstream_failed
skipped
```

初始 TaskInstance 的 `state` 为数据库 `NULL`，对应 Python `None`，不额外发明
`pending` 状态。它表示任务存在，但依赖条件尚未完成判定。

合法主路径：

```text
None → scheduled → queued → running → success
                                  ├──→ up_for_retry → scheduled
                                  └──→ failed

None → upstream_failed
None → skipped

queued → scheduled
```

`queued → scheduled` 只用于 Executor 尚未启动任务且 queue lease 已过期的系统恢复。首次
claim 会分配下一个 `try_number`；重新 claim 复用尚未 started 的同一 TaskTry，不再增加
`try_number`。任务已经进入 running 后进程丢失，则按本次 try 的失败处理。

状态迁移必须使用带前置状态的条件更新：

```sql
UPDATE task_instance
SET state = :next
WHERE key = :key AND state = :expected
```

更新行数不是 1 时抛出 `StateConflictError`，禁止无条件覆盖。

### 14.3 TaskInstance 与 TaskTry 标识

一个逻辑 TaskInstance 的稳定标识为：

```text
(dag_id, run_id, task_id, map_index)
```

一次具体执行尝试的标识为：

```text
(dag_id, run_id, task_id, map_index, try_number)
```

TaskInstance 保存当前状态和当前 `try_number`；TaskTry 保存每次尝试的日志、错误、开始时间、
结束时间和 Executor 信息。重试只增加 TaskTry，不创建第二个逻辑 TaskInstance。

v0.1 不支持 Dynamic Task Mapping，但固定保存 `map_index=-1`，防止未来修改主键。

`try_number` 在 TaskInstance 首次从 scheduled claim 为 queued 的原子事务中分配。TaskTry
从 queued 进入 running 时记录真正的 `started_at`。日志、错误和持续时间都按 TaskTry 保存。

### 14.4 DagRun 终态判定

只有全部 TaskInstance 都进入终态后，才计算 DagRun 终态。最终结果使用叶子 Task 判断：

- 所有叶子为 `success` 或 `skipped`：DagRun `success`；
- 任一叶子为 `failed` 或 `upstream_failed`：DagRun `failed`；
- 任一 TaskInstance 仍为非终态：DagRun 保持 `running`，即使叶子 Task 已提前完成。

## 15. 依赖与 TriggerRule

v0.1 支持：

```python
TriggerRule.ALL_SUCCESS
TriggerRule.ALL_DONE
TriggerRule.ONE_SUCCESS
TriggerRule.ONE_FAILED
TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
TriggerRule.ALWAYS
```

规则只检查直接上游，不扫描全部祖先。

默认 `ALL_SUCCESS`。

Scheduler 在直接上游状态变化时重新计算规则：

| 规则 | 进入 scheduled 的条件 | 必须等待全部上游终态 |
|---|---|---:|
| `ALL_SUCCESS` | 全部上游 success | 是 |
| `ALL_DONE` | 全部上游进入任意终态 | 是 |
| `ONE_SUCCESS` | 任一上游 success | 否 |
| `ONE_FAILED` | 任一上游 failed 或 upstream_failed | 否 |
| `NONE_FAILED_MIN_ONE_SUCCESS` | 无失败且至少一个 success | 是 |
| `ALWAYS` | DagRun 已进入 running | 否 |

在规则尚未满足时：

- 规则满足：当前 TaskInstance 从 `None` 进入 `scheduled`；
- 规则未来仍可能满足：保持 `None`；
- 规则不满足且全部相关上游已终态：进入 `upstream_failed` 或 `skipped`；
- 上游存在 `failed` 或 `upstream_failed` 时使用 `upstream_failed`，否则使用 `skipped`；
- 不得在所有上游终态后永远停留在 `None`。

Branch TaskFlow、`LatestOnlyOperator` 放入后续版本，但 v0.1 状态机保留 `skipped`，
避免未来重构 trigger rule。

### 15.2 depends_on_past

`depends_on_past=True` 是跨 DagRun 的额外依赖，主要用于量化数据按时间顺序加工的场景。

方案比较：

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| A：所有 run_type 都互相作为过去依赖 | manual、scheduled、backfill 全部按 logical date 排序 | 行为表面统一 | 手动修复会意外阻塞生产调度 |
| B：只在 scheduled/backfill 之间生效 | 生产调度和历史回补共享顺序，manual 默认隔离 | 更适合稳定生产和临时排障 | 与部分 Airflow 边角行为不完全一致 |
| C：完全不支持 | 实现最简单 | 无法保护时序数据顺序 | 不满足量化数据处理需求 |

定稿采用方案 B。

规则：

- 仅对 `run_type in {"scheduled", "backfill"}` 的 DagRun 生效；
- manual DagRun 默认不参与 `depends_on_past`，也不阻塞 scheduled/backfill；
- 上一个实例定义为同一 `dag_id`、同一 `task_id`、同一 `map_index`、logical date 小于当前值的最近 scheduled/backfill DagRun；
- 如果不存在上一个实例，当前 TaskInstance 允许继续调度；
- 上一个实例为 `success` 或 `skipped` 时，依赖满足；
- 上一个实例为 `failed` 或 `upstream_failed` 时，当前 TaskInstance 进入 `upstream_failed`；
- 上一个实例仍为非终态时，当前 TaskInstance 保持 `None`，并记录等待原因 `WAITING_DEPENDS_ON_PAST`；
- backfill 默认按 logical date 正序创建和调度，除非未来显式设计 `--run-backwards`，否则不提供逆序执行。

设计理由：

- 量化数据常见模式是“先补 T 日原始数据，再算 T 日衍生数据，再进入 T+1”，失败后继续跑未来日期通常比停住更危险；
- manual run 常用于验证和临时修复，不应默认改变生产时间线；
- 如果用户需要重处理历史正式链路，应使用 backfill，而不是 manual run。

## 16. 重试语义

### 16.1 重试次数

`retries=0` 表示只执行一次。

最大执行次数：

```text
1 + retries
```

### 16.2 重试时间

普通重试：

```text
next_retry_at = failed_at + retry_delay
```

指数退避：

```text
factor = 2.0              当 retry_exponential_backoff is True
factor = float(value)     当 retry_exponential_backoff > 1.0
delay = min(retry_delay * factor^(try_number - 1), max_retry_delay)
```

`retry_exponential_backoff=False` 或 `0` 表示不启用指数退避。`retry_exponential_backoff=1`
等价于普通固定间隔，不建议使用。

实现可以加入稳定随机抖动，但同一 TaskInstance 重启计算必须得到同一结果。抖动种子必须来自
稳定标识，例如 `(dag_id, run_id, task_id, map_index, try_number)`。

### 16.3 失败类型

| 类型 | 示例 | 默认处理 |
|---|---|---|
| 用户任务错误 | Python 异常、Bash 非零退出 | retry 或 failed |
| 模板错误 | 未定义变量、渲染类型错误 | retry 或 failed |
| XCom 错误 | 不可序列化、超限 | retry 或 failed |
| 调度定义错误 | 环、重复 task_id | DAG 不可调度 |
| 系统瞬时错误 | SQLite busy、Executor 暂不可用 | scheduler 自身重试，不消耗 Task try |
| 运行进程丢失 | Worker 退出、lease 超时 | retry 或 failed |

禁止 scheduler 把自己的瞬时故障计入业务任务重试次数。

## 17. Executor 与并发

### 17.1 用户边界

Executor 是部署配置，不是 DAG 业务定义。

不支持：

```python
DAG("example", executor=LocalExecutor())
```

配置方式：

```toml
[core]
executor = "local"
parallelism = 4
max_active_tasks_per_dagrun = 16
```

环境变量覆盖：

```bash
EASYFLOW__CORE__EXECUTOR=local
EASYFLOW__CORE__PARALLELISM=4
EASYFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAGRUN=16
```

并发限制优先级：

- `parallelism` 限制整个 easyflow 实例同时 running/queued 的 TaskInstance 数；
- `DAG(max_active_tasks=...)` 限制单个 DagRun 内同时 running/queued 的 TaskInstance 数；
- `DAG(max_active_runs=...)` 限制同一 DAG 同时 active 的 DagRun 数；
- scheduler 在 claim TaskInstance 前必须同时检查三类限制。

### 17.2 SequentialExecutor

- 同一时刻只运行一个 TaskInstance；
- 用于测试、调试和最小环境；
- Task 可以直接在受控当前进程运行；
- scheduler 仍通过统一 Executor 协议提交任务。

### 17.3 LocalExecutor

- 使用独立本地进程运行 TaskInstance；
- 最大并发受全局 `parallelism` 限制；
- scheduler 额外执行每个 DagRun 的 `max_active_tasks` 限制；
- Worker 不直接决定依赖和重试；
- Worker 报告 started、heartbeat 和进程退出；
- TaskRunner 通过受限 StateStore 接口提交本次尝试的 success 或 failure；
- scheduler 是依赖判定、任务 claim、恢复和下一次调度的唯一协调者。

### 17.4 内部 Executor 协议

```python
class Executor(Protocol):
    def start(self) -> None: ...

    def submit(self, workload: TaskWorkload) -> None: ...

    def poll(self) -> Sequence[ExecutorEvent]: ...

    def terminate(self, key: TaskTryKey, reason: str) -> None: ...

    def shutdown(self, *, wait: bool) -> None: ...
```

契约：

- `submit()` 对同一 workload key 必须幂等；
- Executor 不直接修改状态数据库；
- `poll()` 可以重复返回事件，scheduler 必须去重；
- `shutdown(wait=False)` 不承诺任务成功完成；
- LocalExecutor 只保证单机进程安全，不承诺分布式安全。

## 18. 持久化与恢复

### 18.1 默认状态库

默认使用 SQLite，至少保存：

- DAG 解析结果和 definition hash；
- DagRun；
- TaskInstance；
- Task try；
- XCom；
- scheduler lease；
- task heartbeat；
- backfill；
- CLI 审计操作。

业务数据不进入 easyflow 状态库。

### 18.2 执行交付语义

任意 Python/Bash 任务都可能访问外部 API、写文件、写数据库或触发交易相关副作用。
easyflow 不能对任意外部系统承诺 exactly-once；v0.1 的正式承诺是：

```text
已提交成功的 TaskInstance 不会被 scheduler 再次自动提交执行，除非用户执行审计型 clear；
未提交成功的 TaskInstance 可能被重试；
用户任务的外部副作用必须按 at-least-once 执行模型设计为幂等。
```

方案比较：

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| A：只依赖 ExecutorEvent，由 scheduler 统一提交结果 | Worker 结束后事件回到 scheduler，再写状态库 | scheduler 绝对中心化 | scheduler 崩溃会丢失成功事件，已完成任务可能重跑 |
| B：TaskRunner 通过 StateStore 直接提交本次尝试终态 | 任务进程在执行结束后用 CAS 事务写 XCom、TaskTry 和 TaskInstance 终态 | 单机 SQLite 下最稳，成功提交后重启不重跑 | TaskRunner 需要状态库写权限，必须严格限制可写接口 |
| C：Worker 写 durable outbox，scheduler 消费 outbox | 结果事件先持久化，再由 scheduler 应用 | 保持 scheduler 统一提交，适合未来远程 Worker | v0.1 复杂度过高，需要额外事件表和清理协议 |

定稿采用方案 B。

契约：

- Executor 仍不直接修改状态库；
- TaskRunner 是受控运行时组件，可以调用受限的 `StateStore.commit_success()` 和
  `StateStore.commit_failure()`；
- 用户代码不能直接访问 easyflow 状态库；
- `commit_success()` 必须在同一事务内写入 XCom、TaskTry 结束信息、渲染模板元数据和
  TaskInstance `success`；
- `commit_failure()` 必须在同一事务内写入错误信息、TaskTry 结束信息，并把 TaskInstance
  转为 `up_for_retry` 或 `failed`；
- 两个 commit 方法都必须带前置状态和 `try_number` 条件更新，重复提交同一结果必须幂等；
- 如果进程在外部副作用完成后、commit 成功前崩溃，scheduler 只能把该 TaskInstance 视为
  未成功提交，并按 retry 策略处理；
- 因此量化数据写入必须使用业务唯一键、临时文件加原子 rename、事务 upsert 或可重复覆盖的
  分区提交。

这个选择修复了“任务已经提交 success 但 scheduler 崩溃后又重跑”的问题，同时诚实暴露外部
副作用的 at-least-once 边界。

### 18.3 事务边界

以下操作必须原子：

- 创建 DagRun 及其初始 `state=NULL` TaskInstance；
- claim scheduled TaskInstance、分配或复用 TaskTry，并转为 queued；
- 提交 XCom 并把 TaskInstance 转为 success；
- 记录失败并决定 up_for_retry 或 failed；
- clear TaskInstance 及清理其当前 run 的 XCom。

### 18.4 Scheduler lease

v0.1 同一状态库只允许一个 active scheduler。

启动时：

1. 尝试原子获得 scheduler lease；
2. 按固定周期续租；
3. lease 已被有效持有时启动失败；
4. lease 丢失后立即停止创建和 claim 新任务。

这不是多 scheduler HA，只是防止误启动两个 scheduler。

### 18.5 Task lease 与 heartbeat

queued/running TaskInstance 保存：

- `executor_id`
- `worker_pid`
- `lease_expires_at`
- `last_heartbeat_at`

超时后：

1. scheduler 向 Executor 查询进程；
2. 确认进程不存在或无法恢复；
3. 记录 `TASK_PROCESS_LOST`；
4. 按剩余 retries 转为 up_for_retry 或 failed。

不得因为一次短暂 scheduler 卡顿立即判定业务任务失败。

### 18.6 DAG 代码变化

每次成功解析生成 `definition_hash`，DagRun 记录创建时的 hash。

v0.1 不保存可执行源代码快照，因此恢复历史 DagRun 有三种方案：

| 方案 | 说明 | 优点 | 缺点 |
|---|---|---|---|
| A：task_id 仍存在就继续 | hash 不一致只记录 warning | 可用性高 | 历史 run 可能混用新旧代码，审计风险高 |
| B：hash 不一致默认停止 | 用户显式确认后才用当前定义继续 | 审计安全，避免悄悄换代码 | 修复后需要明确操作 |
| C：保存完整 DAG bundle/source | 每个 run 使用创建时的代码版本恢复 | 最接近生产级版本化 | v0.1 实现量较大，需要 bundle 管理 |

定稿采用方案 B，方案 C 作为后续演进。

恢复历史 DagRun 时：

- 当前 `definition_hash` 与 DagRun 记录一致：允许继续；
- hash 不一致：默认停止该 DagRun，错误码 `DAG_DEFINITION_CHANGED`；
- 用户可通过未来单独设计的审计型命令显式选择“用当前定义继续”，该操作必须记录操作者、
  原 hash、新 hash 和原因；
- 当前 DAG 删除或改变关键 Task：停止该 DagRun，错误码 `DAG_DEFINITION_INCOMPATIBLE`；
- 不猜测旧 Task 应映射到哪个新 Task。

设计理由：量化数据回补需要能解释“某天某批数据由哪版代码生成”。在没有 source snapshot
之前，默认停止比默认混用新代码更安全。

## 19. 模板与运行 Context

### 19.1 模板字段

`BaseOperator.template_fields` 声明需要渲染的属性：

```python
class BashOperator(BaseOperator):
    template_fields = ("bash_command", "env", "cwd")
```

支持递归渲染：

- 字符串；
- list、tuple；
- key 为字符串的 dict；
- 嵌套的上述结构。

不自动遍历任意用户对象属性。

### 19.2 Context 字段

v0.1 提供：

```text
dag
task
ti / task_instance
dag_run
run_id
logical_date
data_interval_start
data_interval_end
ds
ts
params
task_instance_key_str
```

`params` 合并顺序：

```text
全局默认 < DAG params < Task params < 手动 trigger conf 中允许覆盖的 params
```

是否允许 trigger conf 覆盖 params 由全局配置控制，默认允许。

### 19.3 模板错误

使用严格未定义变量策略。模板引用不存在字段时任务失败，不允许渲染为空字符串。

模板渲染后的值记录在 Task try 元数据中，但包含敏感信息的字段必须支持 Operator 指定
redact 规则。

## 20. 日志

每次 Task try 独立记录日志：

```text
{log_root}/{dag_id}/{run_id}/{task_id}/{map_index}/{try_number}.log
```

要求：

- stdout、stderr 和 Python logging 汇总；
- 每行包含 UTC 时间、级别、dag_id、run_id、task_id、try_number；
- 日志写入失败不得被伪装成任务成功；
- CLI 能读取正在运行和已完成任务日志；
- 默认只使用本地文件，不实现远程日志。

## 21. CLI 用户接口

v0.1 提供以下稳定意图，不承诺逐参数复制 Airflow CLI：

```bash
easyflow dags list
easyflow dags show DAG_ID
easyflow dags test DAG_ID
easyflow dags trigger DAG_ID
easyflow dags state DAG_ID RUN_ID

easyflow tasks list DAG_ID
easyflow tasks test DAG_ID TASK_ID
easyflow tasks state DAG_ID RUN_ID TASK_ID
easyflow tasks logs DAG_ID RUN_ID TASK_ID
easyflow tasks clear DAG_ID --run-id RUN_ID

easyflow backfill create --dag-id DAG_ID --start-date DATE --end-date DATE

easyflow scheduler
easyflow db init
```

### 21.1 危险操作

`tasks clear` 会删除选定 TaskInstance 的当前结果和 XCom，并可能导致重新执行外部副作用。

因此必须：

1. 默认输出影响预览；
2. 无 `--yes` 时要求交互确认；
3. 非交互环境缺少 `--yes` 时拒绝执行；
4. 写入审计记录；
5. 支持 `--dry-run`。

补充约束：

- 默认只允许 clear `success`、`failed`、`upstream_failed`、`skipped` 和 `up_for_retry`；
- queued/running Task 必须先终止并确认 Worker 退出，v0.1 不提供强制 clear；
- clear 把选中的 TaskInstance 置为 `NULL`，删除其当前可读 XCom，但保留历史 TaskTry 和日志；
- clear 已终态 DagRun 时，以审计型迁移把 DagRun 恢复为 queued；
- clear 不删除 DagRun，不覆盖历史 TaskTry。

v0.1 不提供 `db reset`。数据库重置只能通过明确的开发工具子命令另行设计。

## 22. 内部架构

```text
DAG Python 文件
      │
      ▼
DagParser ──→ DagBag ──→ DagDefinition
                              │
                              ▼
                         Scheduler
                         │   │   │
                         │   │   └── LogStore
                         │   └────── XComBackend
                         └────────── StateStore(SQLite)
                              │
                              ▼
                           Executor
                              │
                              ▼
                          TaskRunner
```

### 22.1 DagParser

职责：

- 在隔离子进程导入 DAG 文件；
- 收集模块顶层 DAG；
- 捕获 import、语法和验证错误；
- 设置解析超时；
- 生成不可变 DagDefinition；
- 不运行 Task。

### 22.2 Scheduler

职责：

- 根据 schedule 创建 DagRun；
- 计算直接上游依赖；
- 应用 trigger rule；
- claim 可运行 TaskInstance；
- 处理 ExecutorEvent；
- 计算 retry；
- 维护 DagRun 终态；
- 恢复 orphaned Task。

Scheduler 不执行用户任务代码。

### 22.3 StateStore

内部协议：

```python
class StateStore(Protocol):
    def create_dag_run(self, request: CreateDagRun) -> DagRunRecord: ...
    def list_runnable_tasks(self, now: datetime, limit: int) -> list[TaskInstanceRecord]: ...
    def claim_task(
        self,
        key: TaskInstanceRef,
        owner: str,
        lease_until: datetime,
    ) -> TaskWorkload | None: ...
    def commit_success(self, result: TaskSuccess) -> None: ...
    def commit_failure(self, result: TaskFailure) -> TaskInstanceState: ...
    def renew_task_lease(self, heartbeat: TaskHeartbeat) -> bool: ...
```

所有 datetime 参数必须带时区。所有写操作明确事务边界和期望前置状态。

### 22.4 TaskRunner

职责：

- 重载 DAG 文件并定位 Task；
- 建立只读运行 Context；
- 读取和解析输入 XCom；
- 渲染模板；
- 执行 Operator；
- 捕获结构化异常；
- 通过 StateStore 原子提交 TaskResult。

TaskRunner 不自行重试，不计算依赖，不 claim 任务；它只把当前 `try_number` 的执行结果转换为
受限状态库提交。是否再次进入 `scheduled` 由 StateStore 按已冻结的 retry 策略写入状态，
scheduler 后续再调度。

### 22.5 Clock

Scheduler、重试和 lease 不直接调用散落的 `datetime.now()`：

```python
class Clock(Protocol):
    def now(self) -> datetime: ...
```

生产使用系统 UTC clock；测试使用 FakeClock。

## 23. 配置

配置优先级：

```text
内置默认值
< easyflow.toml
< EASYFLOW__SECTION__KEY 环境变量
< CLI 显式参数
```

首版核心配置：

```toml
[core]
dags_folder = "./dags"
executor = "sequential"
parallelism = 1
default_timezone = "Asia/Shanghai"
max_active_tasks_per_dagrun = 16
max_active_runs_per_dag = 1
dag_file_processor_timeout_seconds = 30

[database]
url = "sqlite:///./.easyflow/easyflow.db"
busy_timeout_seconds = 30

[scheduler]
heartbeat_seconds = 5
lease_seconds = 30
catchup_by_default = false

[xcom]
backend = "json"
max_value_bytes = 1048576

[logging]
base_log_folder = "./.easyflow/logs"
```

DAG 作者不应读取内部配置对象来表达业务参数；业务参数使用 DAG `params` 或普通代码配置。

### 23.1 本地密钥与外部 API

v0.1 不实现 Airflow `Connection` / `Variable` 存储，也不提供 Web UI 密钥管理。
量化数据抓取需要的 API key、token 和数据库凭据按以下方式处理：

- 生产任务优先从环境变量或业务自己的密钥系统读取；
- easyflow 配置文件不得鼓励明文保存交易账号、行情源 token 或数据库密码；
- 日志和渲染模板元数据必须支持按 key 名称脱敏，默认脱敏包含 `password`、`secret`、`token`、
  `key`、`credential` 的字段；
- `params` 适合传业务参数，不适合传密钥；
- 后续如增加 `Connection` / `Variable`，必须单独设计加密、权限、导出和审计规则。

## 24. 错误模型

公共异常：

```python
EasyflowError
├── DagDefinitionError
│   ├── DuplicateDagIdError
│   ├── DuplicateTaskIdError
│   ├── DagCycleError
│   └── TaskBindingError
├── UnsupportedFeatureError
├── ContextNotAvailableError
├── TemplateRenderError
├── XComError
│   ├── XComSerializationError
│   └── XComValueTooLargeError
└── StateConflictError
```

持久化错误必须同时保存：

- 稳定 error code；
- 用户可读 message；
- 异常类型；
- traceback；
- try_number；
- 发生时间。

CLI 根据 error code 提供修复建议，不依赖字符串匹配。

## 25. 兼容矩阵

| Airflow 能力 | v0.1 | 说明 |
|---|---:|---|
| `DAG` 构造 | 支持 | 参数子集 |
| `with DAG(...)` | 支持 | 行为兼容 |
| `@dag` | 支持 | 参数子集 |
| `BaseOperator` | 支持 | 构造参数子集 |
| `@task` | 支持 | Python TaskFlow |
| `XComArg` 自动依赖 | 支持 | 小型值 |
| `PythonOperator` | 支持 | 标准 provider |
| `BashOperator` | 支持 | 标准 provider |
| `EmptyOperator` | 支持 | 标准 provider |
| `>>`、`<<`、`chain` | 支持 | 包含 list |
| Jinja template fields | 支持 | Context 子集 |
| cron / timedelta schedule | 支持 | 无自定义 Timetable |
| catchup | 支持 | |
| backfill | 支持 | CLI 子集 |
| retries | 支持 | 普通和指数退避，`retry_exponential_backoff` 支持 bool/float 子集 |
| `max_active_tasks` | 支持 | 限制单个 DagRun 内并发 |
| trigger rules | 部分支持 | 六种核心规则 |
| `depends_on_past` | 部分支持 | scheduled/backfill 生效，manual 默认隔离 |
| Task logs | 支持 | 本地文件 |
| `dag.test()` | 支持 | 默认隔离状态 |
| Branch Task | 延后 | 状态机已预留 skipped |
| TaskGroup | 延后 | 不影响核心执行 |
| Variable / Connection | 延后 | v0.1 用环境变量或业务密钥系统，easyflow 负责脱敏 |
| Sensor | 延后 | 先不占用 Worker 等待 |
| Dynamic Task Mapping | 不支持 | `map_index` 已预留 |
| setup / teardown | 不支持 | |
| Asset / Dataset | 不支持 | 属于后续扩展层 |
| Provider 插件兼容 | 不支持 | 只提供内置 standard |
| 分布式 Executor | 不支持 | 单机边界 |
| Airflow Web/API | 不支持 | |

## 26. MVP 实现范围

### 26.1 M0：定义与进程内测试

- 包结构和 `easyflow.sdk`
- DAG 三种声明方式
- BaseOperator
- 三个标准 Operator
- TaskFlow 和 XComArg
- 依赖构建与 DAG 验证
- Context 和模板
- `dag.test()`

验收标准：示例 DAG 可以在当前进程正确执行；失败、跳过和返回值语义有测试。

### 26.2 M1：持久化手动运行

- SQLite schema
- DAG 文件发现与隔离解析
- DagRun/TaskInstance 状态机
- SequentialExecutor
- XCom backend
- Task try 日志
- CLI trigger/state/logs/clear
- retries
- 进程退出恢复
- TaskRunner 结果原子提交
- at-least-once 执行语义文档和幂等示例

验收标准：scheduler/runner 被强制结束后，重新启动不会重复提交已经原子 commit success 的任务；
未 commit success 的孤儿任务按重试策略恢复；示例覆盖一个按 logical date 分区写入并可安全重跑的
量化数据抓取任务。

### 26.3 M2：本地并发与时间调度

- LocalExecutor
- cron/timedelta schedule
- scheduler lease
- catchup
- max_active_tasks
- max_active_runs
- backfill
- depends_on_past

验收标准：单机常驻运行一周，定时 DagRun 不重复、不漏建；`parallelism`、`max_active_tasks`
和 `max_active_runs` 同时生效；backfill 区间和顺序正确；`depends_on_past` 能在前一业务区间失败时
阻止后续 scheduled/backfill 任务继续处理。

## 27. 关键设计理由与推翻条件

| 决策 | 当前假设 | 设计理由 | 推翻条件 |
|---|---|---|---|
| 以 Airflow 3 Task SDK 严格兼容子集为基线 | 新项目无需兼容 Airflow 2 内部路径，也不要求全量 Airflow 3 | Airflow 3 已提供稳定 DAG 作者接口，子集更可控 | 主要用户必须直接运行大量 Airflow 2 DAG 或大量超出子集的 Airflow 3 DAG |
| 单机持久化 | 任务可以在同一机器运行 | 保留核心价值并控制复杂度 | 明确需要多机 Worker 或 HA scheduler |
| SQLite 默认状态库 | 只有一个 active scheduler | 零运维、事务能力足够 | 多 scheduler、远程 Worker 或高写并发成为刚需 |
| Executor 不进入 DAG 参数 | 执行方式是部署决策 | 同一 DAG 可在不同环境运行 | 用户存在不可替代的逐 DAG 隔离需求 |
| 默认 JSON XCom | XCom 只传小型控制数据 | 安全、可检查、跨进程稳定 | 可信环境大量依赖任意 Python 对象 |
| TaskRunner 原子提交结果 | 单机 TaskRunner 可以访问受限 StateStore | 已提交 success 后重启不重跑，事件不依赖 scheduler 内存 | 需要远程 Worker 或不允许任务进程持有状态库写权限 |
| 对外部副作用只承诺 at-least-once | 任意 API/文件/数据库副作用无法由调度器统一事务化 | 避免虚假的 exactly-once，要求业务幂等 | easyflow 只调度无外部副作用的纯函数任务 |
| 不提供 `dag.run()` | 正式运行需要 DagRun 和状态库 | 避免测试调用与生产调度混淆 | easyflow 最终退化为纯进程内库 |
| Dataset/Slice 不进入核心 | 当前目标是 Airflow 编排兼容，量化数据用幂等 Task 和外部存储表达 | 减少概念和运行时耦合 | 数据资产调度成为主要产品目标，且核心调度语义已稳定 |
| 不静默接受 Airflow 参数 | 行为一致比表面兼容重要 | 防止用户误以为 pool、queue 等生效 | 不应推翻 |
| 保存 `map_index=-1` | 未来可能支持动态映射 | 避免修改 TaskInstance 主键 | 确定永远不支持动态映射 |
| DAG definition hash 不一致默认停止 | v0.1 不保存 source snapshot | 量化回补需要可审计代码版本，默认混用新代码风险高 | 实现完整 DAG bundle/source 版本化 |

## 28. 三轮查漏补缺

### 28.1 第一轮：用户接口和兼容性

检查问题：

1. 是否只复制了 Airflow 名字，却没有复制语义？
2. 简单 DAG 是否可以在 3～5 行内表达？
3. 经典 Operator 和 TaskFlow 是否共享同一运行模型？
4. 是否把 Executor、StateStore 等内部概念暴露给 DAG 作者？
5. 不支持参数是否会被静默忽略？
6. 是否把“严格兼容子集”误写成“任意 Airflow DAG 都能只改 import 运行”？

发现与修正：

- 原始 `DAG(executor=...)` 把部署决策泄漏到 DAG，已移到全局配置；
- 原始 `dag.run()` 混淆测试和正式运行，改为 `dag.test()` 与 CLI trigger；
- TaskFlow 调用明确返回 XComArg，不直接执行函数；
- 正式兼容入口固定为 `easyflow.sdk`，顶层 import 只做重导出；
- 已知但不支持的 Airflow 参数统一抛出 `UnsupportedFeatureError`；
- 用户目标改为“属于 v0.1 兼容子集的 Airflow 3 DAG 可只改 import 迁移”。

结论：用户概念收敛为 DAG、Task、DagRun、TaskInstance 和 XCom，没有暴露状态库或进程池。

### 28.2 第二轮：状态、时间和恢复

检查问题：

1. scheduler 重启后能否判断哪些任务已经完成？
2. Worker 丢失是否会让 Task 永远停在 running？
3. retry 是否可能重复执行超过次数？
4. catchup/backfill 是否有稳定 logical date 和数据区间？
5. XCom 已写入但状态未成功时，下游会发生什么？
6. 任务成功后 scheduler 崩溃是否会导致已提交成功任务重跑？
7. backfill 是否允许同一 logical date 多次审计型重处理？
8. `depends_on_past` 是否能阻止量化时序任务在前序失败后继续运行？

发现与修正：

- Operator 不再保存运行状态，状态统一放入持久化 TaskInstance；
- 增加 scheduler lease、task lease 和 heartbeat；
- `try_number` 在首次进入 queued 时分配，未 started 的系统重投复用同一 TaskTry；
- 从 v0.1 保存 logical date 和左闭右开 data interval；
- XCom 与 success 状态在同一事务中提交；
- TaskInstance 状态更新采用 compare-and-set，避免并发覆盖；
- TaskRunner 通过受限 StateStore 接口原子提交 success/failure，已提交 success 后重启不重跑；
- backfill 新建独立 run，不覆盖历史 DagRun，并修正唯一性为 scheduled/backfill 分离；
- `depends_on_past` 定义为 scheduled/backfill 生效、manual 默认隔离。

结论：状态机可恢复，时间语义足以支撑 scheduler、catchup 和 backfill；外部副作用采用
at-least-once 模型，由业务幂等保证数据正确。

### 28.3 第三轮：风险、安全和演进

检查问题：

1. 任意 Python 对象反序列化是否隐藏代码执行风险？
2. Bash 重试是否可能重复外部副作用？
3. DAG 代码变化后恢复旧运行是否会执行错误 Task？
4. 第一版预留是否演变成提前实现全部 Airflow？
5. Dataset/Slice 是否仍在核心接口中留下隐性耦合？
6. 量化数据 API key 是否会被写入日志或配置文件？
7. 大型行情数据是否会被误用 XCom 传递？

发现与修正：

- 默认 XCom 改为 JSON，Pickle 必须显式开启不安全反序列化；
- BashOperator 文档明确幂等责任，重试必须由用户显式配置；
- DagRun 保存 definition hash，hash 不一致默认停止，拓扑不兼容时停止而不是猜测映射；
- Dynamic Mapping 只预留 `map_index`，不实现扩展逻辑；
- Branch 只预留 skipped 状态，暂不实现用户接口；
- Dataset/Slice、缓存、数据版本完全移出 v0.1 核心；
- 量化数据抓取密钥使用环境变量或业务密钥系统，日志和模板元数据默认脱敏；
- XCom 只传 manifest、路径、checksum、row_count 等小型控制数据。

结论：危险副作用可见，扩展点只保留不可逆的数据结构，不提前增加用户概念；量化数据流水线
依赖 logical date/data interval、外部幂等存储和审计型 backfill 达到稳定运行。

## 29. 最终结论

easyflow v0.1 的核心不是“少实现几个 Airflow 类”，而是完整实现一条较小但闭合的编排链：

```text
DAG 定义
→ 解析与验证
→ DagRun
→ TaskInstance 依赖判定
→ 本地执行
→ XCom 与日志
→ 重试和终态
→ 持久化恢复
→ 定时调度与 backfill
```

公共接口优先与 Airflow 3 Task SDK 的严格子集保持一致；内部只实现单机所需能力。量化数据场景
通过可靠调度、at-least-once 重试、幂等外部写入、审计型 backfill 和清晰时间区间来稳定运行，
而不是把数据资产系统塞进 v0.1 核心。

任何新增特性必须先回答：

1. 它是否属于 DAG 编排核心？
2. 它是否破坏 Airflow 兼容语义？
3. 它是否迫使用户理解内部实现？
4. 它是否需要扩展状态机、时间模型或持久化主键？

无法明确回答时，不进入 v0.1。
