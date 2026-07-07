# 工作流用户接口示例对比

日期：2026-07-02

状态：讨论稿

## 1. 目的

本文用同一组业务场景对比三套用户接口：

- easyflow 设计接口。
- Apache Airflow 当前推荐的 TaskFlow / Asset 接口。
- Prefect 当前推荐的 Flow / Task / Asset 接口。

本文只关注用户如何写工作流，不讨论内部调度器实现。

本文包含两个例子：

- 简单例子：抓取某个交易日的日线行情，产出 `market.daily_bar`。
- 复杂例子：抓取日线行情，基于 20 日窗口计算 `factor.ma20`，再基于当日日线和 `ma20` 计算 `factor.bias20`。

## 2. 共同业务假设

以下对象是业务库，不属于调度器接口：

```python
# my_project/vendor.py
def fetch_daily_bar(trade_date: str):
    ...
```

```python
# my_project/factor.py
def calc_ma20(bars):
    ...


def calc_bias20(daily_bar, ma20):
    ...
```

```python
# my_project/calendar.py
def last_n_trade_dates(calendar: str, end: str, n: int) -> list[str]:
    ...
```

```python
# my_project/market_store.py
def write_daily_bar(trade_date: str, data):
    ...


def read_daily_bar(trade_dates: str | list[str]):
    ...
```

```python
# my_project/factor_store.py
def write_ma20(trade_date: str, data):
    ...


def read_ma20(trade_date: str):
    ...


def write_bias20(trade_date: str, data):
    ...
```

设计理由：

- 对比调度器用户接口时，业务读取、计算、存储能力应该固定。
- 如果业务 helper 混在调度器接口里，会掩盖 Airflow、Prefect、easyflow 的真实差异。
- easyflow 第一版不预设业务数据存储协议，示例中的 `market_store` 和 `factor_store` 都是用户自己的代码。
- 这里的金融数据只是样例，`daily_bar` 可以替换成订单、日志、报表、特征表等其他数据产品。

## 3. easyflow 设计接口

easyflow 的目标接口是：

```text
函数式写法 + 显式静态 DAG + Dataset/Slice 一等概念
```

### 3.1 简单例子：抓取日线行情

```python
from datetime import date

from easyflow import dag, dataset, task

from my_project import market_store, vendor


daily_bar = dataset("market.daily_bar", slice_by="trade_date")


@task(outputs=[daily_bar], retries=3)
def fetch_daily_bar(trade_date: str):
    df = vendor.fetch_daily_bar(trade_date)
    market_store.write_daily_bar(trade_date, df)


@dag(
    dag_id="market_data_fetch",
    schedule=None,
    start_date=date(2026, 1, 1),
    catchup=False,
    calendar="cn_stock",
    tags=["market"],
    state_store="sqlite:///easyflow.db",
)
def market_data_fetch(trade_date: str):
    fetch_daily_bar(trade_date=trade_date)
```

运行方式：

```bash
easyflow run market_data_fetch --trade-date 2026-07-02
```

语义：

```text
fetch_daily_bar 自己调用 market_store 写入业务数据。
任务成功后，easyflow 把 market.daily_bar[trade_date=2026-07-02] 标记为 ready。
```

设计理由：

- 简单任务不需要学习 easyflow 的数据存储协议。
- `outputs=[daily_bar]` 明确表达外部副作用，用户知道这个任务会产出数据。
- `slice_by="trade_date"` 表达逻辑切片，不表达物理存储分区。
- `state_store` 只记录运行状态和 Dataset Slice 状态，不负责业务数据读写。

### 3.2 复杂例子：窗口指标和指标依赖

```python
from datetime import date

from easyflow import dag, dataset, task

from my_project import factor, factor_store, market_store, vendor


daily_bar = dataset("market.daily_bar", slice_by="trade_date")
ma20 = dataset("factor.ma20", slice_by="trade_date")
bias20 = dataset("factor.bias20", slice_by="trade_date")

daily_bar_20d = daily_bar.window(20)


@task(outputs=[daily_bar], retries=3)
def fetch_daily_bar(trade_date: str):
    df = vendor.fetch_daily_bar(trade_date)
    market_store.write_daily_bar(trade_date, df)


@task(inputs=[daily_bar_20d], outputs=[ma20], retries=2)
def calc_ma20(ctx, trade_date: str):
    slices = ctx.slices(daily_bar_20d)
    dates = [item["trade_date"] for item in slices]
    bars = market_store.read_daily_bar(dates)
    result = factor.calc_ma20(bars)
    factor_store.write_ma20(trade_date, result)


@task(inputs=[daily_bar, ma20], outputs=[bias20], retries=2)
def calc_bias20(trade_date: str):
    bar = market_store.read_daily_bar(trade_date)
    ma = factor_store.read_ma20(trade_date)
    result = factor.calc_bias20(bar, ma)
    factor_store.write_bias20(trade_date, result)


@dag(
    dag_id="daily_factor",
    schedule=None,
    start_date=date(2026, 1, 1),
    catchup=False,
    calendar="cn_stock",
    tags=["factor"],
    state_store="sqlite:///easyflow.db",
)
def daily_factor(trade_date: str):
    fetch_daily_bar(trade_date=trade_date)
    calc_ma20(trade_date=trade_date)
    calc_bias20(trade_date=trade_date)
```

运行方式：

```bash
easyflow run daily_factor --trade-date 2026-07-02
```

补历史：

```bash
easyflow backfill daily_factor --trade-date 2026-06-01:2026-07-02
```

语义：

```text
daily_bar.window(20) 表示 calc_ma20 需要当前 trade_date 往前 20 个 cn_stock 交易日的 daily_bar Slice。
```

```text
calc_bias20 需要当前 trade_date 的 daily_bar 和 ma20。
```

```text
如果历史 daily_bar 切片缺失，calc_ma20 不会隐式补算历史任务，而是进入 UPSTREAM_DATA_MISSING。
```

设计理由：

- `inputs` / `outputs` 是数据依赖声明，不需要用户额外写 `ma20_ref = calc_ma20(...)`。
- `ctx.slices(daily_bar_20d)` 只解析逻辑 Slice，不读取业务数据。
- `window(20)` 是一等依赖，调度器能在运行前做计划预览、缺数检查、缓存判断和重建影响分析。
- DAG body 只声明本次 DAG 中有哪些任务实例，不靠函数返回值传递大对象。
- 复杂依赖仍然可绘图：`fetch_daily_bar -> calc_ma20 -> calc_bias20`，且 `fetch_daily_bar -> calc_bias20`。
- 不暴露 `XComArg`、`Future`、`ResultRef`，这些属于内部调度实现概念。

## 4. Airflow 用户接口

Airflow 当前主流写法是 TaskFlow API。Airflow 3 中原 Dataset 概念已经改名为 Asset，Asset 可以作为任务的 `inlets` / `outlets`，也可以用于数据感知调度。

参考官方文档：

- [Pythonic DAGs with the TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Asset Definitions](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)

### 4.1 简单例子：抓取日线行情

```python
import pendulum
from airflow.sdk import Asset, dag, get_current_context, task

from my_project import market_store, vendor


daily_bar_asset = Asset(
    uri="easyflow://market.daily_bar",
    name="market.daily_bar",
)


def current_trade_date() -> str:
    context = get_current_context()
    dag_run = context["dag_run"]
    return dag_run.conf.get("trade_date") or context["ds"]


@dag(
    dag_id="market_data_fetch",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    tags=["market"],
)
def market_data_fetch():
    @task(outlets=[daily_bar_asset], retries=3)
    def fetch_daily_bar() -> dict:
        trade_date = current_trade_date()
        df = vendor.fetch_daily_bar(trade_date)

        market_store.write_daily_bar(trade_date, df)

        return {
            "dataset": "market.daily_bar",
            "trade_date": trade_date,
        }

    fetch_daily_bar()


market_data_fetch()
```

运行方式：

```bash
airflow dags trigger market_data_fetch -c '{"trade_date": "2026-07-02"}'
```

语义：

```text
outlets=[daily_bar_asset] 表示该 task 会产生 market.daily_bar 这个 Asset 的更新事件。
```

观察：

- Airflow 的 Asset 是逻辑数据对象，但默认不帮用户定义 `slice_by`。
- 业务数据写入需要用户自己调用 `market_store.write_daily_bar`。
- task 返回值更适合传递小型引用，不适合传递大 DataFrame。

### 4.2 复杂例子：窗口指标和指标依赖

```python
import pendulum
from airflow.sdk import Asset, dag, get_current_context, task

from my_project import calendar, factor, factor_store, market_store, vendor


daily_bar_asset = Asset(
    uri="easyflow://market.daily_bar",
    name="market.daily_bar",
)
ma20_asset = Asset(
    uri="easyflow://factor.ma20",
    name="factor.ma20",
)
bias20_asset = Asset(
    uri="easyflow://factor.bias20",
    name="factor.bias20",
)


def current_trade_date() -> str:
    context = get_current_context()
    dag_run = context["dag_run"]
    return dag_run.conf.get("trade_date") or context["ds"]


@dag(
    dag_id="daily_factor",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    tags=["factor"],
)
def daily_factor():
    @task(outlets=[daily_bar_asset], retries=3)
    def fetch_daily_bar() -> dict:
        trade_date = current_trade_date()
        df = vendor.fetch_daily_bar(trade_date)

        market_store.write_daily_bar(trade_date, df)

        return {
            "dataset": "market.daily_bar",
            "trade_date": trade_date,
        }

    @task(inlets=[daily_bar_asset], outlets=[ma20_asset], retries=2)
    def calc_ma20(daily_bar_ref: dict) -> dict:
        trade_date = daily_bar_ref["trade_date"]
        dates = calendar.last_n_trade_dates("cn_stock", trade_date, 20)

        bars = market_store.read_daily_bar(dates)

        result = factor.calc_ma20(bars)

        factor_store.write_ma20(trade_date, result)

        return {
            "dataset": "factor.ma20",
            "trade_date": trade_date,
        }

    @task(inlets=[daily_bar_asset, ma20_asset], outlets=[bias20_asset], retries=2)
    def calc_bias20(daily_bar_ref: dict, ma20_ref: dict) -> dict:
        trade_date = ma20_ref["trade_date"]

        bar = market_store.read_daily_bar(trade_date)
        ma = factor_store.read_ma20(trade_date)

        result = factor.calc_bias20(bar, ma)

        factor_store.write_bias20(trade_date, result)

        return {
            "dataset": "factor.bias20",
            "trade_date": trade_date,
        }

    daily_bar_ref = fetch_daily_bar()
    ma20_ref = calc_ma20(daily_bar_ref)
    calc_bias20(daily_bar_ref, ma20_ref)


daily_factor()
```

运行方式：

```bash
airflow dags trigger daily_factor -c '{"trade_date": "2026-07-02"}'
```

语义：

```text
daily_bar_ref 和 ma20_ref 是 Airflow TaskFlow 的任务返回引用，实际用于构建任务依赖。
```

```text
inlets/outlets 表达资产血缘，但 20 日窗口需要用户自己通过 calendar + market_store 实现。
```

观察：

- DAG 结构显式，任务状态、重试、重跑和 UI 能力成熟。
- 用户需要理解 `Asset`、`inlets`、`outlets`、TaskFlow 返回值和上下文。
- 窗口依赖不是一等用户接口，缺历史切片的检查需要业务代码或额外框架逻辑实现。
- 复杂场景下用户接口负担明显高于 easyflow 目标接口。

## 5. Prefect 用户接口

Prefect 当前主流写法是 `@flow` 和 `@task`。Prefect 也提供 Asset 能力，使用 `@materialize` 标记函数会产出某个 asset。Prefect 的任务依赖主要来自 Python 数据流，任务返回值或 future 传入下游任务时会自动形成状态依赖。

参考官方文档：

- [Tasks](https://docs.prefect.io/v3/concepts/tasks)
- [Assets](https://docs.prefect.io/v3/concepts/assets)
- [How to use assets to track workflow outputs](https://docs.prefect.io/v3/how-to-guides/workflows/assets)

### 5.1 简单例子：抓取日线行情

```python
from prefect import flow
from prefect.assets import materialize

from my_project import market_store, vendor


@materialize("easyflow://market.daily_bar", retries=3)
def fetch_daily_bar(trade_date: str) -> dict:
    df = vendor.fetch_daily_bar(trade_date)

    market_store.write_daily_bar(trade_date, df)

    return {
        "dataset": "market.daily_bar",
        "trade_date": trade_date,
    }


@flow(name="market_data_fetch")
def market_data_fetch(trade_date: str):
    return fetch_daily_bar(trade_date)


if __name__ == "__main__":
    market_data_fetch(trade_date="2026-07-02")
```

运行方式：

```bash
python flows/market_data_fetch.py
```

语义：

```text
@materialize("easyflow://market.daily_bar") 表示该函数物化了一个 Prefect Asset。
```

观察：

- 写法非常接近普通 Python，学习成本低。
- `@materialize` 能表达资产产出，但默认不提供 `slice_by` 这样的逻辑切片模型。
- 业务数据写入需要用户自己调用 `market_store.write_daily_bar`。

### 5.2 复杂例子：窗口指标和指标依赖

```python
from prefect import flow
from prefect.assets import materialize

from my_project import calendar, factor, factor_store, market_store, vendor


@materialize("easyflow://market.daily_bar", retries=3)
def fetch_daily_bar(trade_date: str) -> dict:
    df = vendor.fetch_daily_bar(trade_date)

    market_store.write_daily_bar(trade_date, df)

    return {
        "dataset": "market.daily_bar",
        "trade_date": trade_date,
    }


@materialize(
    "easyflow://factor.ma20",
    asset_deps=["easyflow://market.daily_bar"],
    retries=2,
)
def calc_ma20(trade_date: str, daily_bar_ref: dict) -> dict:
    dates = calendar.last_n_trade_dates("cn_stock", trade_date, 20)

    bars = market_store.read_daily_bar(dates)

    result = factor.calc_ma20(bars)

    factor_store.write_ma20(trade_date, result)

    return {
        "dataset": "factor.ma20",
        "trade_date": trade_date,
    }


@materialize(
    "easyflow://factor.bias20",
    asset_deps=[
        "easyflow://market.daily_bar",
        "easyflow://factor.ma20",
    ],
    retries=2,
)
def calc_bias20(trade_date: str, daily_bar_ref: dict, ma20_ref: dict) -> dict:
    bar = market_store.read_daily_bar(trade_date)
    ma = factor_store.read_ma20(trade_date)

    result = factor.calc_bias20(bar, ma)

    factor_store.write_bias20(trade_date, result)

    return {
        "dataset": "factor.bias20",
        "trade_date": trade_date,
    }


@flow(name="daily_factor")
def daily_factor(trade_date: str):
    daily_bar_ref = fetch_daily_bar(trade_date)
    ma20_ref = calc_ma20(trade_date, daily_bar_ref)
    bias20_ref = calc_bias20(trade_date, daily_bar_ref, ma20_ref)

    return {
        "daily_bar": daily_bar_ref,
        "ma20": ma20_ref,
        "bias20": bias20_ref,
    }


if __name__ == "__main__":
    daily_factor(trade_date="2026-07-02")
```

运行方式：

```bash
python flows/daily_factor.py
```

语义：

```text
daily_bar_ref 传入 calc_ma20，ma20_ref 传入 calc_bias20，因此 Prefect 建立运行时状态依赖。
```

```text
asset_deps 表达资产血缘，20 日窗口仍由 calendar + market_store 在任务内部实现。
```

观察：

- 代码接近普通 Python，适合脚本迁移和本地调试。
- 依赖图很大程度来自运行时调用和参数传递，静态计划能力弱于 Airflow。
- `asset_deps` 可以表达资产依赖，但没有直接表达 `daily_bar.window(20)`。
- 如果用户忘了把上游返回值传给下游，或者只在任务内部读外部数据，任务状态依赖可能不符合真实数据依赖。

## 6. 对比结论

| 维度 | easyflow 设计 | Airflow | Prefect |
|---|---|---|---|
| 简单任务样板代码 | 最少 | 中等 | 少 |
| DAG 显式性 | 强 | 强 | 中等 |
| 数据集一等概念 | 强，`Dataset` | 强，`Asset` | 有，`Asset` |
| 逻辑切片 | 强，`slice_by` | 有 partition 相关能力，但用户接口较重 | 需要用户自行约定 |
| 窗口依赖 | 强，`dataset.window(n)` | 需要用户写业务代码 | 需要用户写业务代码 |
| 任务间大数据传递 | 默认不传，用户自管业务存储 | 不建议通过 XCom 传 | 可以传，但生产中仍应外部存储 |
| 重跑和补历史 | 设计目标是一等能力 | 成熟 | 可做，但更偏运行时流程管理 |
| 用户需要理解的框架概念 | `dag`、`task`、`dataset`、`ctx` | `dag`、`task`、`Asset`、`inlets`、`outlets`、context、XComArg | `flow`、`task`、`materialize`、asset、future、wait_for |

核心判断：

```text
Airflow 的强项是显式调度和运维治理，但用户接口偏重。
```

```text
Prefect 的强项是 Pythonic 和本地体验，但数据依赖容易变成隐式约定。
```

```text
easyflow 应该保留 Airflow 的显式 DAG 和状态模型，吸收 Prefect 的低样板函数式接口，同时把 Dataset/Slice/window 做成第一版核心概念。
```

## 7. 对 easyflow 用户接口的反推

应保留：

- `dataset(name, slice_by=...)`
- `Dataset.window(n)`
- `@task(inputs=[...], outputs=[...])`
- `@dag(schedule=..., start_date=..., catchup=..., calendar=..., tags=..., state_store=...)`
- `ctx.slice(dataset)`
- `ctx.slices(dataset_window)`
- 用户自己的业务 store/client 负责读写数据

应避免：

- 不要把 `XComArg`、`Future`、`ResultRef` 作为主用户概念。
- 不要同时提供 `inputs/outputs` 和大量返回值依赖两套主模型。
- 不要让 `window(20)` 隐式触发 20 次历史任务。
- 不要把 `slice_by` 命名为 `partition_by`，避免和物理存储分区混淆。
- 不要在第一版提供 `ctx.read` / `ctx.write` / `data_store`，避免框架变成通用数据访问平台。
- 不要提供 `output=...` 单数语法糖，第一版只保留 `outputs=[...]`。

需要明确的契约：

- `slice_by` 必须能从 DAG run 参数中找到对应值。
- 一个 DAG 内同一个 Dataset 只能有一个生产任务。
- `inputs=[dataset.window(20)]` 表示当前 slice 需要历史 20 个逻辑 slice。
- 历史 slice 缺失时，任务进入 `UPSTREAM_DATA_MISSING`，不隐式补算。
- `ctx.slices(...)` 只能解析当前任务声明过的窗口输入，避免隐藏数据依赖。
- `outputs=[dataset]` 只声明任务成功后要标记 ready 的 Dataset Slice；业务数据写入由用户任务代码完成。
- task 返回值不作为数据依赖和业务数据传递机制。

## 8. 暂定设计结论

第一版 easyflow 用户接口应以如下形式稳定：

```python
daily_bar = dataset("market.daily_bar", slice_by="trade_date")
ma20 = dataset("factor.ma20", slice_by="trade_date")

daily_bar_20d = daily_bar.window(20)


@task(outputs=[daily_bar])
def fetch_daily_bar(trade_date: str):
    ...


@task(inputs=[daily_bar_20d], outputs=[ma20])
def calc_ma20(ctx, trade_date: str):
    ...


@dag(
    dag_id="daily_factor",
    schedule=None,
    start_date="2026-01-01",
    catchup=False,
    calendar="cn_stock",
)
def daily_factor(trade_date: str):
    fetch_daily_bar(trade_date=trade_date)
    calc_ma20(trade_date=trade_date)
```

设计理由：

- 这个接口能覆盖简单任务，不需要用户理解内部状态模型。
- 这个接口能覆盖窗口指标，不需要把窗口逻辑藏在任务内部。
- 这个接口能在定义期生成稳定 DAG，便于绘图、计划预览、重跑和补算。
- 这个接口没有强绑定金融场景，`slice_by` 可以是 `date`、`hour`、`region`、`batch_id` 等通用业务切片键。
- 这个接口把数据依赖和控制依赖分开，后续可以补充 `a >> b` 表示纯控制依赖。
- 这个接口不绑定业务数据存储，用户已有代码可以直接迁移进任务函数。
