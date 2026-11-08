---
name: easyflow-usage
description: easyflow 单机工作流调度器的使用指南。用于编写和测试 DAG、配置任务依赖与调度、运行 CLI 或 Web、排查任务及管理回补。
---

# easyflow 使用指南

> 创建时间：2026-11-09 01:29 Asia/Shanghai。创建模型：Codex，基于 GPT-6；环境未提供完整模型 ID。分析范围：easyflow 0.1.0 的 SDK、CLI、Web 和示例；源码基准提交 `d4778622be685143381220ed86a5917b84e282fc`。

easyflow 是单机、SQLite 状态存储的轻量工作流调度器，提供 Python DAG、CLI 和本机 Web；它实现 Airflow 3.2 常用接口的子集，而非完整 Airflow。先确认实际使用的 easyflow 版本、DAG 目录、工作目录及状态库；遇到本文未覆盖或版本已变化的行为，查看该版本的签名、`easyflow ... --help`、源码和示例，不要按 Airflow 的完整功能推断。

## 按任务查阅

- 编写 TaskFlow、Operator、依赖、参数、XCom、重试或模板：读 [DAG 编写参考](references/dag-authoring.md)。
- 安装、启动 Web/scheduler、查询状态、清理任务或回补：读 [运行与管理参考](references/operations.md)。
- 在 easyflow 仓库中，`README.md` 与 `examples/README.md` 提供项目说明和场景索引；实现可查 `src/easyflow/sdk/`、`src/easyflow/providers/standard/operators/`、`src/easyflow/cli.py`、`src/easyflow/_internal/schedules.py`，相应行为可参考 `tests/`。这些路径相对于 easyflow 仓库根目录；Skill 复制到别处时不要把它们误认为当前项目路径。

## 快速写一个手动工作流

在已安装 easyflow 的 Python 环境中，将下例保存为 `dags/greeting.py`：

```python
from datetime import datetime, timezone

from easyflow.sdk import dag, get_current_context, task


@dag(
    dag_id="easyflow_greeting",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    params={"name": "world"},
    tags=["project:demo", "example"],
)
def greeting():
    @task
    def prepare() -> dict[str, str]:
        return {"name": str(get_current_context()["params"]["name"])}

    @task
    def say_hello(value: dict[str, str]) -> str:
        message = f"Hello, {value['name']}!"
        print(message)
        return message

    say_hello(prepare())


workflow = greeting()
```

```bash
easyflow dags list --file dags/greeting.py
easyflow dags test easyflow_greeting --file dags/greeting.py \
  --logical-date 2026-07-01T00:00:00+00:00 --conf-json '{"name":"Ada"}'
```

期望看到 `Hello, Ada!` 和 `success`。调用 `@task` 装饰的函数时，返回值是 `XComArg`，并在构图时建立依赖；只有运行任务时才会执行函数体。`@dag` 定义后需要调用工厂来生成 DAG；DAG ID 在整个实例中应唯一。`project:demo` 是供界面分组的标签，不提供数据或权限隔离。

`dags list` 和其他发现入口会解析并执行 DAG 文件的顶层 Python 代码。`dags test` 是在当前进程中的真实任务执行，不写入运行状态库，但任务仍可能读写文件、调用网络或产生其他副作用。检查将要执行的代码、目标和参数；需要纯演示时使用上例或项目中不写业务文件的示例。

## 必须区分的运行语义

- `schedule=None` 只手动触发；定时工作流需要运行 scheduler。Web 默认在需要时启动本机 scheduler，因此启动 Web 也可能触发定时任务。
- 调度时区取 DAG `start_date` 的时区，未设置时使用 UTC；日期时间必须带时区。cron 与 `timedelta` 的数据区间起止当前都等于触发时刻，前一日等业务窗口要显式计算。
- `dags test` 在内存中顺序运行，立即重试，不覆盖实际 scheduler、并发、持久化或跨运行依赖的行为。手动 `dags trigger` 会创建持久运行并执行；暂停时允许入队但不会执行。
- 任务重试、恢复、回补或清理后重跑都可能再次产生副作用；外部写入应按业务保证幂等。先了解具体影响，再运行实际任务。
- 发现时可能出现跨文件重复 DAG ID 的时间窗口；启动调度前可运行 `dags list` 并检查标准错误，即使命令退出码为 0 也要检查解析问题。该检查不能防止检查后文件再次变化。
- 早于 `start_date` 的 `dags test` 当前仍会执行任务，与部分 Airflow 测试行为不同。使用不早于开始时间的逻辑日期，并核对任务状态或输出。
