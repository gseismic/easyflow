# KI-20261108-1025-001：异步发现重复 DAG ID 前可能已触发运行

- 创建时间：2026-11-08 10:25 Asia/Shanghai
- 创建模型：Codex，基于 GPT-6；环境未提供实际模型 ID。
- 初始状态：未解决。
- 来源：本次轻量 Airflow 高频功能优化的实现自查及本机复现。
- 发现版本：`WORKTREE`，基准提交 `0e402689a9396eeb7d742c2ed39efa064ece9b47`；涉及未提交文件 `src/easyflow/_internal/dag_parser.py`、`src/easyflow/cli.py` 和 `src/easyflow/_internal/scheduler.py`，该工作区内容尚未固定为提交。引入提交：尚无，本问题出现在本次未提交的异步发现实现中。

## 问题与影响

常驻 scheduler 为避免单个文件解析缓慢阻塞其他 DAG，会先调度已经解析完成的文件。若另一个尚在解析的文件定义了相同 DAG ID，重复 ID 只能在后者完成解析后被识别；在此之前，较快定义中的任务可能已经执行并产生外部副作用。冲突发现后会报告解析问题并停止继续提交这组冲突定义，但无法撤回之前已执行的任务。

依据：`src/easyflow/_internal/dag_parser.py:48` 按文件异步收集并检查当前已完成结果，`src/easyflow/_internal/dag_parser.py:360` 对已发现结果排除重复 ID；`src/easyflow/cli.py:539` 将当前结果提交给 scheduler。

## 复现与临时绕过

本机使用临时 SQLite 状态库和两个 Python 文件复现：一个快速文件定义 `@once` DAG 并创建标记文件，另一个文件先等待 3 秒再声明相同 ID。标记文件在慢文件解析完成前出现，随后 scheduler 报告“发现重复 dag_id”。未触及已有用户数据。

部署或修改 DAG 文件后，先运行 `easyflow dags list --dags-folder DAG目录`，检查标准错误中没有解析错误或重复 ID，再启动 scheduler；确保所有 DAG ID 在目录内唯一。一次性检查会等待各文件解析或超时，不能消除检查之后文件再次变化的风险。

## 2026-11-08 10:26 Asia/Shanghai：创建时间更正

- 模型：Codex，基于 GPT-6；环境未提供实际模型 ID。
- 依据文件系统记录，详情文件实际创建于 10:26:04，开头的 10:25 是创建前预分配编号的时间，并非文件实际创建时间。为保持已建立的 ID 与路径稳定，保留原编号；状态仍为未解决。
