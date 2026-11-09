---
name: easyflow-usage
description: easyflow 单机工作流调度器的使用指南。用于编写和测试 DAG、配置任务依赖与调度、运行 CLI 或 Web、排查任务及管理回补。
---

# easyflow 使用指南

> 创建时间：2026-11-09 01:29 Asia/Shanghai；更新时间：2026-11-10 04:36 Asia/Shanghai。创建和更新模型：Codex，基于 GPT-6；环境未提供完整模型 ID。创建基准为提交 `d4778622be685143381220ed86a5917b84e282fc`；本次整理依据 easyflow 0.1.0 源码提交 `6fc1614feae89124a5040f22dbd1b41b791bbaa6`。

easyflow 是单机、SQLite 状态存储的轻量工作流调度器，提供 Python DAG、CLI 和本机 Web，实现 Airflow 3.2 常用接口的子集。先确认实际使用的 easyflow 版本、DAG 目录、工作目录及状态库；版本不同时，以对应版本的代码、命令帮助和测试为准。

## 按任务查阅

- 从零安装、编写第一个 DAG、使用 Web、测试、定时、重跑和回补：读[使用教程](references/quickstart.md)。教程可直接供用户阅读。
- 使用 `@dag`、Operator、触发规则、XCom、模板或其他进阶任务配置：读 [DAG 编写进阶](references/dag-authoring.md)。
- 使用独立 scheduler、并发限制、账户或进阶回补选项：读[运行与管理进阶](references/operations.md)。

教程是基本使用流程的主要维护位置；两个进阶参考补充教程未覆盖的细节。只按任务读取需要的参考资料。Skill 可以单独复制；其中提及的 easyflow 源码仓库路径只在用户确有该仓库时适用。

## 执行边界

发现 DAG 会执行其文件顶层代码；测试会真实执行任务；启动 Web 默认可能启动 scheduler 并执行定时 DAG。运行前确认用户授权、目标、路径和实际副作用。清理、重试或回补也可能重复产生外部影响；具体说明和示例见[使用教程](references/quickstart.md)。
