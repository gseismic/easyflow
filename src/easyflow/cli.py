from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from easyflow._internal.dag_parser import (
    DagParseIssue,
    DagSource,
    discover_dag_sources,
    inspect_dags_from_file,
)
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.scheduler import (
    ScheduledRunSummary,
    SchedulerIssue,
    _run_sources_with_lease,
    create_backfill,
    run_scheduler_loop,
    run_scheduler_once_for_sources,
    scheduler_session,
)
from easyflow._internal.state_store import DagRunCapacityError, SQLiteStateStore

DEFAULT_DATABASE_URL = "sqlite:///./.easyflow/easyflow.db"
DEFAULT_LOG_ROOT = "./.easyflow/logs"
DEFAULT_DAGS_FOLDER = "dags"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="easyflow")
    subparsers = parser.add_subparsers(dest="command")

    db_parser = subparsers.add_parser("db", help="管理 easyflow 状态库")
    db_subparsers = db_parser.add_subparsers(dest="db_command")
    db_init = db_subparsers.add_parser("init", help="初始化 SQLite 状态库")
    db_init.add_argument(
        "--database-url",
        default=DEFAULT_DATABASE_URL,
        help="SQLite 数据库 URL，默认 sqlite:///./.easyflow/easyflow.db",
    )

    dags_parser = subparsers.add_parser("dags", help="DAG 命令")
    dags_subparsers = dags_parser.add_subparsers(dest="dags_command")
    dags_list = dags_subparsers.add_parser("list", help="列出 DAG 文件中的 DAG")
    dags_list.add_argument("--file", help="只解析指定 DAG 文件")
    dags_list.add_argument(
        "--dags-folder", default=DEFAULT_DAGS_FOLDER, help="DAG 目录，默认 dags"
    )
    dags_list.add_argument("--dag-id", help="只列出指定 DAG")

    trigger = dags_subparsers.add_parser("trigger", help="创建并同步执行手动 DagRun")
    trigger.add_argument("dag_id")
    trigger.add_argument("--file", help="包含 DAG 定义的 Python 文件")
    trigger.add_argument(
        "--dags-folder",
        default=DEFAULT_DAGS_FOLDER,
        help="未传 --file 时扫描的 DAG 目录",
    )
    trigger.add_argument("--run-id", help="自定义 run_id，默认 manual__<logical_date>")
    trigger.add_argument(
        "--logical-date",
        help="带时区 ISO datetime，默认当前 UTC 时间",
    )
    trigger.add_argument("--conf-json", default="{}", help="DagRun conf JSON")
    trigger.add_argument("--database-url", default=DEFAULT_DATABASE_URL)
    trigger.add_argument("--log-root", default=DEFAULT_LOG_ROOT)
    trigger.add_argument(
        "--executor", choices=["sequential", "local"], default="sequential"
    )
    trigger.add_argument("--parallelism", type=int, default=1)

    state = dags_subparsers.add_parser("state", help="查询 DagRun 状态")
    state.add_argument("dag_id")
    state.add_argument("run_id")
    state.add_argument("--database-url", default=DEFAULT_DATABASE_URL)

    dags_subparsers.add_parser("test", help="后续里程碑实现完整 Airflow 风格 test CLI")

    tasks_parser = subparsers.add_parser("tasks", help="Task 命令")
    tasks_subparsers = tasks_parser.add_subparsers(dest="tasks_command")
    task_state = tasks_subparsers.add_parser("state", help="查询 TaskInstance 状态")
    task_state.add_argument("dag_id")
    task_state.add_argument("run_id")
    task_state.add_argument("task_id")
    task_state.add_argument("--database-url", default=DEFAULT_DATABASE_URL)

    task_logs = tasks_subparsers.add_parser("logs", help="读取 Task 最新 try 日志")
    task_logs.add_argument("dag_id")
    task_logs.add_argument("run_id")
    task_logs.add_argument("task_id")
    task_logs.add_argument("--database-url", default=DEFAULT_DATABASE_URL)

    task_clear = tasks_subparsers.add_parser(
        "clear", help="清理 TaskInstance，允许重新执行"
    )
    task_clear.add_argument("dag_id")
    task_clear.add_argument("--run-id", required=True)
    task_clear.add_argument(
        "--task-id",
        action="append",
        help="可重复传入；不传则 clear 当前 run 中可清理任务",
    )
    task_clear.add_argument("--dry-run", action="store_true")
    task_clear.add_argument("--yes", action="store_true", help="确认执行危险操作")
    task_clear.add_argument("--reason", default="")
    task_clear.add_argument("--database-url", default=DEFAULT_DATABASE_URL)

    scheduler_parser = subparsers.add_parser("scheduler", help="运行 scheduler")
    scheduler_parser.add_argument(
        "--once", action="store_true", help="只扫描并执行一次"
    )
    scheduler_parser.add_argument(
        "--dag-id", help="只调度指定 DAG；不传则调度发现到的全部 DAG"
    )
    scheduler_parser.add_argument("--file", help="包含 DAG 定义的 Python 文件")
    scheduler_parser.add_argument(
        "--dags-folder",
        default=DEFAULT_DAGS_FOLDER,
        help="未传 --file 时扫描的 DAG 目录",
    )
    scheduler_parser.add_argument(
        "--now", help="带时区 ISO datetime，测试或手动推进时使用"
    )
    scheduler_parser.add_argument("--database-url", default=DEFAULT_DATABASE_URL)
    scheduler_parser.add_argument("--log-root", default=DEFAULT_LOG_ROOT)
    scheduler_parser.add_argument(
        "--owner", help="scheduler lease owner，默认使用当前进程"
    )
    scheduler_parser.add_argument("--lease-seconds", type=int, default=30)
    scheduler_parser.add_argument("--interval-seconds", type=float, default=5.0)
    scheduler_parser.add_argument(
        "--max-loops", type=int, help="测试用：运行指定轮数后退出"
    )
    scheduler_parser.add_argument(
        "--executor", choices=["sequential", "local"], default="sequential"
    )
    scheduler_parser.add_argument("--parallelism", type=int, default=1)

    backfill_parser = subparsers.add_parser("backfill", help="Backfill 命令")
    backfill_subparsers = backfill_parser.add_subparsers(dest="backfill_command")
    backfill_create = backfill_subparsers.add_parser(
        "create", help="创建并同步执行 backfill"
    )
    backfill_create.add_argument("--dag-id", required=True)
    backfill_create.add_argument("--file", help="包含 DAG 定义的 Python 文件")
    backfill_create.add_argument(
        "--dags-folder",
        default=DEFAULT_DAGS_FOLDER,
        help="未传 --file 时扫描的 DAG 目录",
    )
    backfill_create.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    backfill_create.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    backfill_create.add_argument(
        "--reprocessing-behavior",
        choices=["none", "failed", "completed"],
        default="none",
    )
    backfill_create.add_argument("--max-active-runs", type=int)
    backfill_create.add_argument("--backfill-id")
    backfill_create.add_argument("--database-url", default=DEFAULT_DATABASE_URL)
    backfill_create.add_argument("--log-root", default=DEFAULT_LOG_ROOT)
    backfill_create.add_argument(
        "--executor", choices=["sequential", "local"], default="sequential"
    )
    backfill_create.add_argument("--parallelism", type=int, default=1)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "db" and args.db_command == "init":
        path = SQLiteStateStore(args.database_url).init_db()
        print(f"Initialized easyflow SQLite database at {path}")
        return 0
    if args.command == "dags" and args.dags_command == "list":
        try:
            sources, issues = _resolve_dag_sources(
                file_path=args.file,
                dags_folder=args.dags_folder,
                dag_id=args.dag_id,
                require_sources=False,
            )
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        _print_dag_parse_issues(issues)
        if not sources:
            print("No DAGs found.")
            return 1 if issues else 0
        for source in sources:
            schedule = (
                source.dag.schedule if source.dag.schedule is not None else "<manual>"
            )
            print(f"{source.dag_id}\t{source.file_path}\t{schedule}")
        return 0
    if args.command == "dags" and args.dags_command == "trigger":
        logical_date = _parse_logical_date(args.logical_date)
        conf = json.loads(args.conf_json)
        try:
            source, issues = _resolve_single_dag_source(
                dag_id=args.dag_id,
                file_path=args.file,
                dags_folder=args.dags_folder,
            )
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        _print_dag_parse_issues(issues)
        dag = source.dag
        store = SQLiteStateStore(args.database_url)
        runner = PersistentDagRunner(store, log_root=args.log_root)
        try:
            run_id = runner.create_manual_run(
                dag,
                file_path=source.file_path,
                logical_date=logical_date,
                run_id=args.run_id,
                conf=conf,
            )
        except DagRunCapacityError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        result = runner.run_existing(
            dag,
            run_id=run_id,
            file_path=source.file_path,
            executor=args.executor,
            parallelism=args.parallelism,
        )
        print(f"{result.dag_id} {result.run_id} {result.state.value}")
        return 0 if result.state.value == "success" else 1
    if args.command == "dags" and args.dags_command == "state":
        record = SQLiteStateStore(args.database_url).get_dag_run(
            args.dag_id, args.run_id
        )
        if record is None:
            print("DagRun not found", file=sys.stderr)
            return 1
        print(record.state)
        return 0
    if args.command == "dags" and args.dags_command == "test":
        print(
            "easyflow dags test 需要 DAG 文件解析功能，将在后续里程碑实现。",
            file=sys.stderr,
        )
        return 2
    if args.command == "tasks" and args.tasks_command == "state":
        state = SQLiteStateStore(args.database_url).get_task_state(
            args.dag_id,
            args.run_id,
            args.task_id,
        )
        if state is None:
            print("NULL")
        else:
            print(state.value)
        return 0
    if args.command == "tasks" and args.tasks_command == "logs":
        store = SQLiteStateStore(args.database_url)
        log_path = store.latest_task_log_path(args.dag_id, args.run_id, args.task_id)
        if log_path is None:
            print("Task log not found", file=sys.stderr)
            return 1
        path = Path(log_path)
        if not path.exists():
            print(f"Task log file not found: {path}", file=sys.stderr)
            return 1
        print(path.read_text(encoding="utf-8"), end="")
        return 0
    if args.command == "tasks" and args.tasks_command == "clear":
        store = SQLiteStateStore(args.database_url)
        preview = store.preview_clear_task_instances(
            dag_id=args.dag_id,
            run_id=args.run_id,
            task_ids=args.task_id,
        )
        _print_clear_result(preview)
        if args.dry_run:
            return 0 if not preview.blocked else 1
        if preview.blocked:
            return 1
        if not args.yes:
            print("缺少 --yes，未执行 clear。", file=sys.stderr)
            return 2
        result = store.clear_task_instances(
            dag_id=args.dag_id,
            run_id=args.run_id,
            task_ids=args.task_id,
            actor="cli",
            reason=args.reason,
        )
        _print_clear_result(result)
        return 0 if not result.blocked else 1
    if args.command == "scheduler":
        try:
            if args.once:
                sources, parse_issues = _resolve_dag_sources(
                    file_path=args.file,
                    dags_folder=args.dags_folder,
                    dag_id=args.dag_id,
                    require_sources=True,
                )
                _print_dag_parse_issues(parse_issues)
                summaries, scheduler_issues = run_scheduler_once_for_sources(
                    sources=sources,
                    store=SQLiteStateStore(args.database_url),
                    now=_parse_logical_date(args.now) if args.now else None,
                    log_root=args.log_root,
                    owner=args.owner,
                    lease_seconds=args.lease_seconds,
                    executor=args.executor,
                    parallelism=args.parallelism,
                )
            else:
                if args.now:
                    print(
                        "scheduler 常驻模式不接受 --now；请使用 --once 测试指定时间。",
                        file=sys.stderr,
                    )
                    return 2
                if args.file and args.dag_id:
                    summaries = run_scheduler_loop(
                        dag_id=args.dag_id,
                        file_path=args.file,
                        store=SQLiteStateStore(args.database_url),
                        log_root=args.log_root,
                        owner=args.owner,
                        lease_seconds=args.lease_seconds,
                        interval_seconds=args.interval_seconds,
                        max_loops=args.max_loops,
                        executor=args.executor,
                        parallelism=args.parallelism,
                    )
                    scheduler_issues = []
                else:
                    summaries, scheduler_issues = _run_scheduler_loop_for_sources(args)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        _print_scheduler_issues(scheduler_issues)
        if not summaries:
            print("No scheduled runs created.")
            return 1 if scheduler_issues else 0
        for summary in summaries:
            dag_id = summary.dag_id or args.dag_id or "<unknown>"
            print(f"{dag_id} {summary.run_id} {summary.state.value}")
        return (
            0
            if all(summary.state.value == "success" for summary in summaries)
            and not scheduler_issues
            else 1
        )
    if args.command == "backfill" and args.backfill_command == "create":
        try:
            source, issues = _resolve_single_dag_source(
                dag_id=args.dag_id,
                file_path=args.file,
                dags_folder=args.dags_folder,
            )
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        _print_dag_parse_issues(issues)
        summary = create_backfill(
            dag_id=args.dag_id,
            file_path=source.file_path,
            store=SQLiteStateStore(args.database_url),
            start_date=_parse_date(args.start_date),
            end_date=_parse_date(args.end_date),
            reprocessing_behavior=args.reprocessing_behavior,
            max_active_runs=args.max_active_runs,
            backfill_id=args.backfill_id,
            log_root=args.log_root,
            executor=args.executor,
            parallelism=args.parallelism,
        )
        print(f"backfill_id={summary.backfill_id}")
        for run in summary.runs:
            print(f"{args.dag_id} {run.run_id} {run.state.value}")
        if not summary.complete:
            print(
                f"backfill 未完成，待处理区间数: {len(summary.pending)}；已有运行可由 scheduler 继续，"
                "容量释放后请使用新的 backfill_id 重试剩余日期。",
                file=sys.stderr,
            )
        return (
            0
            if summary.complete
            and all(run.state.value == "success" for run in summary.runs)
            else 1
        )
    parser.print_help()
    return 1


def _parse_logical_date(value: str | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("--logical-date 必须带时区。")
    return parsed


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _resolve_single_dag_source(
    *,
    dag_id: str,
    file_path: str | None,
    dags_folder: str,
) -> tuple[DagSource, list[DagParseIssue]]:
    sources, issues = _resolve_dag_sources(
        file_path=file_path,
        dags_folder=dags_folder,
        dag_id=dag_id,
        require_sources=True,
    )
    if len(sources) != 1:
        raise ValueError(f"找到 {len(sources)} 个 DAG {dag_id!r}，预期恰好 1 个。")
    return sources[0], issues


def _resolve_dag_sources(
    *,
    file_path: str | None,
    dags_folder: str,
    dag_id: str | None,
    require_sources: bool,
) -> tuple[list[DagSource], list[DagParseIssue]]:
    if file_path:
        result = inspect_dags_from_file(file_path)
        sources = [
            source
            for source in result.sources
            if dag_id is None or source.dag_id == dag_id
        ]
        if require_sources and not sources:
            issue = next(
                (item for item in result.issues if item.dag_id in (None, dag_id)), None
            )
            if issue is not None:
                raise ValueError(
                    f"文件 {issue.file_path} DAG {dag_id!r} 解析失败：{issue.message}"
                )
            raise ValueError(
                f"文件 {Path(file_path).resolve()} 中未找到 DAG {dag_id!r}。"
            )
        return sources, result.issues

    result = discover_dag_sources(dags_folder, dag_id=dag_id)
    if require_sources and not result.sources:
        target = f" DAG {dag_id!r}" if dag_id else ""
        raise ValueError(
            f"DAG 目录 {Path(dags_folder).resolve()} 中未找到可运行{target}。"
        )
    return result.sources, result.issues


def _run_scheduler_loop_for_sources(
    args: argparse.Namespace,
) -> tuple[list[ScheduledRunSummary], list[SchedulerIssue]]:
    store = SQLiteStateStore(args.database_url)
    all_summaries: list[ScheduledRunSummary] = []
    all_issues: list[SchedulerIssue] = []
    loops = 0
    with scheduler_session(
        store=store, owner=args.owner, lease_seconds=args.lease_seconds
    ) as heartbeat:
        while True:
            sources, parse_issues = _resolve_dag_sources(
                file_path=args.file,
                dags_folder=args.dags_folder,
                dag_id=args.dag_id,
                require_sources=False,
            )
            _print_dag_parse_issues(parse_issues)
            summaries, scheduler_issues = _run_sources_with_lease(
                sources=sources,
                store=store,
                now=datetime.now(timezone.utc),
                log_root=args.log_root,
                executor=args.executor,
                parallelism=args.parallelism,
                heartbeat=heartbeat,
            )
            all_summaries.extend(summaries)
            all_issues.extend(scheduler_issues)
            loops += 1
            if args.max_loops is not None and loops >= args.max_loops:
                return all_summaries, all_issues
            if heartbeat.lost.wait(args.interval_seconds):
                raise RuntimeError("scheduler lease 已丢失，停止调度。")


def _print_dag_parse_issues(issues: list[DagParseIssue]) -> None:
    for issue in issues:
        dag_part = f" dag_id={issue.dag_id}" if issue.dag_id else ""
        print(
            f"DAG_PARSE_ISSUE file={issue.file_path}{dag_part} message={issue.message}",
            file=sys.stderr,
        )


def _print_scheduler_issues(issues: list[SchedulerIssue]) -> None:
    for issue in issues:
        print(
            f"SCHEDULER_ISSUE dag_id={issue.dag_id} file={issue.file_path} message={issue.message}",
            file=sys.stderr,
        )


def _print_clear_result(result: object) -> None:
    to_clear = getattr(result, "to_clear")
    blocked = getattr(result, "blocked")
    mode = "DRY-RUN" if getattr(result, "dry_run") else "EXECUTED"
    print(f"{mode} clear candidates: {', '.join(to_clear) if to_clear else '<none>'}")
    for item in blocked:
        print(f"BLOCKED {item.task_id} state={item.state} reason={item.reason}")


if __name__ == "__main__":
    raise SystemExit(main())
