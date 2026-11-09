from __future__ import annotations

import json
import sys
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated

import click
import typer

from easyflow._internal.dag_parser import (
    AsyncDagDiscovery,
    DagParseIssue,
    DagSource,
    discover_dag_sources,
    inspect_dags_from_file,
    load_dag_from_file,
)
from easyflow._internal.persistent_runner import PersistentDagRunner
from easyflow._internal.scheduler import (
    ScheduledRunSummary,
    SchedulerCoordinator,
    SchedulerIssue,
    create_backfill,
    preview_backfill,
    run_scheduler_loop,
    run_scheduler_once_for_sources,
    scheduler_session,
)
from easyflow._internal.state_store import DagRunCapacityError, SQLiteStateStore
from easyflow._internal.schedules import parse_backfill_boundary

DEFAULT_DATABASE_URL = "sqlite:///./.easyflow/easyflow.db"
DEFAULT_LOG_ROOT = "./.easyflow/logs"
DEFAULT_DAGS_FOLDER = "dags"

app = typer.Typer(
    help="单机工作流调度与管理。", no_args_is_help=True, add_completion=False
)
db_app = typer.Typer(help="管理状态库。", no_args_is_help=True)
dags_app = typer.Typer(help="管理 DAG。", no_args_is_help=True)
tasks_app = typer.Typer(help="管理任务实例。", no_args_is_help=True)
backfill_app = typer.Typer(help="回补历史运行。", no_args_is_help=True)
users_app = typer.Typer(help="管理本机 Web 账户。", no_args_is_help=True)
app.add_typer(db_app, name="db")
app.add_typer(dags_app, name="dags")
app.add_typer(tasks_app, name="tasks")
app.add_typer(backfill_app, name="backfill")
app.add_typer(users_app, name="users")


def _configure_parallelism(store: SQLiteStateStore, requested: int | None) -> int:
    return store.configure_parallelism(
        1 if requested is None else requested, override=requested is not None
    )


def _error(message: str, code: int = 1) -> None:
    typer.echo(message, err=True)
    raise typer.Exit(code)


def _source(dag_id: str, file: str | None, dags_folder: str) -> DagSource:
    try:
        source, issues = _resolve_single_dag_source(
            dag_id=dag_id, file_path=file, dags_folder=dags_folder
        )
    except ValueError as exc:
        _error(str(exc))
    _print_dag_parse_issues(issues)
    return source


@db_app.command("init")
def db_init(database_url: str = DEFAULT_DATABASE_URL) -> None:
    """初始化 SQLite 状态库。"""
    path = SQLiteStateStore(database_url).init_db()
    typer.echo(f"Initialized easyflow SQLite database at {path}")


def _auth_store(database_url: str):
    from easyflow.web.auth import AuthStore

    store = SQLiteStateStore(database_url)
    store.init_db()
    return AuthStore(store.path)


def _prompt_password() -> str:
    return typer.prompt(
        "密码（至少 10 个字符）", hide_input=True, confirmation_prompt=True
    )


@users_app.command("create")
def users_create(
    username: str,
    role: Annotated[str, typer.Option("--role", help="admin 或 viewer。")],
    database_url: str = DEFAULT_DATABASE_URL,
) -> None:
    """创建 Web 账户，不在命令行参数中传递密码。"""
    password = _prompt_password()
    try:
        _auth_store(database_url).create_user(username, password, role)
    except ValueError as exc:
        _error(str(exc), 2)
    typer.echo(f"已创建 {username}（{role}）。")


@users_app.command("passwd")
def users_passwd(username: str, database_url: str = DEFAULT_DATABASE_URL) -> None:
    """重置密码并使该用户的现有会话失效。"""
    password = _prompt_password()
    try:
        _auth_store(database_url).set_password(username, password)
    except ValueError as exc:
        _error(str(exc), 2)
    typer.echo(f"已重置 {username} 的密码。")


@users_app.command("list")
def users_list(database_url: str = DEFAULT_DATABASE_URL) -> None:
    """列出本机 Web 账户和角色。"""
    for user in _auth_store(database_url).list_users():
        typer.echo(f"{user.username}\t{user.role}")


@users_app.command("remove")
def users_remove(
    username: str,
    yes: Annotated[bool, typer.Option("--yes", help="确认移除账户。")] = False,
    database_url: str = DEFAULT_DATABASE_URL,
) -> None:
    """移除账户并注销其会话；保留至少一个管理员。"""
    if not yes:
        _error("缺少 --yes，未移除账户。", 2)
    try:
        _auth_store(database_url).remove_user(username)
    except ValueError as exc:
        _error(str(exc), 2)
    typer.echo(f"已移除 {username}。")


@dags_app.command("list")
def dags_list(
    file: str | None = None,
    dags_folder: str = DEFAULT_DAGS_FOLDER,
    dag_id: str | None = None,
) -> None:
    """列出 DAG 和解析错误。"""
    try:
        sources, issues = _resolve_dag_sources(
            file_path=file,
            dags_folder=dags_folder,
            dag_id=dag_id,
            require_sources=False,
        )
    except ValueError as exc:
        _error(str(exc))
    _print_dag_parse_issues(issues)
    if not sources:
        typer.echo("No DAGs found.")
        if issues:
            raise typer.Exit(1)
        return
    for source in sources:
        schedule = (
            source.dag.schedule if source.dag.schedule is not None else "<manual>"
        )
        typer.echo(f"{source.dag_id}\t{source.file_path}\t{schedule}")


def _set_paused(dag_id: str, database_url: str, paused: bool) -> None:
    SQLiteStateStore(database_url).set_dag_paused(dag_id, paused)
    typer.echo(f"{dag_id} {'paused' if paused else 'unpaused'}")


@dags_app.command("pause")
def dags_pause(dag_id: str, database_url: str = DEFAULT_DATABASE_URL) -> None:
    """暂停 DAG 调度。"""
    _set_paused(dag_id, database_url, True)


@dags_app.command("unpause")
def dags_unpause(dag_id: str, database_url: str = DEFAULT_DATABASE_URL) -> None:
    """恢复 DAG 调度。"""
    _set_paused(dag_id, database_url, False)


@dags_app.command("trigger")
def dags_trigger(
    dag_id: str,
    file: str | None = None,
    dags_folder: str = DEFAULT_DAGS_FOLDER,
    run_id: str | None = None,
    logical_date: str | None = None,
    conf_json: str = "{}",
    database_url: str = DEFAULT_DATABASE_URL,
    log_root: str = DEFAULT_LOG_ROOT,
    executor: str = "sequential",
    parallelism: int | None = None,
) -> None:
    """创建并执行一次手动运行。"""
    source = _source(dag_id, file, dags_folder)
    store = SQLiteStateStore(database_url)
    try:
        conf = json.loads(conf_json)
        if not isinstance(conf, dict):
            raise ValueError("--conf-json 必须是 JSON 对象。")
        chosen_parallelism = _configure_parallelism(store, parallelism)
        runner = PersistentDagRunner(store, log_root=log_root)
        created = runner.create_manual_run(
            source.dag,
            file_path=source.file_path,
            logical_date=_parse_logical_date(logical_date),
            run_id=run_id,
            conf=conf,
        )
        result = runner.run_existing(
            source.dag,
            run_id=created,
            file_path=source.file_path,
            executor=executor,
            parallelism=chosen_parallelism,
        )
    except (ValueError, DagRunCapacityError) as exc:
        _error(str(exc))
    typer.echo(f"{result.dag_id} {result.run_id} {result.state.value}")
    if result.state.value != "success":
        raise typer.Exit(1)


@dags_app.command("state")
def dags_state(
    dag_id: str, run_id: str, database_url: str = DEFAULT_DATABASE_URL
) -> None:
    """查询运行状态。"""
    record = SQLiteStateStore(database_url).get_dag_run(dag_id, run_id)
    if record is None:
        _error("DagRun not found")
    typer.echo(record.state)


@dags_app.command("test")
def dags_test(
    dag_id: str,
    logical_date: str | None = None,
    file: str | None = None,
    dags_folder: str = DEFAULT_DAGS_FOLDER,
    conf_json: str = "{}",
) -> None:
    """在当前进程测试 DAG，不写入运行状态库。"""
    source = _source(dag_id, file, dags_folder)
    try:
        conf = json.loads(conf_json)
        if not isinstance(conf, dict):
            raise ValueError("--conf-json 必须是 JSON 对象。")
        dag = load_dag_from_file(source.file_path, dag_id)
        result = dag.test(logical_date=_parse_logical_date(logical_date), run_conf=conf)
    except ValueError as exc:
        _error(str(exc))
    typer.echo(f"{dag_id} {result.run_id} {'success' if result.success else 'failed'}")
    if not result.success:
        raise typer.Exit(1)


@tasks_app.command("state")
def tasks_state(
    dag_id: str, run_id: str, task_id: str, database_url: str = DEFAULT_DATABASE_URL
) -> None:
    """查询任务状态。"""
    state = SQLiteStateStore(database_url).get_task_state(dag_id, run_id, task_id)
    typer.echo(state.value if state else "NULL")


@tasks_app.command("logs")
def tasks_logs(
    dag_id: str, run_id: str, task_id: str, database_url: str = DEFAULT_DATABASE_URL
) -> None:
    """读取任务最新一次尝试的日志。"""
    log_path = SQLiteStateStore(database_url).latest_task_log_path(
        dag_id, run_id, task_id
    )
    if log_path is None:
        _error("Task log not found")
    path = Path(log_path)
    if not path.exists():
        _error(f"Task log file not found: {path}")
    typer.echo(path.read_text(encoding="utf-8"), nl=False)


@tasks_app.command("clear")
def tasks_clear(
    dag_id: str,
    run_id: Annotated[str, typer.Option("--run-id")],
    task_id: Annotated[list[str] | None, typer.Option("--task-id")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    yes: Annotated[bool, typer.Option("--yes")] = False,
    reason: str = "",
    database_url: str = DEFAULT_DATABASE_URL,
) -> None:
    """预览或清理任务实例。"""
    store = SQLiteStateStore(database_url)
    preview = store.preview_clear_task_instances(
        dag_id=dag_id, run_id=run_id, task_ids=task_id
    )
    _print_clear_result(preview)
    if dry_run:
        if preview.blocked:
            raise typer.Exit(1)
        return
    if preview.blocked:
        raise typer.Exit(1)
    if not yes:
        _error("缺少 --yes，未执行 clear。", 2)
    result = store.clear_task_instances(
        dag_id=dag_id, run_id=run_id, task_ids=task_id, actor="cli", reason=reason
    )
    _print_clear_result(result)
    if result.blocked:
        raise typer.Exit(1)


@app.command("scheduler")
def scheduler(
    once: Annotated[bool, typer.Option("--once")] = False,
    dag_id: str | None = None,
    file: str | None = None,
    dags_folder: str = DEFAULT_DAGS_FOLDER,
    now: str | None = None,
    database_url: str = DEFAULT_DATABASE_URL,
    log_root: str = DEFAULT_LOG_ROOT,
    owner: str | None = None,
    lease_seconds: int = 30,
    interval_seconds: float = 5.0,
    max_loops: int | None = None,
    executor: str = "sequential",
    parallelism: int | None = None,
) -> None:
    """运行调度器。"""
    try:
        store = SQLiteStateStore(database_url)
        chosen_parallelism = _configure_parallelism(store, parallelism)
        if once:
            sources, parse_issues = _resolve_dag_sources(
                file_path=file,
                dags_folder=dags_folder,
                dag_id=dag_id,
                require_sources=True,
            )
            _print_dag_parse_issues(parse_issues)
            summaries, issues = run_scheduler_once_for_sources(
                sources=sources,
                store=store,
                now=_parse_logical_date(now) if now else None,
                log_root=log_root,
                owner=owner,
                lease_seconds=lease_seconds,
                executor=executor,
                parallelism=chosen_parallelism,
            )
        elif now:
            _error("scheduler 常驻模式不接受 --now；请使用 --once。", 2)
        elif file and dag_id:
            summaries = run_scheduler_loop(
                dag_id=dag_id,
                file_path=file,
                store=store,
                log_root=log_root,
                owner=owner,
                lease_seconds=lease_seconds,
                interval_seconds=interval_seconds,
                max_loops=max_loops,
                executor=executor,
                parallelism=chosen_parallelism,
            )
            issues = []
        else:
            args = SimpleNamespace(
                database_url=database_url,
                owner=owner,
                lease_seconds=lease_seconds,
                log_root=log_root,
                executor=executor,
                parallelism=chosen_parallelism,
                max_loops=max_loops,
                file=file,
                dags_folder=dags_folder,
                dag_id=dag_id,
                interval_seconds=interval_seconds,
            )
            summaries, issues = _run_scheduler_loop_for_sources(args)
    except (RuntimeError, ValueError) as exc:
        _error(str(exc))
    _print_scheduler_issues(issues)
    if not summaries:
        typer.echo("No scheduled runs created.")
        if issues:
            raise typer.Exit(1)
        return
    for summary in summaries:
        typer.echo(
            f"{summary.dag_id or dag_id or '<unknown>'} {summary.run_id} {summary.state.value}"
        )
    if issues or any(summary.state.value != "success" for summary in summaries):
        raise typer.Exit(1)


@backfill_app.command("create")
def backfill_create(
    dag_id: Annotated[str, typer.Option("--dag-id")],
    start_date: Annotated[
        str,
        typer.Option("--start-date", help="开始日期或带时区的 ISO 时间，包含端点。"),
    ],
    end_date: Annotated[
        str,
        typer.Option(
            "--end-date", help="结束日期（含整日）或带时区的 ISO 时间，包含端点。"
        ),
    ],
    file: str | None = None,
    dags_folder: str = DEFAULT_DAGS_FOLDER,
    reprocessing_behavior: str = "none",
    max_active_runs: int | None = None,
    backfill_id: str | None = None,
    database_url: str = DEFAULT_DATABASE_URL,
    log_root: str = DEFAULT_LOG_ROOT,
    executor: str = "sequential",
    parallelism: int | None = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
) -> None:
    """预览或创建回补。"""
    source = _source(dag_id, file, dags_folder)
    store = SQLiteStateStore(database_url)
    try:
        start, end = (
            parse_backfill_boundary(start_date),
            parse_backfill_boundary(end_date),
        )
        if dry_run:
            preview = preview_backfill(
                dag=source.dag,
                store=store,
                start_date=start,
                end_date=end,
                reprocessing_behavior=reprocessing_behavior,
            )
            for item in preview:
                state = item.latest_state.value if item.latest_state else "none"
                typer.echo(
                    f"{item.logical_date.isoformat()}\t{'create' if item.eligible else 'skip'}\t{state}"
                )
            typer.echo("预览基于当前状态，实际创建时会重新检查。")
            return
        chosen_parallelism = _configure_parallelism(store, parallelism)
        summary = create_backfill(
            dag_id=dag_id,
            file_path=source.file_path,
            store=store,
            start_date=start,
            end_date=end,
            reprocessing_behavior=reprocessing_behavior,
            max_active_runs=max_active_runs,
            backfill_id=backfill_id,
            log_root=log_root,
            executor=executor,
            parallelism=chosen_parallelism,
        )
    except ValueError as exc:
        _error(str(exc), 2)
    typer.echo(f"backfill_id={summary.backfill_id}")
    for run in summary.runs:
        typer.echo(f"{dag_id} {run.run_id} {run.state.value}")
    if not summary.complete:
        typer.echo(
            f"backfill 未完成，待处理区间数: {len(summary.pending)}；已有运行可由 scheduler 继续。",
            err=True,
        )
    if not summary.complete or any(
        run.state.value != "success" for run in summary.runs
    ):
        raise typer.Exit(1)


@app.command("web")
def web(
    dags_folder: str = DEFAULT_DAGS_FOLDER,
    database_url: str = DEFAULT_DATABASE_URL,
    log_root: str = DEFAULT_LOG_ROOT,
    port: int = 8790,
    parallelism: int | None = None,
    no_scheduler: Annotated[bool, typer.Option("--no-scheduler")] = False,
) -> None:
    """启动本机管理界面和调度器。"""
    from easyflow.web.app import serve

    code = serve(
        SimpleNamespace(
            dags_folder=dags_folder,
            database_url=database_url,
            log_root=log_root,
            port=port,
            parallelism=parallelism,
            no_scheduler=no_scheduler,
        )
    )
    if code:
        raise typer.Exit(code)


def main(argv: list[str] | None = None) -> int:
    try:
        result = app(args=argv, prog_name="easyflow", standalone_mode=False)
        return result if isinstance(result, int) else 0
    except click.ClickException as exc:
        exc.show()
        return exc.exit_code
    except click.exceptions.Exit as exc:
        return exc.exit_code


def _parse_logical_date(value: str | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("--logical-date 必须带时区。")
    return parsed


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
    args: SimpleNamespace,
) -> tuple[list[ScheduledRunSummary], list[SchedulerIssue]]:
    store = SQLiteStateStore(args.database_url)
    all_summaries: list[ScheduledRunSummary] = []
    all_issues: list[SchedulerIssue] = []
    loops = 0
    last_issues: tuple[str, ...] = ()
    with (
        closing(AsyncDagDiscovery(args.dags_folder, file_path=args.file)) as discovery,
        scheduler_session(
            store=store, owner=args.owner, lease_seconds=args.lease_seconds
        ) as heartbeat,
    ):
        with SchedulerCoordinator(
            store, heartbeat, args.log_root, args.executor, args.parallelism
        ) as coordinator:
            while True:
                summaries, scheduler_issues = coordinator.collect()
                if args.max_loops is not None:
                    all_summaries.extend(summaries)
                    all_issues.extend(scheduler_issues)
                else:
                    _print_scheduler_issues(scheduler_issues)
                result = discovery.scan(dag_id=args.dag_id, wait_seconds=0.2)
                issue_messages = tuple(
                    f"{item.file_path}:{item.dag_id}:{item.message}"
                    for item in result.issues
                )
                if issue_messages != last_issues:
                    _print_dag_parse_issues(result.issues)
                    last_issues = issue_messages
                coordinator.submit(result.sources, datetime.now(timezone.utc))
                loops += 1
                if args.max_loops is not None and loops >= args.max_loops:
                    summaries, scheduler_issues = coordinator.collect(wait=True)
                    return all_summaries + summaries, all_issues + scheduler_issues
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
