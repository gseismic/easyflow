from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from types import FunctionType, MethodType, ModuleType
from typing import Any
from zoneinfo import ZoneInfo

from easyflow._internal.schedules import validate_schedule
from easyflow._internal.source_imports import LocalSourceFinder, local_source_imports
from easyflow.sdk.dag import DAG, collect_registered_dags
from easyflow.sdk.operators.base import BaseOperator

PARSER_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class DagSource:
    dag_id: str
    file_path: Path
    dag: DAG


@dataclass(frozen=True)
class DagParseIssue:
    file_path: Path
    dag_id: str | None
    message: str


@dataclass(frozen=True)
class DagDiscoveryResult:
    sources: list[DagSource]
    issues: list[DagParseIssue]
    pending: bool = False


class AsyncDagDiscovery:
    """逐文件刷新快照，慢文件不会阻止其他文件完成发现。"""

    def __init__(
        self, dags_folder: str | Path, *, file_path: str | Path | None = None
    ) -> None:
        self.dags_folder = Path(dags_folder)
        self.file_path = Path(file_path).resolve() if file_path else None
        self.pool = ThreadPoolExecutor(
            max_workers=8, thread_name_prefix="easyflow-parse"
        )
        self.entries: dict[Path, tuple[tuple[int, int], float, DagDiscoveryResult]] = {}
        self.pending: dict[
            Path, tuple[tuple[int, int], Future[DagDiscoveryResult]]
        ] = {}

    def close(self) -> None:
        self.pool.shutdown(wait=False, cancel_futures=True)

    def scan(
        self, *, dag_id: str | None = None, wait_seconds: float = 0
    ) -> DagDiscoveryResult:
        files = (
            [self.file_path] if self.file_path else discover_dag_files(self.dags_folder)
        )
        if self.file_path is not None and not self.file_path.is_file():
            raise ValueError(f"DAG 文件不存在: {self.file_path}")
        signatures = {}
        for path in files:
            try:
                stat = path.stat()
            except OSError:
                continue
            signatures[path] = (stat.st_mtime_ns, stat.st_size)
        for path in set(self.entries) - signatures.keys():
            self.entries.pop(path)
        for path in set(self.pending) - signatures.keys():
            self.pending.pop(path)[1].cancel()

        def harvest() -> None:
            for path, (signature, future) in list(self.pending.items()):
                if not future.done():
                    continue
                self.pending.pop(path)
                if signatures.get(path) != signature:
                    continue
                try:
                    result = future.result()
                except Exception as exc:  # noqa: BLE001 - 解析异常只影响当前文件。
                    result = DagDiscoveryResult(
                        [], [DagParseIssue(path, None, str(exc))]
                    )
                self.entries[path] = (signature, time.monotonic(), result)

        harvest()
        for path, signature in signatures.items():
            entry = self.entries.get(path)
            if entry and entry[0] != signature:
                self.entries.pop(path)
                entry = None
            if path not in self.pending and (
                entry is None or time.monotonic() - entry[1] > 5
            ):
                self.pending[path] = (
                    signature,
                    self.pool.submit(inspect_dags_from_file, path),
                )
        if wait_seconds and self.pending:
            wait(
                [item[1] for item in self.pending.values()],
                timeout=wait_seconds,
                return_when=FIRST_COMPLETED,
            )
            harvest()
        sources = [
            source
            for _, _, result in self.entries.values()
            for source in result.sources
            if dag_id is None or source.dag_id == dag_id
        ]
        issues = [
            issue for _, _, result in self.entries.values() for issue in result.issues
        ]
        sources, duplicate_issues = _remove_duplicate_dag_ids(sources)
        issues.extend(duplicate_issues)
        return DagDiscoveryResult(
            sorted(
                sources, key=lambda source: (source.dag_id, source.file_path.as_posix())
            ),
            issues,
            bool(self.pending),
        )


def load_dag_from_file(file_path: str | Path, dag_id: str) -> DAG:
    path = Path(file_path).resolve()
    return _select_dag(_inspect_dags_in_process(path), path, dag_id)


def load_dag_snapshot(file_path: str | Path, dag_id: str) -> DAG:
    path = Path(file_path).resolve()
    return _select_dag(inspect_dags_from_file(path), path, dag_id)


def _select_dag(result: DagDiscoveryResult, path: Path, dag_id: str) -> DAG:
    candidates = result.sources
    for source in candidates:
        if source.dag_id == dag_id:
            return source.dag
    issue = next(
        (item for item in result.issues if item.dag_id in (None, dag_id)), None
    )
    if issue is not None:
        raise ValueError(f"文件 {path} DAG {dag_id!r} 解析失败：{issue.message}")
    available = ", ".join(source.dag_id for source in candidates) or "<none>"
    raise ValueError(f"文件 {path} 中找不到 DAG {dag_id!r}。可用 DAG: {available}")


def load_dags_from_file(file_path: str | Path) -> list[DagSource]:
    result = inspect_dags_from_file(file_path)
    if result.issues and not result.sources:
        first = result.issues[0]
        dag_part = f" DAG {first.dag_id!r}" if first.dag_id else ""
        raise ValueError(f"文件 {first.file_path}{dag_part} 解析失败：{first.message}")
    return result.sources


def inspect_dags_from_file(
    file_path: str | Path, *, timeout: float = PARSER_TIMEOUT_SECONDS
) -> DagDiscoveryResult:
    path = Path(file_path).resolve()
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(str(item or Path.cwd()) for item in sys.path)
    with tempfile.TemporaryDirectory(prefix="easyflow-parser-") as directory:
        output_path = Path(directory) / "snapshot.json"
        try:
            process = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "easyflow._internal.dag_parser",
                    str(path),
                    str(output_path),
                ],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return DagDiscoveryResult(
                [], [DagParseIssue(path, None, f"DAG 解析超时（{timeout} 秒）。")]
            )
        if process.returncode != 0 or not output_path.exists():
            detail = process.stderr.strip()[-2000:]
            message = f"DAG 解析进程退出，退出码 {process.returncode}。"
            return DagDiscoveryResult(
                [], [DagParseIssue(path, None, f"{message} {detail}".strip())]
            )
        try:
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            sources = [
                DagSource(item["dag_id"], path, _restore_snapshot(item))
                for item in payload["dags"]
            ]
            issues = [
                DagParseIssue(path, item["dag_id"], item["message"])
                for item in payload["issues"]
            ]
        except (KeyError, TypeError, ValueError, OSError) as exc:
            return DagDiscoveryResult(
                [], [DagParseIssue(path, None, f"DAG 解析结果无效：{exc}")]
            )
    return DagDiscoveryResult(sources, issues)


def _inspect_dags_in_process(file_path: str | Path) -> DagDiscoveryResult:
    path = Path(file_path).resolve()
    try:
        with local_source_imports(path):
            module = _load_module(path)
            sources, issues = _sources_from_module(module, path)
    except Exception as exc:
        return DagDiscoveryResult([], [DagParseIssue(path, None, str(exc))])
    sources, duplicate_issues = _remove_duplicate_dag_ids(sources)
    issues.extend(duplicate_issues)
    return DagDiscoveryResult(sources, issues)


def discover_dag_files(dags_folder: str | Path) -> list[Path]:
    root = Path(dags_folder).resolve()
    if not root.exists():
        raise ValueError(f"DAG 目录不存在: {root}")
    if not root.is_dir():
        raise ValueError(f"DAG 路径不是目录: {root}")
    files = []
    for path in root.rglob("*.py"):
        relative_parts = path.relative_to(root).parts
        if any(
            part.startswith(".") or part == "__pycache__" for part in relative_parts
        ):
            continue
        files.append(path.resolve())
    return sorted(files)


def discover_dag_sources(
    dags_folder: str | Path,
    *,
    dag_id: str | None = None,
) -> DagDiscoveryResult:
    sources: list[DagSource] = []
    issues: list[DagParseIssue] = []
    for path in discover_dag_files(dags_folder):
        result = inspect_dags_from_file(path)
        issues.extend(result.issues)
        sources.extend(
            source
            for source in result.sources
            if dag_id is None or source.dag_id == dag_id
        )
    sources, duplicate_issues = _remove_duplicate_dag_ids(sources)
    issues.extend(duplicate_issues)
    return DagDiscoveryResult(
        sources=sorted(
            sources, key=lambda source: (source.dag_id, source.file_path.as_posix())
        ),
        issues=issues,
    )


def definition_hash_for_dag(
    dag: DAG, file_path: str | Path | None = None, *, source_sha256: str | None = None
) -> str:
    payload: dict[str, Any] = {
        "dag_id": dag.dag_id,
        "schedule": str(dag.schedule),
        "tasks": [
            {
                "task_id": task.task_id,
                "type": getattr(task, "_easyflow_operator_type", type(task).__name__),
                "upstream": sorted(task.upstream_task_ids),
                "trigger_rule": str(task.trigger_rule),
                "depends_on_past": task.depends_on_past,
            }
            for task in sorted(dag.tasks, key=lambda item: item.task_id)
        ],
    }
    if file_path is not None:
        path = Path(file_path)
        if path.exists():
            payload["source_sha256"] = (
                source_sha256 or hashlib.sha256(path.read_bytes()).hexdigest()
            )
        dependencies = getattr(dag, "_easyflow_dependency_hashes", {})
        payload["dependencies"] = {
            name: digest if source_sha256 is not None else _file_hash(Path(name))
            for name, digest in sorted(dependencies.items())
        }
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_module(path: Path) -> ModuleType:
    module_name = f"_easyflow_dag_{hashlib.sha256(str(path).encode()).hexdigest()}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ValueError(f"不能加载 DAG 文件: {path}")
    module = importlib.util.module_from_spec(spec)
    old_module = sys.modules.get(module_name)
    sys.modules[module_name] = module
    try:
        # 直接编译当前源码，避免同秒且同大小的修改命中陈旧的 pyc。
        source = path.read_bytes()
        module.__easyflow_source_sha256__ = hashlib.sha256(source).hexdigest()
        with collect_registered_dags() as dags:
            exec(compile(source, str(path), "exec"), module.__dict__)
        module.__easyflow_registered_dags__ = dags
    except Exception:
        if old_module is not None:
            sys.modules[module_name] = old_module
        else:
            sys.modules.pop(module_name, None)
        raise
    return module


def _sources_from_module(
    module: ModuleType, path: Path
) -> tuple[list[DagSource], list[DagParseIssue]]:
    candidates = [value for value in vars(module).values() if isinstance(value, DAG)]
    candidates.extend(module.__easyflow_registered_dags__)
    sources: list[DagSource] = []
    issues: list[DagParseIssue] = []
    seen_object_ids: set[int] = set()
    for dag in sorted(candidates, key=lambda item: item.dag_id):
        if id(dag) in seen_object_ids:
            continue
        seen_object_ids.add(id(dag))
        try:
            dag.validate()
            validate_schedule(dag.schedule)
            dag._easyflow_source_sha256 = module.__easyflow_source_sha256__
            dag._easyflow_dependency_hashes = _local_dependency_hashes(
                module, path, dag
            )
        except Exception as exc:  # noqa: BLE001 - 单个 DAG 无效不应阻塞同文件其他 DAG。
            issues.append(DagParseIssue(path, dag.dag_id, str(exc)))
            continue
        sources.append(DagSource(dag_id=dag.dag_id, file_path=path, dag=dag))
    return sources, issues


def _remove_duplicate_dag_ids(
    sources: list[DagSource],
) -> tuple[list[DagSource], list[DagParseIssue]]:
    by_dag_id: dict[str, list[DagSource]] = {}
    for source in sources:
        by_dag_id.setdefault(source.dag_id, []).append(source)

    unique_sources: list[DagSource] = []
    issues: list[DagParseIssue] = []
    for dag_id, group in by_dag_id.items():
        if len(group) == 1:
            unique_sources.append(group[0])
            continue
        files = ", ".join(sorted(source.file_path.as_posix() for source in group))
        for source in group:
            issues.append(
                DagParseIssue(
                    source.file_path,
                    dag_id,
                    f"发现重复 dag_id {dag_id!r}，候选文件: {files}",
                )
            )
    return unique_sources, issues


def _encode_datetime(value: datetime | None) -> dict[str, str | None] | None:
    if value is None:
        return None
    return {"value": value.isoformat(), "zone": getattr(value.tzinfo, "key", None)}


def _decode_datetime(value: dict[str, str | None] | None) -> datetime | None:
    if value is None:
        return None
    result = datetime.fromisoformat(value["value"])
    return result.astimezone(ZoneInfo(value["zone"])) if value["zone"] else result


def _encode_delta(value: timedelta | None) -> list[int] | None:
    return None if value is None else [value.days, value.seconds, value.microseconds]


def _decode_delta(value: list[int] | None) -> timedelta | None:
    return (
        None
        if value is None
        else timedelta(days=value[0], seconds=value[1], microseconds=value[2])
    )


def _snapshot_dag(dag: DAG) -> dict[str, Any]:
    return {
        "dag_id": dag.dag_id,
        "description": dag.description,
        "schedule": _encode_delta(dag.schedule)
        if isinstance(dag.schedule, timedelta)
        else dag.schedule,
        "schedule_is_delta": isinstance(dag.schedule, timedelta),
        "start_date": _encode_datetime(dag.start_date),
        "end_date": _encode_datetime(dag.end_date),
        "catchup": dag.catchup,
        "tags": list(dag.tags),
        "max_active_tasks": dag.max_active_tasks,
        "max_active_runs": dag.max_active_runs,
        "source_sha256": dag._easyflow_source_sha256,
        "dependency_hashes": dag._easyflow_dependency_hashes,
        "tasks": [
            {
                "task_id": task.task_id,
                "operator_type": type(task).__name__,
                "owner": task.owner,
                "retries": task.retries,
                "retry_delay": _encode_delta(task.retry_delay),
                "retry_exponential_backoff": task.retry_exponential_backoff,
                "max_retry_delay": _encode_delta(task.max_retry_delay),
                "trigger_rule": task.trigger_rule.value,
                "depends_on_past": task.depends_on_past,
                "upstream": sorted(task.upstream_task_ids),
            }
            for task in dag.tasks
        ],
    }


def _restore_snapshot(snapshot: dict[str, Any]) -> DAG:
    schedule = snapshot["schedule"]
    if snapshot["schedule_is_delta"]:
        schedule = _decode_delta(schedule)
    dag = DAG(
        snapshot["dag_id"],
        description=snapshot["description"],
        schedule=schedule,
        start_date=_decode_datetime(snapshot["start_date"]),
        end_date=_decode_datetime(snapshot["end_date"]),
        catchup=snapshot["catchup"],
        tags=snapshot["tags"],
        max_active_tasks=snapshot["max_active_tasks"],
        max_active_runs=snapshot["max_active_runs"],
        auto_register=False,
    )
    for item in snapshot["tasks"]:
        operator = BaseOperator(
            task_id=item["task_id"],
            dag=dag,
            owner=item["owner"],
            retries=item["retries"],
            retry_delay=_decode_delta(item["retry_delay"]),
            retry_exponential_backoff=item["retry_exponential_backoff"],
            max_retry_delay=_decode_delta(item["max_retry_delay"]),
            trigger_rule=item["trigger_rule"],
            depends_on_past=item["depends_on_past"],
        )
        operator._easyflow_operator_type = item["operator_type"]
    for item in snapshot["tasks"]:
        for upstream in item["upstream"]:
            dag.get_task(upstream).set_downstream(dag.get_task(item["task_id"]))
    dag._easyflow_source_sha256 = snapshot["source_sha256"]
    dag._easyflow_dependency_hashes = snapshot["dependency_hashes"]
    dag._easyflow_snapshot = True
    return dag


def _file_hash(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return "<missing>"


def _local_dependency_hashes(
    module: ModuleType, dag_path: Path, dag: DAG
) -> dict[str, str]:
    finder = LocalSourceFinder(dag_path)
    pending: list[ModuleType] = [module]
    seen: set[str] = set()
    files: dict[str, str] = {}

    def include(value: Any) -> None:
        if isinstance(value, ModuleType):
            candidate = value
        elif isinstance(value, (FunctionType, MethodType, type)):
            candidate = sys.modules.get(getattr(value, "__module__", ""))
        else:
            return
        filename = getattr(candidate, "__file__", None)
        if not isinstance(filename, (str, os.PathLike)):
            return
        path = Path(filename).resolve()
        if path == dag_path or not finder.is_local(path):
            return
        if str(path) not in files:
            digest = getattr(candidate, "__easyflow_source_sha256__", None)
            if digest is None:
                raise ValueError(f"无法确认本地依赖实际加载的源码：{path}")
            files[str(path)] = digest
            pending.append(candidate)

    for task in dag.tasks:
        include(getattr(task, "python_callable", None))
    while pending:
        current = pending.pop()
        if current.__name__ in seen:
            continue
        seen.add(current.__name__)
        for value in vars(current).values():
            include(value)
    return files


def _write_snapshot(file_path: str, output_path: str) -> None:
    result = _inspect_dags_in_process(file_path)
    payload = {
        "dags": [_snapshot_dag(source.dag) for source in result.sources],
        "issues": [
            {"dag_id": issue.dag_id, "message": issue.message}
            for issue in result.issues
        ],
    }
    Path(output_path).write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )


if __name__ == "__main__":
    _write_snapshot(sys.argv[1], sys.argv[2])
