from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

from easyflow._internal.schedules import validate_schedule
from easyflow.sdk.dag import DAG


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


def load_dag_from_file(file_path: str | Path, dag_id: str) -> DAG:
    path = Path(file_path).resolve()
    candidates = load_dags_from_file(path)
    for source in candidates:
        if source.dag_id == dag_id:
            return source.dag
    available = ", ".join(source.dag_id for source in candidates) or "<none>"
    raise ValueError(f"文件 {path} 中找不到 DAG {dag_id!r}。可用 DAG: {available}")


def load_dags_from_file(file_path: str | Path) -> list[DagSource]:
    path = Path(file_path).resolve()
    module = _load_module(path)
    sources, issues = _sources_from_module(module, path)
    sources, duplicate_issues = _remove_duplicate_dag_ids(sources)
    issues.extend(duplicate_issues)
    if issues:
        first = issues[0]
        dag_part = f" DAG {first.dag_id!r}" if first.dag_id else ""
        raise ValueError(f"文件 {first.file_path}{dag_part} 解析失败：{first.message}")
    return sources


def discover_dag_files(dags_folder: str | Path) -> list[Path]:
    root = Path(dags_folder).resolve()
    if not root.exists():
        raise ValueError(f"DAG 目录不存在: {root}")
    if not root.is_dir():
        raise ValueError(f"DAG 路径不是目录: {root}")
    files = []
    for path in root.rglob("*.py"):
        relative_parts = path.relative_to(root).parts
        if any(part.startswith(".") or part == "__pycache__" for part in relative_parts):
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
        try:
            module = _load_module(path)
        except Exception as exc:  # noqa: BLE001 - DAG import 错误需要收集后继续处理其他文件。
            issues.append(DagParseIssue(path, None, str(exc)))
            continue
        file_sources, file_issues = _sources_from_module(module, path)
        issues.extend(file_issues)
        sources.extend(
            source
            for source in file_sources
            if dag_id is None or source.dag_id == dag_id
        )
    sources, duplicate_issues = _remove_duplicate_dag_ids(sources)
    issues.extend(duplicate_issues)
    return DagDiscoveryResult(
        sources=sorted(sources, key=lambda source: (source.dag_id, source.file_path.as_posix())),
        issues=issues,
    )


def definition_hash_for_dag(dag: DAG, file_path: str | Path | None = None) -> str:
    payload: dict[str, Any] = {
        "dag_id": dag.dag_id,
        "schedule": str(dag.schedule),
        "tasks": [
            {
                "task_id": task.task_id,
                "type": type(task).__name__,
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
            payload["source_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
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
        spec.loader.exec_module(module)
    except Exception:
        if old_module is not None:
            sys.modules[module_name] = old_module
        else:
            sys.modules.pop(module_name, None)
        raise
    return module


def _sources_from_module(module: ModuleType, path: Path) -> tuple[list[DagSource], list[DagParseIssue]]:
    candidates = [value for value in vars(module).values() if isinstance(value, DAG)]
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
