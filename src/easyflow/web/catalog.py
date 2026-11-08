from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from easyflow._internal.dag_parser import DagSource

ProjectKind = Literal["project", "unassigned", "conflict", "unknown"]


@dataclass(frozen=True)
class WorkflowMetadata:
    dag_id: str
    description: str
    project_kind: ProjectKind
    project: str | None
    tags: tuple[str, ...]
    task_ids: tuple[str, ...] = ()
    issue: str | None = None

    @property
    def search_text(self) -> str:
        return " ".join(
            (self.dag_id, self.description, self.project or "", *self.tags)
        ).casefold()

    @property
    def workflow_search_text(self) -> str:
        return " ".join((self.search_text, *self.task_ids)).casefold()

    def as_dict(self) -> dict:
        return {
            "dag_id": self.dag_id,
            "description": self.description,
            "project_kind": self.project_kind,
            "project": self.project,
            "tags": list(self.tags),
            "metadata_issue": self.issue,
        }


def workflow_metadata(source: DagSource) -> WorkflowMetadata:
    project_tags = []
    tags = set()
    invalid = False
    for tag in source.dag.tags:
        if not isinstance(tag, str):
            invalid = True
            continue
        if tag.startswith("project:"):
            project_tags.append(tag[len("project:") :].strip())
        elif tag:
            tags.add(tag)
    projects = set(project_tags)
    if "" in projects or len(projects) > 1:
        kind: ProjectKind = "conflict"
        project = None
        issue = "项目标签为空或指定了多个项目，请保留一个 project:<名称>。"
    elif projects:
        kind = "project"
        project = next(iter(projects))
        issue = None
    else:
        kind = "unassigned"
        project = None
        issue = None
    if invalid:
        issue = (issue + " " if issue else "") + "存在非字符串标签，已忽略。"
    description = source.dag.description
    return WorkflowMetadata(
        dag_id=source.dag_id,
        description=str(description) if description is not None else "",
        project_kind=kind,
        project=project,
        tags=tuple(sorted(tags, key=lambda value: (value.casefold(), value))),
        task_ids=tuple(sorted(task.task_id for task in source.dag.tasks)),
        issue=issue,
    )


def matches_project(metadata: WorkflowMetadata, kind: str, project: str | None) -> bool:
    if kind == "all":
        return True
    if kind == "project":
        return metadata.project_kind == "project" and metadata.project == project
    return metadata.project_kind == kind


def validate_project_filter(kind: str, project: str | None) -> None:
    if kind == "project" and (project is None or not project):
        raise ValueError("请选择项目。")


def search_terms(value: str | None) -> list[str]:
    return value.casefold().split() if value else []
