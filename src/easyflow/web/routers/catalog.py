from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Query, Request

router = APIRouter(prefix="/api")


@router.get("/catalog")
def catalog(request: Request):
    service = request.app.state.service
    try:
        return service.catalog()
    except ValueError as exc:
        scheduler = service.scheduler_status()
        return {
            "projects": [],
            "tags": [],
            "issues": [str(exc)],
            "loading": False,
            "scheduler": scheduler,
            "scheduler_running": scheduler["running"],
        }


@router.get("/workflows")
def workflows(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 25,
    cursor: str | None = None,
    q: Annotated[str | None, Query(max_length=1000)] = None,
    project_kind: Literal[
        "all", "project", "unassigned", "conflict", "unknown"
    ] = "all",
    project: str | None = None,
    tag: Annotated[list[str] | None, Query()] = None,
    state: Literal["all", "running", "failed", "paused"] = "all",
):
    return request.app.state.service.workflows(
        limit=limit,
        cursor=cursor,
        query=q,
        project_kind=project_kind,
        project=project,
        tags=tag or [],
        state=state,
    )


@router.get("/workflows/facets")
def workflow_facets(
    request: Request,
    q: Annotated[str | None, Query(max_length=1000)] = None,
    tag: Annotated[list[str] | None, Query()] = None,
    state: Literal["all", "running", "failed", "paused"] = "all",
):
    return request.app.state.service.workflow_facets(
        query=q, tags=tag or [], state=state
    )
