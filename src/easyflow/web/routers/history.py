from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(prefix="/api/history")


@router.get("/runs")
def history(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 25,
    cursor: Annotated[str | None, Query(max_length=32768)] = None,
    dag: Annotated[str | None, Query(max_length=1000)] = None,
    dag_id: str | None = None,
    q: Annotated[str | None, Query(max_length=1000)] = None,
    project_kind: Literal[
        "all", "project", "unassigned", "conflict", "unknown"
    ] = "all",
    project: str | None = None,
    tag: Annotated[list[str] | None, Query()] = None,
    state: Literal["queued", "running", "success", "failed"] | None = None,
):
    return request.app.state.service.history(
        limit=limit,
        cursor=cursor,
        dag=dag,
        dag_id=dag_id,
        state=state,
        query=q,
        project_kind=project_kind,
        project=project,
        tags=tag or [],
    )


@router.get("/runs/facets")
def history_facets(
    request: Request,
    dag: Annotated[str | None, Query(max_length=1000)] = None,
    dag_id: str | None = None,
    q: Annotated[str | None, Query(max_length=1000)] = None,
    tag: Annotated[list[str] | None, Query()] = None,
    state: Literal["queued", "running", "success", "failed"] | None = None,
):
    return request.app.state.service.history_facets(
        dag=dag, dag_id=dag_id, query=q, tags=tag or [], state=state
    )


@router.get("/run")
def run_detail(request: Request, dag_id: str, run_id: str):
    return request.app.state.service.run_detail(dag_id, run_id)


@router.get("/task-log")
def task_log(
    request: Request, dag_id: str, run_id: str, task_id: str, attempt: int | None = None
):
    service = request.app.state.service
    if service.store.get_dag_run(dag_id, run_id) is None:
        raise HTTPException(status_code=404, detail="运行不存在。")
    if task_id not in service.store.list_task_states(dag_id, run_id):
        raise HTTPException(status_code=404, detail="任务不存在。")
    return service.task_log(dag_id, run_id, task_id, attempt)
