from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

router = APIRouter(prefix="/api/dags/{dag_id}/runs/{run_id}")


class ClearRequest(BaseModel):
    task_ids: list[str] | None = None


def _check_run(request: Request, dag_id: str, run_id: str):
    service = request.app.state.service
    record = service.store.get_dag_run(dag_id, run_id)
    if record is None:
        raise HTTPException(status_code=404, detail="运行不存在。")
    return service, record


@router.get("")
def run_detail(dag_id: str, run_id: str, request: Request):
    service, record = _check_run(request, dag_id, run_id)
    return {
        "run_id": run_id,
        "state": record.state,
        "logical_date": record.logical_date,
        "tasks": {
            key: value.value if value else None
            for key, value in service.store.list_task_states(dag_id, run_id).items()
        },
    }


@router.get("/tasks/{task_id}/logs")
def task_logs(
    dag_id: str, run_id: str, task_id: str, request: Request, attempt: int | None = None
):
    service, _ = _check_run(request, dag_id, run_id)
    if task_id not in service.store.list_task_states(dag_id, run_id):
        raise HTTPException(status_code=404, detail="任务不存在。")
    return service.task_log(dag_id, run_id, task_id, attempt)


@router.post("/clear-preview")
def clear_preview(dag_id: str, run_id: str, body: ClearRequest, request: Request):
    service, _ = _check_run(request, dag_id, run_id)
    result = service.store.preview_clear_task_instances(
        dag_id=dag_id, run_id=run_id, task_ids=body.task_ids
    )
    return {
        "to_clear": result.to_clear,
        "blocked": [
            {"task_id": item.task_id, "state": item.state, "reason": item.reason}
            for item in result.blocked
        ],
    }


@router.post("/clear")
def clear_tasks(dag_id: str, run_id: str, body: ClearRequest, request: Request):
    service, _ = _check_run(request, dag_id, run_id)
    result = service.store.clear_task_instances(
        dag_id=dag_id, run_id=run_id, task_ids=body.task_ids, actor="web"
    )
    if result.blocked:
        raise HTTPException(
            status_code=409, detail="任务状态已变化或仍在运行，请重新预览。"
        )
    return {"to_clear": result.to_clear}
