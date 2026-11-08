from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from easyflow._internal.scheduler import preview_backfill
from easyflow._internal.state_store import DagRunCapacityError

router = APIRouter(prefix="/api/dags")


class PauseRequest(BaseModel):
    paused: bool


class TriggerRequest(BaseModel):
    conf: dict = Field(default_factory=dict)


class BackfillRequest(BaseModel):
    start_date: date
    end_date: date
    reprocessing_behavior: str = "none"


@router.get("")
def list_dags(request: Request):
    service = request.app.state.service
    try:
        discovered = service.discover()
    except ValueError as exc:
        return {
            "dags": [],
            "issues": [str(exc)],
            "scheduler_running": service.scheduler_running(),
        }
    return {
        "dags": [service.dag_info(source) for source in discovered.sources],
        "issues": [
            f"{item.file_path.name}: {item.message}" for item in discovered.issues
        ],
        "scheduler_running": service.scheduler_running(),
        "loading": discovered.pending,
    }


@router.get("/{dag_id}")
def dag_detail(dag_id: str, request: Request):
    return request.app.state.service.detail(dag_id)


@router.post("/{dag_id}/pause")
def pause_dag(dag_id: str, body: PauseRequest, request: Request):
    service = request.app.state.service
    service.source(dag_id)
    service.store.set_dag_paused(dag_id, body.paused)
    return {"paused": body.paused}


@router.post("/{dag_id}/trigger")
def trigger_dag(dag_id: str, body: TriggerRequest, request: Request):
    service = request.app.state.service
    try:
        run_id = service.trigger(dag_id, body.conf)
    except DagRunCapacityError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {
        "run_id": run_id,
        "paused": service.store.is_dag_paused(dag_id),
        "scheduler_running": service.scheduler_running(),
    }


@router.post("/{dag_id}/backfill-preview")
def backfill_preview(dag_id: str, body: BackfillRequest, request: Request):
    service = request.app.state.service
    source = service.source(dag_id)
    items = preview_backfill(
        dag=source.dag,
        store=service.store,
        start_date=body.start_date,
        end_date=body.end_date,
        reprocessing_behavior=body.reprocessing_behavior,
    )
    return [
        {
            "logical_date": item.logical_date.isoformat(),
            "latest_state": item.latest_state.value if item.latest_state else None,
            "eligible": item.eligible,
        }
        for item in items
    ]
