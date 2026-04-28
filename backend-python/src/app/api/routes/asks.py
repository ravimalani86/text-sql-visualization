from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Literal, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from app.api.ask_status import AskStatus, DEFAULT_STAGE_STATUS, STAGE_TO_STATUS
from app.langgraph.service import analyze_with_langgraph


router = APIRouter(tags=["asks"])


class StatusHistoryItem(BaseModel):
    status: AskStatus
    ts: float
    stage: Optional[str] = None


class AskResultError(BaseModel):
    code: Literal["HTTP_ERROR", "OTHERS", "STOPPED"]
    message: str


class AskResultResponse(BaseModel):
    query_id: str
    status: AskStatus
    conversation_id: Optional[str] = None
    turn_id: Optional[str] = None
    prompt: Optional[str] = None
    intent_type: Optional[str] = None
    sql: Optional[str] = None
    table: dict[str, Any] = Field(default_factory=lambda: {"columns": [], "data": [], "total_count": None})
    chart: dict[str, Any] = Field(default_factory=lambda: {"chart_intent": None, "chart_config": None})
    assistant_text: Optional[str] = None
    source: Optional[str] = None
    error: Optional[AskResultError] = None
    status_history: list[StatusHistoryItem] = Field(default_factory=list)
    created_at: str
    updated_at: float


class AskJobState(AskResultResponse):
    query: str
    stop_requested: bool = False


_ASK_JOBS: Dict[str, AskJobState] = {}
_ASK_JOBS_LOCK = threading.Lock()
_ASK_JOB_TTL_SECONDS = 60 * 30


def _now_ts() -> float:
    return time.time()


def _cleanup_expired_jobs() -> None:
    now = _now_ts()
    expired: list[str] = []
    with _ASK_JOBS_LOCK:
        for query_id, job in _ASK_JOBS.items():
            updated_at = float(job.updated_at)
            if now - updated_at > _ASK_JOB_TTL_SECONDS:
                expired.append(query_id)
        for query_id in expired:
            _ASK_JOBS.pop(query_id, None)


def _stage_to_status(stage_name: str) -> AskStatus:
    return STAGE_TO_STATUS.get(stage_name, DEFAULT_STAGE_STATUS)


def _new_job(*, query_id: str, query: str, conversation_id: Optional[str]) -> AskJobState:
    now = _now_ts()
    return AskJobState(
        query_id=query_id,
        query=query,
        conversation_id=conversation_id,
        status=AskStatus.UNDERSTANDING,
        status_history=[StatusHistoryItem(status=AskStatus.UNDERSTANDING, stage="created", ts=now)],
        turn_id=None,
        prompt=query,
        intent_type=None,
        sql=None,
        table={"columns": [], "data": [], "total_count": None},
        chart={"chart_intent": None, "chart_config": None},
        assistant_text=None,
        source=None,
        error=None,
        stop_requested=False,
        created_at=datetime.utcnow().isoformat(),
        updated_at=now,
    )


def _update_job(query_id: str, **updates: Any) -> None:
    with _ASK_JOBS_LOCK:
        job = _ASK_JOBS.get(query_id)
        if not job:
            return
        for key, value in updates.items():
            setattr(job, key, value)
        status_value = updates.get("status")
        if isinstance(status_value, AskStatus):
            last_stage = job.status_history[-1].stage if job.status_history else None
            job.status_history.append(StatusHistoryItem(status=status_value, stage=last_stage, ts=_now_ts()))
        job.updated_at = _now_ts()


def _get_job(query_id: str) -> AskJobState:
    with _ASK_JOBS_LOCK:
        job = _ASK_JOBS.get(query_id)
        if not job:
            raise HTTPException(status_code=404, detail="Query not found")
        return job


def _append_status(job: AskJobState, *, status: AskStatus, stage: Optional[str], ts: float) -> None:
    job.status = status
    job.status_history.append(StatusHistoryItem(stage=stage, status=status, ts=ts))


def _apply_stage_payload(job: AskJobState, stage_name: str, payload: Dict[str, Any]) -> None:
    if stage_name in ("sql_generated", "prompt_cache_hit") and payload.get("sql"):
        job.sql = payload.get("sql")

    if stage_name in ("query_executed", "reused_previous_result"):
        columns = payload.get("columns")
        preview_rows = payload.get("preview_rows")
        total_count = payload.get("total_count")
        if isinstance(columns, list):
            job.table["columns"] = columns
        if isinstance(preview_rows, list):
            job.table["data"] = preview_rows
        if total_count is not None:
            job.table["total_count"] = total_count

    if stage_name in ("chart_intent_ready", "chart_intent_reused"):
        chart_intent = payload.get("chart_intent")
        if isinstance(chart_intent, dict):
            job.chart["chart_intent"] = chart_intent

    if stage_name in ("chart_ready", "chart_reused"):
        chart_config = payload.get("chart_config")
        if isinstance(chart_config, dict):
            job.chart["chart_config"] = chart_config

    if stage_name == "assistant_ready" and payload.get("assistant_text"):
        job.assistant_text = payload.get("assistant_text")

def _handle_emit(query_id: str, payload: Dict[str, Any]) -> None:
    with _ASK_JOBS_LOCK:
        job = _ASK_JOBS.get(query_id)
        if not job:
            return
        if job.stop_requested:
            return

        if payload.get("type") == "meta":
            job.conversation_id = payload.get("conversation_id") or job.conversation_id
            job.updated_at = _now_ts()
            return

        if payload.get("type") != "stage":
            return

        now = _now_ts()
        stage_name = str(payload.get("name") or "").strip()
        status_name = _stage_to_status(stage_name)
        _append_status(job, status=status_name, stage=stage_name, ts=now)
        _apply_stage_payload(job, stage_name, payload)
        job.updated_at = now


def _run_ask_job(query_id: str, query: str, conversation_id: Optional[str]) -> None:
    try:
        final_result = analyze_with_langgraph(
            prompt=query,
            conversation_id=conversation_id,
            emit_event=lambda payload: _handle_emit(query_id, payload),
        )
        with _ASK_JOBS_LOCK:
            job = _ASK_JOBS.get(query_id)
            if not job:
                return
            if job.stop_requested:
                job.status = AskStatus.STOPPED
                job.updated_at = _now_ts()
                return
            job.status = AskStatus.FINISHED
            job.status_history.append(StatusHistoryItem(stage="finished", status=AskStatus.FINISHED, ts=_now_ts()))
            job.conversation_id = final_result.get("conversation_id") or job.conversation_id
            job.turn_id = final_result.get("turn_id")
            job.prompt = final_result.get("prompt")
            job.intent_type = final_result.get("intent_type")
            job.sql = final_result.get("sql")
            job.table = {
                "columns": list(final_result.get("columns") or []),
                "data": list(final_result.get("data") or []),
                "total_count": final_result.get("total_count"),
            }
            job.chart = {
                "chart_intent": final_result.get("chart_intent"),
                "chart_config": final_result.get("chart_config"),
            }
            job.assistant_text = final_result.get("assistant_text")
            job.source = final_result.get("source")
            job.updated_at = _now_ts()
    except HTTPException as exc:
        _update_job(
            query_id,
            status=AskStatus.FAILED,
            error=AskResultError(code="HTTP_ERROR", message=str(exc.detail)),
        )
    except Exception as exc:
        _update_job(
            query_id,
            status=AskStatus.FAILED,
            error=AskResultError(code="OTHERS", message=str(exc)),
        )


class AskRequest(BaseModel):
    query: str = Field(min_length=1)
    conversation_id: Optional[str] = None


class AskResponse(BaseModel):
    query_id: str


class StopAskRequest(BaseModel):
    status: Literal["stopped"] = AskStatus.STOPPED.value


class StopAskResponse(BaseModel):
    query_id: str


@router.post("/v1/asks", response_model=AskResponse)
async def create_ask(payload: AskRequest, background_tasks: BackgroundTasks) -> AskResponse:
    _cleanup_expired_jobs()
    query = (payload.query or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    query_id = str(uuid.uuid4())
    job = _new_job(query_id=query_id, query=query, conversation_id=payload.conversation_id)
    with _ASK_JOBS_LOCK:
        _ASK_JOBS[query_id] = job

    background_tasks.add_task(_run_ask_job, query_id, query, payload.conversation_id)
    return AskResponse(query_id=query_id)


@router.get("/v1/asks/{query_id}/result", response_model=AskResultResponse)
async def get_ask_result(query_id: str) -> AskResultResponse:
    _cleanup_expired_jobs()
    job = _get_job(query_id)
    payload = job.model_dump(exclude={"query", "stop_requested"})
    return AskResultResponse(**payload)


@router.patch("/v1/asks/{query_id}", response_model=StopAskResponse)
async def stop_ask(query_id: str, _: StopAskRequest) -> StopAskResponse:
    _cleanup_expired_jobs()
    with _ASK_JOBS_LOCK:
        job = _ASK_JOBS.get(query_id)
        if not job:
            raise HTTPException(status_code=404, detail="Query not found")
        job.stop_requested = True
        job.status = AskStatus.STOPPED
        job.error = AskResultError(code="STOPPED", message="Stopped by user")
        job.status_history.append(StatusHistoryItem(stage="stopped", status=AskStatus.STOPPED, ts=_now_ts()))
        job.updated_at = _now_ts()
    return StopAskResponse(query_id=query_id)
