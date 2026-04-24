from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Literal, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from app.langgraph_analyzer import analyze_with_langgraph


router = APIRouter(tags=["asks"])

_ASK_JOBS: Dict[str, Dict[str, Any]] = {}
_ASK_JOBS_LOCK = threading.Lock()
_ASK_JOB_TTL_SECONDS = 60 * 30


def _now_ts() -> float:
    return time.time()


def _cleanup_expired_jobs() -> None:
    now = _now_ts()
    expired: list[str] = []
    with _ASK_JOBS_LOCK:
        for query_id, job in _ASK_JOBS.items():
            updated_at = float(job.get("updated_at", 0))
            if now - updated_at > _ASK_JOB_TTL_SECONDS:
                expired.append(query_id)
        for query_id in expired:
            _ASK_JOBS.pop(query_id, None)


def _stage_to_status(stage_name: str) -> str:
    mapping = {
        "intent_classified": "understanding",
        "searching": "searching",
        "searching_done": "searching",
        "planning": "planning",
        "planning_done": "planning",
        "generating": "generating",
        "sql_generated": "generating",
        "correcting": "correcting",
        "query_executed": "generating",
        "reused_previous_result": "searching",
        "prompt_cache_hit": "searching",
        "chart_intent_ready": "generating",
        "chart_intent_reused": "generating",
        "chart_reused": "generating",
        "chart_ready": "generating",
        "assistant_ready": "generating",
    }
    return mapping.get(stage_name, "generating")


def _new_job(*, query_id: str, query: str, conversation_id: Optional[str]) -> Dict[str, Any]:
    return {
        "query_id": query_id,
        "query": query,
        "conversation_id": conversation_id,
        "status": "understanding",
        "stage": "created",
        "sql": None,
        "columns": [],
        "preview_rows": [],
        "total_count": None,
        "chart_intent": None,
        "chart_config": None,
        "assistant_text": None,
        "retrieved_tables": [],
        "sql_generation_reasoning": None,
        "result": None,
        "error": None,
        "stop_requested": False,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": _now_ts(),
    }


def _update_job(query_id: str, **updates: Any) -> None:
    with _ASK_JOBS_LOCK:
        job = _ASK_JOBS.get(query_id)
        if not job:
            return
        job.update(updates)
        job["updated_at"] = _now_ts()


def _get_job(query_id: str) -> Dict[str, Any]:
    with _ASK_JOBS_LOCK:
        job = _ASK_JOBS.get(query_id)
        if not job:
            raise HTTPException(status_code=404, detail="Query not found")
        return dict(job)


def _handle_emit(query_id: str, payload: Dict[str, Any]) -> None:
    with _ASK_JOBS_LOCK:
        job = _ASK_JOBS.get(query_id)
        if not job:
            return
        if job.get("stop_requested"):
            return

        if payload.get("type") == "meta":
            job["conversation_id"] = payload.get("conversation_id") or job.get("conversation_id")
            job["updated_at"] = _now_ts()
            return

        if payload.get("type") != "stage":
            return

        stage_name = str(payload.get("name") or "").strip()
        job["stage"] = stage_name
        job["status"] = _stage_to_status(stage_name)

        if stage_name in ("sql_generated", "prompt_cache_hit") and payload.get("sql"):
            job["sql"] = payload.get("sql")
        if stage_name in ("query_executed", "reused_previous_result"):
            if isinstance(payload.get("columns"), list):
                job["columns"] = payload.get("columns")
            if isinstance(payload.get("preview_rows"), list):
                job["preview_rows"] = payload.get("preview_rows")
            if payload.get("total_count") is not None:
                job["total_count"] = payload.get("total_count")
        if stage_name in ("chart_intent_ready", "chart_intent_reused") and isinstance(
            payload.get("chart_intent"), dict
        ):
            job["chart_intent"] = payload.get("chart_intent")
        if stage_name in ("chart_ready", "chart_reused"):
            if isinstance(payload.get("chart_config"), dict):
                job["chart_config"] = payload.get("chart_config")
        if stage_name == "assistant_ready" and payload.get("assistant_text"):
            job["assistant_text"] = payload.get("assistant_text")
        if stage_name == "searching_done" and isinstance(payload.get("retrieved_tables"), list):
            job["retrieved_tables"] = payload.get("retrieved_tables")
        if stage_name == "planning_done" and payload.get("sql_generation_reasoning"):
            job["sql_generation_reasoning"] = payload.get("sql_generation_reasoning")
        job["updated_at"] = _now_ts()


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
            if job.get("stop_requested"):
                job["status"] = "stopped"
                job["stage"] = "stopped"
                job["updated_at"] = _now_ts()
                return
            job["status"] = "finished"
            job["stage"] = "finished"
            job["conversation_id"] = final_result.get("conversation_id") or job.get("conversation_id")
            job["result"] = final_result
            job["updated_at"] = _now_ts()
    except HTTPException as exc:
        _update_job(
            query_id,
            status="failed",
            stage="failed",
            error={"code": "HTTP_ERROR", "message": str(exc.detail)},
        )
    except Exception as exc:
        _update_job(
            query_id,
            status="failed",
            stage="failed",
            error={"code": "OTHERS", "message": str(exc)},
        )


class AskRequest(BaseModel):
    query: str = Field(min_length=1)
    conversation_id: Optional[str] = None


class AskResponse(BaseModel):
    query_id: str


class StopAskRequest(BaseModel):
    status: Literal["stopped"] = "stopped"


class StopAskResponse(BaseModel):
    query_id: str


class AskResultError(BaseModel):
    code: Literal["HTTP_ERROR", "OTHERS", "STOPPED"]
    message: str


class AskResultResponse(BaseModel):
    query_id: str
    status: Literal[
        "understanding",
        "searching",
        "planning",
        "generating",
        "correcting",
        "finished",
        "failed",
        "stopped",
    ]
    stage: str
    conversation_id: Optional[str] = None
    sql: Optional[str] = None
    columns: list[str] = Field(default_factory=list)
    preview_rows: list[dict[str, Any]] = Field(default_factory=list)
    total_count: Optional[int] = None
    chart_intent: Optional[dict[str, Any]] = None
    chart_config: Optional[dict[str, Any]] = None
    assistant_text: Optional[str] = None
    retrieved_tables: list[str] = Field(default_factory=list)
    sql_generation_reasoning: Optional[str] = None
    result: Optional[dict[str, Any]] = None
    error: Optional[AskResultError] = None
    created_at: str
    updated_at: float


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
    err = job.get("error")
    error_obj = None
    if isinstance(err, dict):
        code = str(err.get("code") or "OTHERS")
        message = str(err.get("message") or "Unknown error")
        if code not in {"HTTP_ERROR", "OTHERS", "STOPPED"}:
            code = "OTHERS"
        error_obj = AskResultError(code=code, message=message)

    return AskResultResponse(
        query_id=query_id,
        status=str(job.get("status") or "failed"),
        stage=str(job.get("stage") or "failed"),
        conversation_id=job.get("conversation_id"),
        sql=job.get("sql"),
        columns=list(job.get("columns") or []),
        preview_rows=list(job.get("preview_rows") or []),
        total_count=job.get("total_count"),
        chart_intent=job.get("chart_intent"),
        chart_config=job.get("chart_config"),
        assistant_text=job.get("assistant_text"),
        retrieved_tables=list(job.get("retrieved_tables") or []),
        sql_generation_reasoning=job.get("sql_generation_reasoning"),
        result=job.get("result"),
        error=error_obj,
        created_at=str(job.get("created_at")),
        updated_at=float(job.get("updated_at") or _now_ts()),
    )


@router.patch("/v1/asks/{query_id}", response_model=StopAskResponse)
async def stop_ask(query_id: str, _: StopAskRequest) -> StopAskResponse:
    _cleanup_expired_jobs()
    with _ASK_JOBS_LOCK:
        job = _ASK_JOBS.get(query_id)
        if not job:
            raise HTTPException(status_code=404, detail="Query not found")
        job["stop_requested"] = True
        job["status"] = "stopped"
        job["stage"] = "stopped"
        job["error"] = {"code": "STOPPED", "message": "Stopped by user"}
        job["updated_at"] = _now_ts()
    return StopAskResponse(query_id=query_id)
