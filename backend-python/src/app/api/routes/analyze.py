from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Form, HTTPException

from app.core.config import get_settings
from app.db.engine import engine
from app.db.schema import get_db_schema
from app.repositories.history_repo import (
    conversation_exists,
    create_conversation,
    get_latest_success_turns,
    save_turn,
)
from app.services.chart_intent_ai import suggest_chart_intent
from app.services.conversation_ai import generate_conversation_reply
from app.services.intent import classify_intent, is_chart_only_prompt
from app.services.plotly_mapper import build_plotly_figure
from app.services.prompt_context import build_effective_prompt
from app.services.response_builder import build_assistant_text, build_response_blocks
from app.services.sql_generator import text_to_sql
from app.services.sql_runtime import execute_sql, normalize_and_validate_sql


router = APIRouter(tags=["analyze"])

_SCHEMA_CACHE: Optional[Dict[str, Any]] = None
_SCHEMA_CACHE_TS: Optional[float] = None


def _get_cached_schema() -> Dict[str, Any]:
    import time

    global _SCHEMA_CACHE, _SCHEMA_CACHE_TS
    settings = get_settings()
    now = time.time()
    if _SCHEMA_CACHE is not None and _SCHEMA_CACHE_TS is not None:
        if now - _SCHEMA_CACHE_TS < settings.schema_cache_ttl_seconds:
            return _SCHEMA_CACHE

    schema = get_db_schema(engine)
    _SCHEMA_CACHE = schema
    _SCHEMA_CACHE_TS = now
    return schema


@router.post("/analyze/")
async def analyze(
    prompt: str = Form(...),
    conversation_id: Optional[str] = Form(None),
) -> Dict[str, Any]:
    settings = get_settings()
    user_prompt = (prompt or "").strip()
    if not user_prompt:
        raise HTTPException(status_code=400, detail="Prompt is required")

    if conversation_id and not conversation_exists(engine, conversation_id):
        raise HTTPException(status_code=404, detail="Conversation not found")

    conversation_id = conversation_id or create_conversation(
        engine,
        title=user_prompt[:120],
    )

    latest_turns = get_latest_success_turns(engine, conversation_id, limit=5)
    latest_turn = latest_turns[0] if latest_turns else None
    effective_prompt = build_effective_prompt(user_prompt, latest_turns)

    sql: Optional[str] = None
    out_columns: Optional[list[str]] = None
    out_rows: Optional[list[dict[str, Any]]] = None
    chart_intent: Optional[dict[str, Any]] = None
    fig: Optional[dict[str, Any]] = None
    assistant_text: Optional[str] = None
    response_blocks: Optional[list[dict[str, Any]]] = None
    chart_only_intent = False

    try:
        intent_type = classify_intent(user_prompt)
        if intent_type == "CONVERSATION":
            assistant_text = generate_conversation_reply(user_prompt)
            response_blocks = [{"type": "text", "content": assistant_text}]
            turn_id = save_turn(
                engine,
                conversation_id=conversation_id,
                prompt=user_prompt,
                context_prompt=None,
                sql=None,
                columns=None,
                data=None,
                chart_intent=None,
                plotly=None,
                assistant_text=assistant_text,
                response_blocks=response_blocks,
                status="success",
                error=None,
            )
            return {
                "conversation_id": conversation_id,
                "turn_id": turn_id,
                "prompt": user_prompt,
                "intent_type": intent_type,
                "sql": None,
                "columns": [],
                "data": [],
                "chart_intent": {"make_chart": False},
                "plotly": None,
                "assistant_text": assistant_text,
                "response_blocks": response_blocks,
                "status": "success",
                "created_at": datetime.utcnow().isoformat(),
            }

        can_reuse_last_result = bool(
            latest_turn
            and latest_turn.get("sql")
            and isinstance(latest_turn.get("columns"), list)
            and isinstance(latest_turn.get("data"), list)
        )
        chart_only_intent = is_chart_only_prompt(user_prompt) and can_reuse_last_result

        if chart_only_intent:
            sql = str(latest_turn.get("sql") or "")
            out_columns = latest_turn.get("columns") or []
            out_rows = latest_turn.get("data") or []

            chart_prompt = user_prompt or f"Visualize this query result: {sql[:300]}"
            chart_intent = suggest_chart_intent(
                user_prompt=chart_prompt,
                sql=sql,
                columns=out_columns,
            )
            if chart_intent.get("make_chart"):
                fig = build_plotly_figure(intent=chart_intent, columns=out_columns, rows=out_rows)
            response_blocks = []
            if fig:
                response_blocks.append(
                    {
                        "type": "chart",
                        "chart_type": chart_intent.get("chart_type"),
                        "plotly": fig,
                        "pin_action": {
                            "title": user_prompt[:120] or "Pinned chart",
                            "sql": sql,
                            "chart_type": chart_intent.get("chart_type"),
                            "x_field": chart_intent.get("x"),
                            "y_field": chart_intent.get("y"),
                        },
                    }
                )
        else:
            schema = _get_cached_schema()
            if not schema:
                raise HTTPException(status_code=400, detail="No tables found in database")

            sql = text_to_sql(effective_prompt, schema)
            sql = normalize_and_validate_sql(sql)

            out_columns, out_rows = execute_sql(engine=engine, sql=sql, max_rows=settings.max_result_rows)

            chart_prompt = user_prompt or f"Visualize this query result: {sql[:300]}"
            chart_intent = suggest_chart_intent(
                user_prompt=chart_prompt,
                sql=sql,
                columns=out_columns,
            )
            if chart_intent.get("make_chart"):
                fig = build_plotly_figure(intent=chart_intent, columns=out_columns, rows=out_rows)

            assistant_text = build_assistant_text(
                prompt=user_prompt,
                columns=out_columns,
                rows=out_rows,
                chart_intent=chart_intent,
            )
            response_blocks = build_response_blocks(
                prompt=user_prompt,
                sql=sql,
                columns=out_columns,
                rows=out_rows,
                chart_intent=chart_intent,
                plotly=fig,
            )
            if fig:
                response_blocks.append(
                    {
                        "type": "pin_action",
                        "title": user_prompt[:120] or "Pinned chart",
                        "sql": sql,
                        "chart_type": chart_intent.get("chart_type"),
                        "x_field": chart_intent.get("x"),
                        "y_field": chart_intent.get("y"),
                    }
                )

        turn_id = save_turn(
            engine,
            conversation_id=conversation_id,
            prompt=user_prompt,
            context_prompt=effective_prompt if effective_prompt != user_prompt else None,
            sql=sql,
            columns=out_columns,
            data=out_rows,
            chart_intent=chart_intent,
            plotly=fig,
            assistant_text=assistant_text,
            response_blocks=response_blocks,
            status="success",
            error=None,
        )
    except HTTPException as exc:
        turn_id = save_turn(
            engine,
            conversation_id=conversation_id,
            prompt=user_prompt,
            context_prompt=effective_prompt if effective_prompt != user_prompt else None,
            sql=sql,
            columns=out_columns,
            data=out_rows,
            chart_intent=chart_intent,
            plotly=fig,
            assistant_text=assistant_text,
            response_blocks=response_blocks,
            status="failed",
            error=str(exc.detail),
        )
        raise exc
    except Exception as exc:
        turn_id = save_turn(
            engine,
            conversation_id=conversation_id,
            prompt=user_prompt,
            context_prompt=effective_prompt if effective_prompt != user_prompt else None,
            sql=sql,
            columns=out_columns,
            data=out_rows,
            chart_intent=chart_intent,
            plotly=fig,
            assistant_text=assistant_text,
            response_blocks=response_blocks,
            status="failed",
            error=str(exc),
        )
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "conversation_id": conversation_id,
        "turn_id": turn_id,
        "prompt": user_prompt,
        "intent_type": "DATA_QUERY",
        "sql": sql if not chart_only_intent else None,
        "columns": (out_columns or []) if not chart_only_intent else [],
        "data": (out_rows or []) if not chart_only_intent else [],
        "chart_intent": chart_intent or {"make_chart": False},
        "plotly": fig,
        "assistant_text": assistant_text,
        "response_blocks": response_blocks or [],
        "status": "success",
        "created_at": datetime.utcnow().isoformat(),
    }

