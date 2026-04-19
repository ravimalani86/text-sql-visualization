from __future__ import annotations

import json
import queue
import threading
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from fastapi import APIRouter, Form, HTTPException
from fastapi.responses import StreamingResponse

from app.core.config import get_settings
from app.db.engine import engine
from app.db.schema import get_db_schema
from app.repositories.history_repo import (
    conversation_exists,
    create_conversation,
    find_latest_success_by_prompt,
    get_latest_success_turns,
    save_turn,
)
from app.services.chart_intent_ai import suggest_chart_intent
from app.services.conversation_ai import generate_conversation_reply
from app.services.intent import classify_intent, is_chart_only_prompt
from app.services.plotly_mapper import build_plotly_figure, normalize_chart_config
from app.services.prompt_context import build_effective_prompt
from app.core.config import DEFAULT_PAGE_SIZE
from app.services.response_builder import build_assistant_text, build_response_blocks
from app.services.schema_selector import select_relevant_schema
from app.services.sql_planner import generate_sql_plan
from app.services.sql_generator import correct_sql, text_to_sql
from app.services.sql_runtime import execute_count, execute_sql, normalize_and_validate_sql


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


def _ndjson_line(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, default=str) + "\n").encode("utf-8")


def _prompt_explicitly_wants_chart(prompt: str) -> bool:
    p = (prompt or "").lower()
    keywords = ("chart", "graph", "plot", "visual", "line", "bar", "area", "pie", "scatter", "stacked", "grouped")
    return any(k in p for k in keywords)


def _is_numeric_value(v: Any) -> bool:
    if v is None or isinstance(v, bool):
        return False
    if isinstance(v, (int, float)):
        return True
    try:
        float(str(v).replace(",", "").strip())
        return True
    except Exception:
        return False


def _fallback_chart_intent_for_explicit_request(*, prompt: str, columns: list[str], rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not columns or not rows:
        return {"make_chart": False}

    numeric_cols: list[str] = []
    for c in columns:
        sample = next((r.get(c) for r in rows if r.get(c) is not None), None)
        if _is_numeric_value(sample):
            numeric_cols.append(c)
    if not numeric_cols:
        return {"make_chart": False}

    x_col: Optional[str] = None
    for c in columns:
        if c in numeric_cols:
            continue
        sample = next((r.get(c) for r in rows if r.get(c) is not None), None)
        if sample is not None:
            x_col = c
            break
    if x_col is None:
        x_col = columns[0] if columns else None

    p = (prompt or "").lower()
    chart_type = "bar"
    if "line" in p or "month" in p or "year" in p or "trend" in p:
        chart_type = "line"
    elif "scatter" in p:
        chart_type = "scatter"
    elif "area" in p:
        chart_type = "area"
    elif "pie" in p:
        chart_type = "pie"
    elif "stacked" in p and "area" in p:
        chart_type = "stacked_area"
    elif "stacked" in p:
        chart_type = "stacked_bar"
    elif "grouped" in p:
        chart_type = "grouped_bar"

    intent: dict[str, Any] = {"make_chart": True, "chart_type": chart_type}
    if x_col in columns:
        intent["x"] = x_col
    intent["y"] = numeric_cols[0]
    if len(numeric_cols) > 1 and chart_type != "pie":
        intent["y_fields"] = numeric_cols
        intent["comparison_mode"] = "multi_metric"
    return intent


def _analyze_core(
    *,
    prompt: str,
    conversation_id: Optional[str],
    emit_event: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    def emit(payload: Dict[str, Any]) -> None:
        if emit_event:
            emit_event(payload)

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
    emit({"type": "meta", "conversation_id": conversation_id})

    latest_turns = get_latest_success_turns(engine, conversation_id, limit=settings.max_turns_in_conversation)
    latest_turn = latest_turns[0] if latest_turns else None
    effective_prompt = build_effective_prompt(user_prompt, latest_turns)

    sql: Optional[str] = None
    out_columns: Optional[list[str]] = None
    out_rows: Optional[list[dict[str, Any]]] = None
    total_count: Optional[int] = None
    chart_intent: Optional[dict[str, Any]] = None
    fig: Optional[dict[str, Any]] = None
    assistant_text: Optional[str] = None
    response_blocks: Optional[list[dict[str, Any]]] = None
    chart_only_intent = False
    response_source = "llm"
    prompt_cache_hit = False
    prompt_mentions_chart = _prompt_explicitly_wants_chart(user_prompt)
    page_size = DEFAULT_PAGE_SIZE

    try:
        intent_type = classify_intent(user_prompt)
        emit({"type": "stage", "name": "intent_classified", "intent_type": intent_type})
        if intent_type == "CONVERSATION":
            assistant_text = generate_conversation_reply(user_prompt)
            emit({"type": "stage", "name": "assistant_ready", "assistant_text": assistant_text})
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
                "chart_config": None,
                "assistant_text": assistant_text,
                "response_blocks": response_blocks,
                "status": "success",
                "source": response_source,
                "created_at": datetime.utcnow().isoformat(),
            }

        if settings.reuse_sql_from_history_by_prompt:
            prompt_cache_turn = find_latest_success_by_prompt(engine, user_prompt)
            prompt_cache_hit = bool(
                prompt_cache_turn
                and prompt_cache_turn.get("sql")
                and isinstance(prompt_cache_turn.get("columns"), list)
                and isinstance(prompt_cache_turn.get("data"), list)
            )
        else:
            prompt_cache_turn = None
            prompt_cache_hit = False
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
            total_count = latest_turn.get("total_count") or len(out_rows)
            emit(
                {
                    "type": "stage",
                    "name": "reused_previous_result",
                    "columns": out_columns,
                    "row_count": len(out_rows),
                    "total_count": total_count,
                    "preview_rows": out_rows[:page_size],
                    "page": 1,
                    "page_size": page_size,
                }
            )

            if prompt_cache_hit:
                cached_chart_intent = prompt_cache_turn.get("chart_intent")
                cached_plotly = prompt_cache_turn.get("plotly")
                if isinstance(cached_chart_intent, dict):
                    chart_intent = cached_chart_intent
                    emit({"type": "stage", "name": "chart_intent_reused", "chart_intent": chart_intent})
                else:
                    chart_intent = {"make_chart": False}

                if isinstance(cached_plotly, dict):
                    fig = normalize_chart_config(cached_plotly)
                    emit({"type": "stage", "name": "chart_reused", "chart_config": fig, "plotly": fig})
                elif prompt_mentions_chart:
                    chart_prompt = user_prompt or f"Visualize this query result: {sql[:300]}"
                    if chart_intent.get("make_chart"):
                        fig = build_plotly_figure(intent=chart_intent, columns=out_columns, rows=out_rows)
                        emit({"type": "stage", "name": "chart_ready", "chart_config": fig, "plotly": fig})
                    else:
                        chart_intent = suggest_chart_intent(
                            user_prompt=chart_prompt,
                            sql=sql,
                            columns=out_columns,
                        )
                        if prompt_mentions_chart and not chart_intent.get("make_chart"):
                            chart_intent = _fallback_chart_intent_for_explicit_request(
                                prompt=user_prompt,
                                columns=out_columns,
                                rows=out_rows,
                            )
                        emit({"type": "stage", "name": "chart_intent_ready", "chart_intent": chart_intent})
                        if chart_intent.get("make_chart"):
                            fig = build_plotly_figure(intent=chart_intent, columns=out_columns, rows=out_rows)
                            emit({"type": "stage", "name": "chart_ready", "chart_config": fig, "plotly": fig})
            else:
                chart_prompt = user_prompt or f"Visualize this query result: {sql[:300]}"
                chart_intent = suggest_chart_intent(
                    user_prompt=chart_prompt,
                    sql=sql,
                    columns=out_columns,
                )
                if prompt_mentions_chart and not chart_intent.get("make_chart"):
                    chart_intent = _fallback_chart_intent_for_explicit_request(
                        prompt=user_prompt,
                        columns=out_columns,
                        rows=out_rows,
                    )
                emit({"type": "stage", "name": "chart_intent_ready", "chart_intent": chart_intent})
                if chart_intent.get("make_chart"):
                    fig = build_plotly_figure(intent=chart_intent, columns=out_columns, rows=out_rows)
                    emit({"type": "stage", "name": "chart_ready", "chart_config": fig, "plotly": fig})
            response_blocks = []
            if fig:
                response_blocks.append(
                    {
                        "type": "chart",
                        "chart_type": chart_intent.get("chart_type"),
                        "chart_config": fig,
                        "plotly": fig,
                        "pin_action": {
                            "title": user_prompt[:120] or "Pinned chart",
                            "sql": sql,
                            "chart_type": chart_intent.get("chart_type"),
                            "x_field": chart_intent.get("x"),
                            "y_field": chart_intent.get("y"),
                            "series_field": chart_intent.get("series"),
                        },
                    }
                )
        else:
            if prompt_cache_hit:
                response_source = "history_cache"
                sql = str(prompt_cache_turn.get("sql") or "")
                sql = normalize_and_validate_sql(sql)
                emit({"type": "stage", "name": "prompt_cache_hit", "sql": sql})
                total_count = execute_count(engine=engine, base_sql=sql)
                out_columns, out_rows = execute_sql(engine=engine, sql=sql, max_rows=settings.max_result_rows)
                emit(
                    {
                        "type": "stage",
                        "name": "query_executed",
                        "sql": sql,
                        "columns": out_columns,
                        "row_count": len(out_rows),
                        "total_count": total_count,
                        "preview_rows": out_rows[:page_size],
                        "page": 1,
                        "page_size": page_size,
                    }
                )
            else:
                schema = _get_cached_schema()
                if not schema:
                    raise HTTPException(status_code=400, detail="No tables found in database")

                emit(
                    {
                        "type": "stage",
                        "name": "searching",
                        "table_count": len(schema),
                    }
                )
                selected_schema = select_relevant_schema(
                    user_prompt=effective_prompt,
                    schema=schema,
                    max_tables=settings.schema_search_max_tables,
                )
                emit(
                    {
                        "type": "stage",
                        "name": "searching_done",
                        "retrieved_tables": list(selected_schema.keys()),
                    }
                )

                reasoning_plan: Optional[str] = None
                if settings.enable_sql_planning:
                    emit({"type": "stage", "name": "planning"})
                    reasoning_plan = generate_sql_plan(
                        user_prompt=effective_prompt,
                        schema=selected_schema,
                    )
                    emit(
                        {
                            "type": "stage",
                            "name": "planning_done",
                            "sql_generation_reasoning": reasoning_plan,
                        }
                    )

                emit({"type": "stage", "name": "generating"})
                sql = text_to_sql(
                    effective_prompt,
                    selected_schema,
                    reasoning_plan=reasoning_plan,
                )
                sql = normalize_and_validate_sql(sql)
                emit({"type": "stage", "name": "sql_generated", "sql": sql, "attempt": 0})

                last_error_message = ""
                for attempt in range(settings.text_to_sql_max_correction_retries + 1):
                    try:
                        total_count = execute_count(engine=engine, base_sql=sql)
                        out_columns, out_rows = execute_sql(
                            engine=engine,
                            sql=sql,
                            max_rows=settings.max_result_rows,
                        )
                        break
                    except Exception as exc:
                        last_error_message = str(exc)
                        if attempt >= settings.text_to_sql_max_correction_retries:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Could not generate executable SQL. Last error: {last_error_message}",
                            )
                        emit(
                            {
                                "type": "stage",
                                "name": "correcting",
                                "attempt": attempt + 1,
                                "invalid_sql": sql,
                                "error": last_error_message,
                            }
                        )
                        sql = correct_sql(
                            user_prompt=effective_prompt,
                            schema=selected_schema,
                            invalid_sql=sql,
                            error_message=last_error_message,
                            reasoning_plan=reasoning_plan,
                        )
                        sql = normalize_and_validate_sql(sql)
                        emit(
                            {
                                "type": "stage",
                                "name": "sql_generated",
                                "sql": sql,
                                "attempt": attempt + 1,
                            }
                        )

                emit(
                    {
                        "type": "stage",
                        "name": "query_executed",
                        "sql": sql,
                        "columns": out_columns,
                        "row_count": len(out_rows),
                        "total_count": total_count,
                        "preview_rows": out_rows[:page_size],
                        "page": 1,
                        "page_size": page_size,
                    }
                )

            chart_prompt = user_prompt or f"Visualize this query result: {sql[:300]}"
            chart_intent = suggest_chart_intent(
                user_prompt=chart_prompt,
                sql=sql,
                columns=out_columns,
            )
            if prompt_mentions_chart and not chart_intent.get("make_chart"):
                chart_intent = _fallback_chart_intent_for_explicit_request(
                    prompt=user_prompt,
                    columns=out_columns,
                    rows=out_rows,
                )
            emit({"type": "stage", "name": "chart_intent_ready", "chart_intent": chart_intent})
            if chart_intent.get("make_chart"):
                fig = build_plotly_figure(intent=chart_intent, columns=out_columns, rows=out_rows)
                emit({"type": "stage", "name": "chart_ready", "chart_config": fig, "plotly": fig})

            assistant_text = build_assistant_text(
                prompt=user_prompt,
                columns=out_columns,
                rows=out_rows,
                chart_intent=chart_intent,
                total_count=total_count,
            )
            emit({"type": "stage", "name": "assistant_ready", "assistant_text": assistant_text})
            response_blocks = build_response_blocks(
                prompt=user_prompt,
                sql=sql,
                columns=out_columns,
                rows=out_rows,
                chart_intent=chart_intent,
                plotly=fig,
                total_count=total_count,
                page=1,
                page_size=page_size,
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
                        "series_field": chart_intent.get("series"),
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
            total_count=total_count,
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
            total_count=total_count,
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
            total_count=total_count,
        )
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "conversation_id": conversation_id,
        "turn_id": turn_id,
        "prompt": user_prompt,
        "intent_type": "DATA_QUERY",
        "sql": sql if not chart_only_intent else None,
        "columns": (out_columns or []) if not chart_only_intent else [],
        "data": (out_rows[:page_size] if out_rows else []) if not chart_only_intent else [],
        "total_count": total_count,
        "chart_intent": chart_intent or {"make_chart": False},
        "plotly": fig,
        "chart_config": fig,
        "assistant_text": assistant_text,
        "response_blocks": response_blocks or [],
        "status": "success",
        "source": response_source,
        "created_at": datetime.utcnow().isoformat(),
    }


@router.post("/analyze/")
async def analyze(
    prompt: str = Form(...),
    conversation_id: Optional[str] = Form(None),
) -> Dict[str, Any]:
    return _analyze_core(prompt=prompt, conversation_id=conversation_id)


@router.post("/analyze/stream")
async def analyze_stream(
    prompt: str = Form(...),
    conversation_id: Optional[str] = Form(None),
) -> StreamingResponse:
    def generate() -> Any:
        event_queue: queue.Queue[bytes] = queue.Queue()
        done = threading.Event()

        def emit(payload: Dict[str, Any]) -> None:
            event_queue.put(_ndjson_line(payload))

        def worker() -> None:
            try:
                final_payload = _analyze_core(prompt=prompt, conversation_id=conversation_id, emit_event=emit)
                event_queue.put(_ndjson_line({"type": "final", "data": final_payload}))
            except HTTPException as exc:
                event_queue.put(
                    _ndjson_line(
                        {
                            "type": "error",
                            "status_code": exc.status_code,
                            "detail": exc.detail,
                        }
                    )
                )
            except Exception as exc:
                event_queue.put(
                    _ndjson_line(
                        {
                            "type": "error",
                            "status_code": 500,
                            "detail": str(exc),
                        }
                    )
                )
            finally:
                done.set()

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()

        while not done.is_set() or not event_queue.empty():
            try:
                yield event_queue.get(timeout=0.1)
            except queue.Empty:
                continue

    return StreamingResponse(
        generate(),
        media_type="application/x-ndjson",
        headers={"Cache-Control": "no-cache"},
    )
