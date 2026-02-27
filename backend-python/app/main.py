# backend/app/main.py

from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import FastAPI, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import text, inspect

from app.database import engine
from app.csv_loader import load_csv_to_db
from app.sql_generator import text_to_sql
from app.schema_utils import get_db_schema
from app.chart_intent_ai import suggest_chart_intent
from app.plotly_mapper import build_plotly_figure
from app.response_builder import build_response_blocks, build_assistant_text
from app.conversation_ai import generate_conversation_reply
from app.history_store import (
    init_history_tables,
    create_conversation,
    conversation_exists,
    save_turn,
    get_latest_success_turn,
    list_conversations as list_saved_conversations,
    get_conversation_with_turns,
    pin_chart,
    list_pinned_charts,
    get_pinned_chart,
    delete_pinned_chart,
    update_pinned_chart_layout,
)


def normalize_and_validate_sql(sql: str) -> str:
    """
    If multiple statements, run only the first one.
    Allow SELECT and WITH (CTE); reject others.
    """
    if not sql or not sql.strip():
        raise HTTPException(status_code=400, detail="Empty SQL")
    # Take only first statement (multiple queries => run only one)
    first = sql.strip().split(";")[0].strip()
    if not first:
        raise HTTPException(status_code=400, detail="No valid SQL statement")
    low = first.lower()
    if not (low.startswith("select") or low.startswith("with")):
        raise HTTPException(
            status_code=400,
            detail=f"Only SELECT (or WITH/CTE) queries are allowed. Got: {first[:80]}...",
        )
    return first


app = FastAPI()


class ChartPinRequest(BaseModel):
    title: str
    sql: str
    chart_type: str
    x_field: Optional[str] = None
    y_field: Optional[str] = None


class ChartLayoutRequest(BaseModel):
    sort_order: Optional[int] = None
    width_units: Optional[int] = None
    height_px: Optional[int] = None

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:3002",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def on_startup():
    init_history_tables(engine)


def _is_chart_only_prompt(prompt: str) -> bool:
    text_low = (prompt or "").strip().lower()
    if not text_low:
        return False

    chart_words = [
        "chart",
        "graph",
        "plot",
        "visual",
        "bar",
        "line",
        "pie",
        "area",
        "scatter",
        "horizontal",
        "stacked",
        "grouped",
    ]
    data_change_words = [
        "total",
        "sum",
        "avg",
        "average",
        "count",
        "max",
        "min",
        "top",
        "bottom",
        "where",
        "filter",
        "between",
        "before",
        "after",
        "2024",
        "2025",
        "2026",
        "month",
        "year",
        "product",
        "category",
        "region",
        "customer",
        "sales",
        "revenue",
        "profit",
        "group by",
        "order by",
    ]

    has_chart_word = any(w in text_low for w in chart_words)
    has_data_change_word = any(w in text_low for w in data_change_words)
    return has_chart_word and not has_data_change_word


def _classify_intent(prompt: str) -> str:
    text_low = (prompt or "").strip().lower()
    if not text_low:
        return "CONVERSATION"

    conversation_phrases = {
        "hi",
        "hello",
        "hey",
        "how are you",
        "thanks",
        "thank you",
        "good morning",
        "good afternoon",
        "good evening",
        "bye",
    }
    if text_low in conversation_phrases:
        return "CONVERSATION"

    data_words = [
        "data",
        "database",
        "sql",
        "query",
        "table",
        "report",
        "analytics",
        "analysis",
        "sales",
        "revenue",
        "profit",
        "chart",
        "graph",
        "plot",
        "top",
        "count",
        "sum",
        "average",
        "avg",
        "total",
        "group by",
        "order by",
        "show me",
        "list",
        "month",
        "year",
        "customer",
        "product",
        "category",
        "region",
    ]
    if any(w in text_low for w in data_words):
        return "DATA_QUERY"

    return "CONVERSATION"


def _looks_incomplete_followup(prompt: str) -> bool:
    text_low = (prompt or "").strip().lower()
    if not text_low:
        return False

    explicit_metric_words = [
        "sales",
        "revenue",
        "profit",
        "amount",
        "quantity",
        "count",
        "sum",
        "avg",
        "average",
        "max",
        "min",
    ]
    explicit_change_words = [
        "where",
        "filter",
        "between",
        "before",
        "after",
        "for ",
        "in ",
        "by ",
        "group by",
        "order by",
    ]

    has_metric = any(w in text_low for w in explicit_metric_words)
    has_explicit_change = any(w in text_low for w in explicit_change_words)
    short_request = len(text_low.split()) <= 8
    return short_request and not has_metric and not has_explicit_change


def _build_effective_prompt(prompt: str, latest_turn: Optional[Dict[str, Any]]) -> str:
    if not latest_turn:
        return prompt

    prev_prompt = (latest_turn.get("prompt") or "").strip()
    prev_sql = (latest_turn.get("sql") or "").strip()
    prev_columns = latest_turn.get("columns") or []
    if not isinstance(prev_columns, list):
        prev_columns = []

    continuation_mode = "incomplete follow-up" if _looks_incomplete_followup(prompt) else "follow-up"
    context_header = (
        "You are continuing an existing analytics conversation.\n"
        f"Continuation mode: {continuation_mode}\n\n"
        "Previous turn context:\n"
        f"- Previous user request: {prev_prompt}\n"
        f"- Previous generated SQL: {prev_sql}\n"
        f"- Previous result columns: {', '.join(str(c) for c in prev_columns)}\n\n"
        "Follow-up SQL rules:\n"
        "1) Treat current request as continuation of previous analysis.\n"
        "2) If current request is incomplete/ambiguous, reuse previous metric, aggregation, grouping, and sorting.\n"
        "3) Only change the parts explicitly requested now (e.g., LIMIT, chart category, time filter).\n"
        "4) Do NOT change metric unless user clearly specifies a new metric.\n"
        "5) Keep table/column references valid for current schema.\n\n"
        f"Current user request:\n{prompt}"
    )
    return context_header


@app.post("/generate-sql/")
async def generate_sql(prompt: str = Form(...)):
    """Generate SQL from prompt only (no execution)."""
    schema = get_db_schema(engine)
    if not schema:
        raise HTTPException(status_code=400, detail="No tables found in database")
    sql = text_to_sql(prompt, schema)
    sql = normalize_and_validate_sql(sql)
    return {"sql": sql}


@app.post("/execute-query/")
async def execute_query(sql: str = Form(...)):
    """Execute SQL and return columns + data (no chart)."""
    sql = normalize_and_validate_sql(sql)
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        columns = list(result.keys())
        rows = [dict(row._mapping) for row in result]
    return {"sql": sql, "columns": columns, "data": rows}


@app.post("/generate-chart/")
async def generate_chart(
    sql: str = Form(...),
    columns: str = Form(...),  # JSON array string
    prompt: str = Form(""),    # optional; when empty, use SQL as context
):
    import json as _json
    try:
        cols = _json.loads(columns)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid columns JSON")
    if not isinstance(cols, list):
        raise HTTPException(status_code=400, detail="columns must be an array")

    chart_prompt = (prompt or "").strip() or f"Visualize this query result: {sql[:300]}"

    sql = normalize_and_validate_sql(sql)

    chart_intent = suggest_chart_intent(user_prompt=chart_prompt, sql=sql, columns=cols)

    with engine.connect() as conn:
        result = conn.execute(text(sql))
        out_columns = list(result.keys())
        out_rows = [dict(row._mapping) for row in result]

    fig = build_plotly_figure(intent=chart_intent, columns=out_columns, rows=out_rows) if chart_intent.get("make_chart") else None

    return {"chart_intent": chart_intent, "plotly": fig}


@app.post("/analyze/")
async def analyze(
    prompt: str = Form(...),
    conversation_id: Optional[str] = Form(None),
):
    user_prompt = (prompt or "").strip()
    if not user_prompt:
        raise HTTPException(status_code=400, detail="Prompt is required")

    if conversation_id and not conversation_exists(engine, conversation_id):
        raise HTTPException(status_code=404, detail="Conversation not found")

    conversation_id = conversation_id or create_conversation(
        engine,
        title=user_prompt[:120],
    )

    latest_turn = get_latest_success_turn(engine, conversation_id)
    effective_prompt = _build_effective_prompt(user_prompt, latest_turn)

    sql: Optional[str] = None
    out_columns: Optional[list[str]] = None
    out_rows: Optional[list[dict[str, Any]]] = None
    chart_intent: Optional[dict[str, Any]] = None
    fig: Optional[dict[str, Any]] = None
    assistant_text: Optional[str] = None
    response_blocks: Optional[list[dict[str, Any]]] = None
    chart_only_intent = False

    try:
        intent_type = _classify_intent(user_prompt)
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
        chart_only_intent = _is_chart_only_prompt(user_prompt) and can_reuse_last_result

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
                    }
                )
        else:
            schema = get_db_schema(engine)
            if not schema:
                raise HTTPException(status_code=400, detail="No tables found in database")

            sql = text_to_sql(effective_prompt, schema)
            sql = normalize_and_validate_sql(sql)

            with engine.connect() as conn:
                result = conn.execute(text(sql))
                out_columns = list(result.keys())
                out_rows = [dict(row._mapping) for row in result]

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


@app.get("/history/conversations")
async def history_conversations():
    return {"items": list_saved_conversations(engine)}


@app.get("/history/{conversation_id}")
async def history_conversation(conversation_id: str):
    payload = get_conversation_with_turns(engine, conversation_id)
    if not payload:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return payload


@app.post("/api/charts/pin")
async def api_pin_chart(payload: ChartPinRequest):
    sql = normalize_and_validate_sql(payload.sql)
    title = (payload.title or "").strip() or "Pinned chart"
    chart_type = (payload.chart_type or "").strip() or "bar"

    saved = pin_chart(
        engine,
        title=title,
        sql_query=sql,
        chart_type=chart_type,
        x_field=(payload.x_field or "").strip() or None,
        y_field=(payload.y_field or "").strip() or None,
    )
    return {"item": saved}


@app.get("/api/charts")
async def api_list_charts():
    return {"items": list_pinned_charts(engine)}


@app.post("/api/charts/{chart_id}/refresh")
async def api_refresh_chart(chart_id: str):
    chart = get_pinned_chart(engine, chart_id)
    if not chart:
        raise HTTPException(status_code=404, detail="Chart not found")

    sql = normalize_and_validate_sql(chart.get("sql_query") or "")
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        out_columns = list(result.keys())
        out_rows = [dict(row._mapping) for row in result]

    chart_intent: Dict[str, Any] = {
        "make_chart": True,
        "chart_type": (chart.get("chart_type") or "bar"),
    }
    if chart.get("x_field") in out_columns:
        chart_intent["x"] = chart.get("x_field")
    if chart.get("y_field") in out_columns:
        chart_intent["y"] = chart.get("y_field")

    fig = build_plotly_figure(intent=chart_intent, columns=out_columns, rows=out_rows)
    return {
        "item": chart,
        "sql": sql,
        "columns": out_columns,
        "data": out_rows,
        "plotly": fig,
        "chart_intent": chart_intent,
    }


@app.delete("/api/charts/{chart_id}")
async def api_delete_chart(chart_id: str):
    deleted = delete_pinned_chart(engine, chart_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Chart not found")
    return {"status": "ok", "id": chart_id}


@app.patch("/api/charts/{chart_id}/layout")
async def api_update_chart_layout(chart_id: str, payload: ChartLayoutRequest):
    updated = update_pinned_chart_layout(
        engine,
        chart_id=chart_id,
        sort_order=payload.sort_order,
        width_units=payload.width_units,
        height_px=payload.height_px,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Chart not found")
    return {"item": updated}


@app.post("/upload-csv/")
async def upload_csv(file: UploadFile):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    table_name = file.filename.replace(".csv", "").lower()
    load_csv_to_db(file, table_name, engine)

    return {"status": "success", "table": table_name}


@app.get("/tables/")
async def list_tables():
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    items = []

    with engine.connect() as conn:
        for table in tables:
            safe_table = table.replace('"', '""')
            row_count = conn.execute(
                text(f'SELECT COUNT(*) FROM "{safe_table}"')
            ).scalar() or 0
            items.append({"table": table, "row_count": int(row_count)})

    return {"tables": items, "count": len(items)}


@app.get("/clear-all-tables/")
async def clear_all_tables():
    """
    Drop all tables from the current database schema.
    """
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    if not tables:
        return {"status": "ok", "message": "No tables found", "dropped_tables": [], "count": 0}

    # Quote table names safely for SQL execution.
    safe_tables = [t.replace('"', '""') for t in tables]

    with engine.connect() as conn:
        for t in safe_tables:
            conn.execute(text(f'DROP TABLE IF EXISTS "{t}" CASCADE'))
        conn.commit()

    return {
        "status": "ok",
        "message": "All tables dropped",
        "dropped_tables": tables,
        "count": len(tables),
    }
