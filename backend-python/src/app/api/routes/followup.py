from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Literal, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core.config import DEFAULT_PAGE_SIZE, get_settings
from app.db.engine import engine
from app.repositories.charts_repo import get_chart
from app.repositories.history_repo import create_conversation, save_turn
from app.repositories.tables_repo import get_table
from app.services.sql_runtime import execute_count, execute_sql, normalize_and_validate_sql

router = APIRouter(tags=["followup"])


class FollowupSeedRequest(BaseModel):
    item_type: Literal["chart", "table"] = Field(alias="type")
    id: str
    page_size: int = Field(default=DEFAULT_PAGE_SIZE, ge=1, le=200)


@router.post("/api/followup/seed")
async def seed_followup(payload: FollowupSeedRequest) -> Dict[str, Any]:
    settings = get_settings()
    item_type = payload.item_type
    item_id = (payload.id or "").strip()
    if not item_id:
        raise HTTPException(status_code=400, detail="id is required")

    title: str = "Follow-up"
    sql: str
    out_columns: list[str]
    out_rows: list[dict[str, Any]]
    total_count: Optional[int] = None
    chart_intent: dict[str, Any] = {"make_chart": False}
    plotly: Optional[dict[str, Any]] = None

    if item_type == "chart":
        chart = get_chart(engine, item_id)
        if not chart:
            raise HTTPException(status_code=404, detail="Pinned chart not found")
        title = (chart.get("title") or "").strip() or "Pinned chart"
        sql = normalize_and_validate_sql(str(chart.get("sql_query") or ""))
        total_count = execute_count(engine=engine, base_sql=sql)
        out_columns, out_rows = execute_sql(engine=engine, sql=sql, max_rows=settings.max_result_rows)

        chart_intent = {"make_chart": True, "chart_type": (chart.get("chart_type") or "bar")}
        if chart.get("x_field") in out_columns:
            chart_intent["x"] = chart.get("x_field")
        if chart.get("y_field") in out_columns:
            chart_intent["y"] = chart.get("y_field")
        if chart.get("series_field") in out_columns:
            chart_intent["series"] = chart.get("series_field")
        # Keep plotly out of the seeded follow-up turn to avoid UI auto-rendering charts.
        plotly = None

    else:
        tbl = get_table(engine, item_id)
        if not tbl:
            raise HTTPException(status_code=404, detail="Pinned table not found")
        title = (tbl.get("title") or "").strip() or "Pinned table"
        sql = normalize_and_validate_sql(str(tbl.get("sql_query") or ""))
        total_count = execute_count(engine=engine, base_sql=sql)
        out_columns, out_rows = execute_sql(engine=engine, sql=sql, max_rows=settings.max_result_rows)
        chart_intent = {"make_chart": False}
        plotly = None

    # Follow-up UI should render only a lightweight banner message.
    # We still save SQL/data on the turn so the next user prompt has context.
    seed_prompt = f"Follow up on pinned {item_type}"
    label = "chart" if item_type == "chart" else "table"
    response_blocks = [
        {
            "type": "text",
            "content": f"↩ Follow-up started for pinned {label}: “{title}”.\n\nAsk your next question in the box below.",
        }
    ]

    conversation_id = create_conversation(engine, title=title[:120])
    turn_id = save_turn(
        engine,
        conversation_id=conversation_id,
        prompt=seed_prompt,
        context_prompt=None,
        sql=sql,
        columns=out_columns,
        data=out_rows,
        chart_intent=chart_intent,
        plotly=None,
        assistant_text=None,
        response_blocks=response_blocks,
        status="success",
        error=None,
        total_count=total_count,
    )

    return {
        "conversation_id": conversation_id,
        "turn": {
            "turn_id": turn_id,
            "prompt": seed_prompt,
            "sql": sql,
            "columns": out_columns,
            "data": out_rows[: payload.page_size],
            "total_count": total_count,
            "chart_intent": chart_intent,
            "plotly": None,
            "chart_config": None,
            "assistant_text": None,
            "response_blocks": response_blocks,
            "status": "success",
            "created_at": datetime.utcnow().isoformat(),
            "title": title,
            "item_type": item_type,
            "item_id": item_id,
        },
    }
