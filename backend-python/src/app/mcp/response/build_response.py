from __future__ import annotations

from typing import Any

from app.core.config import DEFAULT_PAGE_SIZE
from app.services.response_builder import build_assistant_text, build_response_blocks


def run(payload: dict[str, Any]) -> dict[str, Any]:
    prompt = str(payload.get("prompt") or "")
    sql = str(payload.get("sql") or "")
    columns = payload.get("columns") or []
    rows = payload.get("rows") or []
    chart_intent = payload.get("chart_intent") or {"make_chart": False}
    fig = payload.get("fig")
    total_count = payload.get("total_count")
    page_size = int(payload.get("page_size") or DEFAULT_PAGE_SIZE)

    assistant_text = build_assistant_text(
        prompt=prompt,
        columns=columns,
        rows=rows,
        chart_intent=chart_intent,
        total_count=total_count,
    )
    response_blocks = build_response_blocks(
        prompt=prompt,
        sql=sql,
        columns=columns,
        rows=rows,
        chart_intent=chart_intent,
        chart_config=fig,
        total_count=total_count,
        page=1,
        page_size=page_size,
    )
    if fig:
        response_blocks.append(
            {
                "type": "pin_action",
                "title": prompt[:120] or "Pinned chart",
                "sql": sql,
                "chart_type": chart_intent.get("chart_type"),
                "x_field": chart_intent.get("x"),
                "y_field": chart_intent.get("y"),
                "series_field": chart_intent.get("series"),
            }
        )
    return {
        "assistant_text": assistant_text,
        "response_blocks": response_blocks,
    }

