from __future__ import annotations

from typing import Any

from app.services.chart_intent_ai import suggest_chart_intent
from app.services.chartjs_mapper import build_chart_config


def run(payload: dict[str, Any]) -> dict[str, Any]:
    user_prompt = str(payload.get("user_prompt") or "")
    sql = str(payload.get("sql") or "")
    columns = payload.get("columns") or []
    rows = payload.get("rows") or []
    force_chart = bool(payload.get("force_chart") or False)

    chart_intent = suggest_chart_intent(
        user_prompt=user_prompt or f"Visualize this query result: {sql[:300]}",
        sql=sql,
        columns=columns,
    )
    if force_chart and not chart_intent.get("make_chart"):
        chart_intent = {"make_chart": True, "chart_type": "bar"}

    fig = build_chart_config(intent=chart_intent, columns=columns, rows=rows) if chart_intent.get("make_chart") else None
    return {
        "chart_intent": chart_intent,
        "fig": fig,
    }

