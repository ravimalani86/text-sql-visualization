from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from app.core.config import get_settings
from app.db.engine import engine
from app.repositories.charts_repo import (
    delete_pinned_chart,
    get_pinned_chart,
    init_charts_table,
    list_pinned_charts,
    pin_chart,
    update_pinned_chart_layout,
)
from app.schemas.charts import ChartLayoutRequest, ChartPinRequest
from app.services.chart_intent_ai import suggest_chart_intent
from app.services.plotly_mapper import build_plotly_figure
from app.services.sql_runtime import execute_sql, normalize_and_validate_sql


router = APIRouter(tags=["charts"])


@router.get("/api/charts")
async def api_list_charts() -> Dict[str, Any]:
    return {"items": list_pinned_charts(engine)}


@router.post("/api/charts/pin")
async def api_pin_chart(payload: ChartPinRequest) -> Dict[str, Any]:
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


@router.post("/api/charts/{chart_id}/refresh")
async def api_refresh_chart(chart_id: str) -> Dict[str, Any]:
    settings = get_settings()
    chart = get_pinned_chart(engine, chart_id)
    if not chart:
        raise HTTPException(status_code=404, detail="Chart not found")

    sql = normalize_and_validate_sql(chart.get("sql_query") or "")
    out_columns, out_rows = execute_sql(engine=engine, sql=sql, max_rows=settings.max_result_rows)

    chart_intent: Dict[str, Any] = {"make_chart": True, "chart_type": (chart.get("chart_type") or "bar")}
    if chart.get("x_field") in out_columns:
        chart_intent["x"] = chart.get("x_field")
    if chart.get("y_field") in out_columns:
        chart_intent["y"] = chart.get("y_field")

    # If x/y are missing but columns exist, let the LLM help pick meaningful roles.
    if out_columns and ("x" not in chart_intent or "y" not in chart_intent):
        refined = suggest_chart_intent(
            user_prompt="Refresh pinned chart",
            sql=sql,
            columns=out_columns,
        )
        if refined:
            chart_intent.update(refined)

    fig = build_plotly_figure(intent=chart_intent, columns=out_columns, rows=out_rows) if out_rows else None
    return {
        "item": chart,
        "sql": sql,
        "columns": out_columns,
        "data": out_rows,
        "plotly": fig,
        "chart_intent": chart_intent,
    }


@router.delete("/api/charts/{chart_id}")
async def api_delete_chart(chart_id: str) -> Dict[str, Any]:
    deleted = delete_pinned_chart(engine, chart_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Chart not found")
    return {"status": "ok", "id": chart_id}


@router.patch("/api/charts/{chart_id}/layout")
async def api_update_chart_layout(chart_id: str, payload: ChartLayoutRequest) -> Dict[str, Any]:
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

