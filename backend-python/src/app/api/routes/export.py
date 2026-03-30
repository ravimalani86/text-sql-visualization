from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Any, Literal

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.db.engine import engine
from app.repositories.history_repo import get_turn_by_id
from app.services.sql_runtime import execute_sql

router = APIRouter(tags=["export"])


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


@router.get("/api/export")
async def export_turn_table(
    turn_id: str = Query(..., description="Conversation turn id that contains SQL"),
    format: Literal["csv", "xlsx", "pdf"] = Query("csv"),
    max_rows: int = Query(5000, ge=1, le=50000),
) -> StreamingResponse:
    turn = get_turn_by_id(engine, turn_id)
    if not turn or not turn.get("sql"):
        raise HTTPException(status_code=404, detail="Turn not found or has no SQL")

    columns, rows = execute_sql(engine=engine, sql=turn["sql"], max_rows=max_rows)
    df = pd.DataFrame(rows, columns=columns)

    safe_base = f"export-{_now_stamp()}-{turn_id[:8]}"

    if format == "csv":
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        data = io.BytesIO(buf.getvalue().encode("utf-8"))
        headers = {"Content-Disposition": f'attachment; filename="{safe_base}.csv"'}
        return StreamingResponse(data, media_type="text/csv; charset=utf-8", headers=headers)

    if format == "xlsx":
        data = io.BytesIO()
        with pd.ExcelWriter(data, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Export")
        data.seek(0)
        headers = {"Content-Disposition": f'attachment; filename="{safe_base}.xlsx"'}
        return StreamingResponse(
            data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    # pdf
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter, landscape
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except Exception as e:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"PDF export dependency missing: {e}")

    pdf_bytes = io.BytesIO()
    doc = SimpleDocTemplate(
        pdf_bytes,
        pagesize=landscape(letter),
        leftMargin=18,
        rightMargin=18,
        topMargin=18,
        bottomMargin=18,
        title="Export",
    )

    styles = getSampleStyleSheet()
    story: list[Any] = []
    story.append(Paragraph("Table Export", styles["Title"]))
    story.append(Paragraph(f"Turn: {turn_id}", styles["Normal"]))
    story.append(Spacer(1, 10))

    max_cols = 25
    cols = columns[:max_cols]
    data_rows = []
    for r in rows:
        row = [r.get(c, "") for c in cols]
        data_rows.append([("" if v is None else str(v))[:200] for v in row])

    table_data = [cols] + data_rows
    t = Table(table_data, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#efeaff")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#2f275a")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f7ff")]),
            ]
        )
    )
    story.append(t)

    doc.build(story)
    pdf_bytes.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="{safe_base}.pdf"'}
    return StreamingResponse(pdf_bytes, media_type="application/pdf", headers=headers)

