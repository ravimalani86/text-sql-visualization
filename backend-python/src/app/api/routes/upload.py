from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Query, UploadFile
from pydantic import BaseModel
from sqlalchemy import inspect, text

from app.db.engine import engine
from app.services.csv_ingestion import load_csv_to_db


router = APIRouter(tags=["upload"])
MANAGED_APP_TABLES = {}


class DeleteTableRecordRequest(BaseModel):
    table_name: str
    row_id: str


class DeleteTableRequest(BaseModel):
    table_name: str


def _get_editable_tables() -> list[str]:
    inspector = inspect(engine)
    all_tables = inspector.get_table_names()
    return [table for table in all_tables if table not in MANAGED_APP_TABLES]


@router.post("/upload-csv/")
async def upload_csv(file: UploadFile) -> Dict[str, Any]:
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    table_name = file.filename.replace(".csv", "").lower()
    load_csv_to_db(file, table_name, engine)
    return {"status": "success", "table": table_name}


@router.get("/api/table-browser/tables")
async def list_browser_tables() -> Dict[str, Any]:
    tables = _get_editable_tables()
    items = []

    with engine.connect() as conn:
        for table in tables:
            safe_table = table.replace('"', '""')
            row_count = conn.execute(text(f'SELECT COUNT(*) FROM "{safe_table}"')).scalar() or 0
            items.append({"table": table, "row_count": int(row_count)})

    return {"tables": items, "count": len(items)}


@router.get("/api/table-browser/rows")
async def get_table_rows_for_browser(
    table_name: str = Query(..., description="Table name"),
) -> Dict[str, Any]:
    tables = _get_editable_tables()
    if table_name not in tables:
        raise HTTPException(status_code=404, detail="Table not found")

    safe_table = table_name.replace('"', '""')
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT ctid::text AS __row_id, * FROM "{safe_table}" ORDER BY ctid'))
        all_columns = list(result.keys())
        columns = [c for c in all_columns if c != "__row_id"]
        rows = [dict(row) for row in result.mappings().all()]

    return {
        "table": table_name,
        "columns": columns,
        "count": len(rows),
        "records": rows,
    }


@router.delete("/api/table-browser/record")
async def delete_table_record(payload: DeleteTableRecordRequest) -> Dict[str, Any]:
    table_name = payload.table_name.strip()
    row_id = payload.row_id.strip()
    if not table_name or not row_id:
        raise HTTPException(status_code=400, detail="table_name and row_id are required")

    tables = _get_editable_tables()
    if table_name not in tables:
        raise HTTPException(status_code=404, detail="Table not found")

    safe_table = table_name.replace('"', '""')
    with engine.begin() as conn:
        deleted = conn.execute(
            text(f'DELETE FROM "{safe_table}" WHERE ctid = CAST(:row_id AS tid)'),
            {"row_id": row_id},
        )

    if (deleted.rowcount or 0) == 0:
        raise HTTPException(status_code=404, detail="Record not found")

    return {"status": "success", "table": table_name, "deleted": int(deleted.rowcount or 0)}


@router.delete("/api/table-browser/table")
async def delete_table(payload: DeleteTableRequest) -> Dict[str, Any]:
    table_name = payload.table_name.strip()
    if not table_name:
        raise HTTPException(status_code=400, detail="table_name is required")

    tables = _get_editable_tables()
    if table_name not in tables:
        raise HTTPException(status_code=404, detail="Table not found")

    safe_table = table_name.replace('"', '""')
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE "{safe_table}"'))

    return {"status": "success", "table": table_name}


@router.get("/clear-all-tables/")
async def clear_all_tables() -> Dict[str, Any]:
    tables = _get_editable_tables()

    if not tables:
        return {"status": "ok", "message": "No tables found", "dropped_tables": [], "count": 0}

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

