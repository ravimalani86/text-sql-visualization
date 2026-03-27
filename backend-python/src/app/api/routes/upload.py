from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Query, UploadFile
from sqlalchemy import inspect, text

from app.db.engine import engine
from app.services.csv_ingestion import load_csv_to_db


router = APIRouter(tags=["upload"])


@router.post("/upload-csv/")
async def upload_csv(file: UploadFile) -> Dict[str, Any]:
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    table_name = file.filename.replace(".csv", "").lower()
    load_csv_to_db(file, table_name, engine)
    return {"status": "success", "table": table_name}


@router.get("/tables/")
async def list_tables() -> Dict[str, Any]:
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    items = []

    with engine.connect() as conn:
        for table in tables:
            safe_table = table.replace('"', '""')
            row_count = conn.execute(text(f'SELECT COUNT(*) FROM "{safe_table}"')).scalar() or 0
            items.append({"table": table, "row_count": int(row_count)})

    return {"tables": items, "count": len(items)}


@router.get("/table-data/")
async def get_table_data(
    table_name: str = Query(..., description="Table name"),
    limit: int = Query(100, description="Number of records")
) -> Dict[str, Any]:

    inspector = inspect(engine)
    tables = inspector.get_table_names()

    if table_name not in tables:
        raise HTTPException(status_code=404, detail="Table not found")

    safe_table = table_name.replace('"', '""')

    with engine.connect() as conn:
        result = conn.execute(
            text(f'SELECT * FROM "{safe_table}" LIMIT :limit'),
            {"limit": limit}
        )
        rows = [dict(row._mapping) for row in result]

    return {
        "table": table_name,
        "count": len(rows),
        "records": rows
    }


@router.get("/clear-all-tables/")
async def clear_all_tables() -> Dict[str, Any]:
    inspector = inspect(engine)
    tables = inspector.get_table_names()

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

