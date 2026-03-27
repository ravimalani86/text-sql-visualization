from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core.config import DEFAULT_PAGE_SIZE
from app.db.engine import engine
from app.repositories.pinned_tables_repo import (
    delete_pinned_table,
    get_pinned_table,
    list_pinned_tables,
    pin_table,
    update_pinned_table_layout,
)
from app.schemas.tables import TableLayoutRequest, TablePinRequest
from app.services.sql_runtime import (
    execute_count,
    execute_paginated,
    make_pagination_meta,
    normalize_and_validate_sql,
)

router = APIRouter(tags=["pinned-tables"])


class FilterItem(BaseModel):
    column: str
    operator: str
    value: Any = None


class PinnedTableDataRequest(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=DEFAULT_PAGE_SIZE, ge=1, le=500)
    sort_column: Optional[str] = None
    sort_direction: str = Field(default="asc", pattern="^(asc|desc)$")
    search: Optional[str] = None
    filters: List[FilterItem] = Field(default_factory=list)


@router.get("/api/tables")
async def api_list_tables() -> Dict[str, Any]:
    return {"items": list_pinned_tables(engine)}


@router.post("/api/tables/pin")
async def api_pin_table(payload: TablePinRequest) -> Dict[str, Any]:
    sql = normalize_and_validate_sql(payload.sql)
    title = (payload.title or "").strip() or "Pinned table"

    saved = pin_table(
        engine,
        title=title,
        sql_query=sql,
        columns=payload.columns,
    )
    return {"item": saved}


@router.post("/api/tables/{table_id}/refresh")
async def api_refresh_table(table_id: str) -> Dict[str, Any]:
    item = get_pinned_table(engine, table_id)
    if not item:
        raise HTTPException(status_code=404, detail="Pinned table not found")

    base_sql = item["sql_query"]
    stored_columns = item.get("columns")
    valid_columns: list[str] | None = None
    if isinstance(stored_columns, list) and stored_columns:
        valid_columns = [str(c) for c in stored_columns]

    total_count = execute_count(engine=engine, base_sql=base_sql, valid_columns=valid_columns)
    columns, rows = execute_paginated(
        engine=engine,
        base_sql=base_sql,
        page=1,
        page_size=DEFAULT_PAGE_SIZE,
        valid_columns=valid_columns,
    )

    meta = make_pagination_meta(
        total_count=total_count,
        page=1,
        page_size=DEFAULT_PAGE_SIZE,
        shown_rows=len(rows),
    )

    return {
        "item": item,
        "columns": columns,
        "rows": rows,
        "meta": meta,
    }


@router.post("/api/tables/{table_id}/data")
async def api_table_data(table_id: str, req: PinnedTableDataRequest) -> Dict[str, Any]:
    item = get_pinned_table(engine, table_id)
    if not item:
        raise HTTPException(status_code=404, detail="Pinned table not found")

    base_sql = item["sql_query"]
    stored_columns = item.get("columns")
    valid_columns: list[str] | None = None
    if isinstance(stored_columns, list) and stored_columns:
        valid_columns = [str(c) for c in stored_columns]

    raw_filters = [f.model_dump() for f in req.filters]

    total_count = execute_count(
        engine=engine,
        base_sql=base_sql,
        filters=raw_filters or None,
        valid_columns=valid_columns,
        search=req.search,
    )

    columns, rows = execute_paginated(
        engine=engine,
        base_sql=base_sql,
        page=req.page,
        page_size=req.page_size,
        sort_column=req.sort_column,
        sort_direction=req.sort_direction,
        filters=raw_filters or None,
        valid_columns=valid_columns,
        search=req.search,
    )

    meta = make_pagination_meta(
        total_count=total_count,
        page=req.page,
        page_size=req.page_size,
        shown_rows=len(rows),
    )

    return {
        "columns": columns,
        "rows": rows,
        "meta": meta,
    }


@router.delete("/api/tables/{table_id}")
async def api_delete_table(table_id: str) -> Dict[str, Any]:
    deleted = delete_pinned_table(engine, table_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Pinned table not found")
    return {"status": "ok", "id": table_id}


@router.patch("/api/tables/{table_id}/layout")
async def api_update_table_layout(table_id: str, payload: TableLayoutRequest) -> Dict[str, Any]:
    updated = update_pinned_table_layout(
        engine,
        table_id=table_id,
        sort_order=payload.sort_order,
        width_units=payload.width_units,
        height_px=payload.height_px,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Pinned table not found")
    return {"item": updated}
