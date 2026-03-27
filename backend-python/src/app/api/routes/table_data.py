from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core.config import DEFAULT_PAGE_SIZE
from app.db.engine import engine
from app.repositories.history_repo import get_turn_by_id
from app.services.sql_runtime import (
    execute_count,
    execute_paginated,
    make_pagination_meta,
)

router = APIRouter(tags=["table-data"])


class FilterItem(BaseModel):
    column: str
    operator: str
    value: Any = None


class TableDataRequest(BaseModel):
    turn_id: str
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=DEFAULT_PAGE_SIZE, ge=1, le=500)
    sort_column: Optional[str] = None
    sort_direction: str = Field(default="asc", pattern="^(asc|desc)$")
    search: Optional[str] = None
    filters: List[FilterItem] = Field(default_factory=list)


@router.post("/api/table-data")
async def get_table_data(req: TableDataRequest) -> Dict[str, Any]:
    turn = get_turn_by_id(engine, req.turn_id)
    if not turn:
        raise HTTPException(status_code=404, detail="Turn not found or has no SQL")

    base_sql = turn["sql"]
    stored_columns = turn.get("columns")
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
        "applied_sort": {
            "column": req.sort_column,
            "direction": req.sort_direction,
        }
        if req.sort_column
        else None,
        "applied_filters": raw_filters if raw_filters else [],
    }
