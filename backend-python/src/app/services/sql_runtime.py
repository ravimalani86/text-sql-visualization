from __future__ import annotations

from typing import Any

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.engine import Engine


def normalize_and_validate_sql(sql: str) -> str:
    if not sql or not sql.strip():
        raise HTTPException(status_code=400, detail="Empty SQL from SQL generator")
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


def execute_sql(
    *,
    engine: Engine,
    sql: str,
    max_rows: int,
) -> tuple[list[str], list[dict[str, Any]]]:
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        columns = list(result.keys())
        rows: list[dict[str, Any]] = []
        for i, row in enumerate(result):
            if i >= max_rows:
                break
            rows.append(dict(row._mapping))
    return columns, rows

