from __future__ import annotations

import math
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


# ---------------------------------------------------------------------------
# Pagination helpers
# ---------------------------------------------------------------------------

_VALID_FILTER_OPS = frozenset(
    {
        "eq",
        "neq",
        "contains",
        "starts_with",
        "gt",
        "gte",
        "lt",
        "lte",
        "between",
        "in",
        "is_null",
        "is_not_null",
    }
)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def build_filter_clause(
    filters: list[dict[str, Any]],
    valid_columns: list[str] | None = None,
) -> tuple[str, dict[str, Any]]:
    if not filters:
        return "", {}

    conditions: list[str] = []
    params: dict[str, Any] = {}

    for i, f in enumerate(filters):
        col = str(f.get("column", ""))
        op = str(f.get("operator", ""))
        val = f.get("value")

        if not col or op not in _VALID_FILTER_OPS:
            continue
        if valid_columns and col not in valid_columns:
            continue

        p = f"_f{i}"
        qcol = _quote_ident(col)

        if op == "eq":
            conditions.append(f"{qcol} = :{p}")
            params[p] = val
        elif op == "neq":
            conditions.append(f"{qcol} != :{p}")
            params[p] = val
        elif op == "contains":
            conditions.append(f"CAST({qcol} AS TEXT) ILIKE :{p}")
            params[p] = f"%{val}%"
        elif op == "starts_with":
            conditions.append(f"CAST({qcol} AS TEXT) ILIKE :{p}")
            params[p] = f"{val}%"
        elif op == "gt":
            conditions.append(f"{qcol} > :{p}")
            params[p] = val
        elif op == "gte":
            conditions.append(f"{qcol} >= :{p}")
            params[p] = val
        elif op == "lt":
            conditions.append(f"{qcol} < :{p}")
            params[p] = val
        elif op == "lte":
            conditions.append(f"{qcol} <= :{p}")
            params[p] = val
        elif op == "between":
            if isinstance(val, (list, tuple)) and len(val) >= 2:
                conditions.append(f"{qcol} BETWEEN :{p}_lo AND :{p}_hi")
                params[f"{p}_lo"] = val[0]
                params[f"{p}_hi"] = val[1]
        elif op == "in":
            if isinstance(val, (list, tuple)) and val:
                phs = ", ".join(f":{p}_{j}" for j in range(len(val)))
                conditions.append(f"{qcol} IN ({phs})")
                for j, v in enumerate(val):
                    params[f"{p}_{j}"] = v
        elif op == "is_null":
            conditions.append(f"{qcol} IS NULL")
        elif op == "is_not_null":
            conditions.append(f"{qcol} IS NOT NULL")

    if not conditions:
        return "", {}
    return "WHERE " + " AND ".join(conditions), params


def _build_search_clause(
    search: str,
    valid_columns: list[str] | None = None,
) -> tuple[str, dict[str, Any]]:
    term = (search or "").strip()
    if not term or not valid_columns:
        return "", {}
    or_parts = [
        f"CAST({_quote_ident(col)} AS TEXT) ILIKE :_search"
        for col in valid_columns
    ]
    return "(" + " OR ".join(or_parts) + ")", {"_search": f"%{term}%"}


def _combine_where(
    filter_clause: str,
    filter_params: dict[str, Any],
    search_clause: str,
    search_params: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    parts: list[str] = []
    params: dict[str, Any] = {}
    if filter_clause:
        parts.append(filter_clause.removeprefix("WHERE "))
        params.update(filter_params)
    if search_clause:
        parts.append(search_clause)
        params.update(search_params)
    if not parts:
        return "", {}
    return "WHERE " + " AND ".join(parts), params


def execute_count(
    *,
    engine: Engine,
    base_sql: str,
    filters: list[dict[str, Any]] | None = None,
    valid_columns: list[str] | None = None,
    search: str | None = None,
) -> int:
    filter_clause, filter_params = build_filter_clause(filters or [], valid_columns)
    search_clause, search_params = _build_search_clause(search, valid_columns)
    where, params = _combine_where(filter_clause, filter_params, search_clause, search_params)
    wrapped = f"SELECT COUNT(*) FROM ({base_sql}) _t {where}"
    with engine.connect() as conn:
        return conn.execute(text(wrapped), params).scalar() or 0


def execute_paginated(
    *,
    engine: Engine,
    base_sql: str,
    page: int = 1,
    page_size: int = 10,
    sort_column: str | None = None,
    sort_direction: str = "asc",
    filters: list[dict[str, Any]] | None = None,
    valid_columns: list[str] | None = None,
    search: str | None = None,
) -> tuple[list[str], list[dict[str, Any]]]:
    filter_clause, filter_params = build_filter_clause(filters or [], valid_columns)
    search_clause, search_params = _build_search_clause(search, valid_columns)
    where, params = _combine_where(filter_clause, filter_params, search_clause, search_params)

    order_clause = ""
    if sort_column:
        if not valid_columns or sort_column in valid_columns:
            direction = "DESC" if sort_direction.lower() == "desc" else "ASC"
            order_clause = f"ORDER BY {_quote_ident(sort_column)} {direction}"

    offset = (page - 1) * page_size
    params["_page_size"] = page_size
    params["_offset"] = offset

    wrapped = (
        f"SELECT * FROM ({base_sql}) _t "
        f"{where} {order_clause} "
        f"LIMIT :_page_size OFFSET :_offset"
    )

    with engine.connect() as conn:
        result = conn.execute(text(wrapped), params)
        columns = list(result.keys())
        rows = [dict(row._mapping) for row in result]
    return columns, rows


def make_pagination_meta(
    *,
    total_count: int,
    page: int,
    page_size: int,
    shown_rows: int,
) -> dict[str, Any]:
    return {
        "total_count": total_count,
        "page": page,
        "page_size": page_size,
        "total_pages": math.ceil(total_count / page_size) if page_size else 0,
        "shown_rows": shown_rows,
        "row_count": total_count,
    }
