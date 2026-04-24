from __future__ import annotations

from typing import Any

from app.core.config import get_settings
from app.mcp.shared.db import get_engine
from app.services.sql_runtime import execute_count, execute_sql


def run(payload: dict[str, Any]) -> dict[str, Any]:
    sql = str(payload.get("sql") or "")
    max_rows = int(payload.get("max_rows") or get_settings().max_result_rows)
    engine = get_engine()
    total_count = execute_count(engine=engine, base_sql=sql)
    columns, rows = execute_sql(engine=engine, sql=sql, max_rows=max_rows)
    return {
        "columns": columns,
        "rows": rows,
        "total_count": total_count,
    }

