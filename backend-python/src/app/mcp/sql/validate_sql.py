from __future__ import annotations

from typing import Any

from app.services.sql_runtime import normalize_and_validate_sql


def run(payload: dict[str, Any]) -> dict[str, Any]:
    sql = str(payload.get("sql") or "")
    return {"sql": normalize_and_validate_sql(sql)}

