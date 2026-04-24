from __future__ import annotations

from typing import Any

from app.services.sql_generator import correct_sql


def run(payload: dict[str, Any]) -> dict[str, Any]:
    sql = correct_sql(
        user_prompt=str(payload.get("user_prompt") or ""),
        schema=payload.get("selected_schema") or {},
        invalid_sql=str(payload.get("invalid_sql") or ""),
        error_message=str(payload.get("error_message") or ""),
        reasoning_plan=payload.get("reasoning_plan"),
    )
    return {"sql": sql}

