from __future__ import annotations

from typing import Any

from app.core.config import get_settings
from app.mcp.shared.cache import get_query_cache, set_query_cache
from app.services.sql_generator import text_to_sql
from app.services.sql_planner import generate_sql_plan


def run(payload: dict[str, Any]) -> dict[str, Any]:
    user_prompt = str(payload.get("user_prompt") or "")
    selected_schema = payload.get("selected_schema") or {}
    settings = get_settings()
    cached = get_query_cache(user_prompt)
    if cached and cached.get("sql"):
        return {
            "sql": cached.get("sql"),
            "reasoning_plan": cached.get("reasoning_plan"),
            "cache_hit": True,
        }

    reasoning_plan = payload.get("reasoning_plan")
    if reasoning_plan is None and settings.enable_sql_planning:
        reasoning_plan = generate_sql_plan(user_prompt=user_prompt, schema=selected_schema)

    sql = text_to_sql(user_prompt, selected_schema, reasoning_plan=reasoning_plan)
    set_query_cache(
        user_prompt,
        {"sql": sql, "reasoning_plan": reasoning_plan},
        ttl_seconds=300,
    )
    return {
        "sql": sql,
        "reasoning_plan": reasoning_plan,
        "cache_hit": False,
    }

