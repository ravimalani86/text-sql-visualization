from __future__ import annotations

from typing import Any

from app.core.config import get_settings
from app.services.schema_cache import get_cached_schema
from app.services.schema_selector import select_relevant_schema
from app.mcp.shared.cache import get_schema_cache, set_schema_cache


def run(payload: dict[str, Any]) -> dict[str, Any]:
    user_prompt = str(payload.get("user_prompt") or "")
    max_tables = int(payload.get("max_tables") or get_settings().schema_search_max_tables)

    schema = get_schema_cache()
    if schema is None:
        schema = get_cached_schema()
        if schema:
            set_schema_cache(schema)
    if not schema:
        raise ValueError("No tables found in database")

    selected = select_relevant_schema(
        user_prompt=user_prompt,
        schema=schema,
        max_tables=max_tables,
    )
    return {
        "schema": schema,
        "selected_schema": selected,
        "retrieved_tables": list(selected.keys()),
    }

