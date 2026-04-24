from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jinja2 import Template

from app.services.llm_client import get_llm_client, get_llm_model, get_response_text


_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "schema_selection_system.j2"
_SCHEMA_SELECTION_SYSTEM_PROMPT_TEMPLATE = Template(_TEMPLATE_PATH.read_text(encoding="utf-8"))


def _render_system_prompt() -> str:
    return _SCHEMA_SELECTION_SYSTEM_PROMPT_TEMPLATE.render()


def _extract_table_names(raw_text: str) -> list[str]:
    text = (raw_text or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            tables = parsed.get("tables")
            if isinstance(tables, list):
                return [str(t).strip() for t in tables if str(t).strip()]
        if isinstance(parsed, list):
            return [str(t).strip() for t in parsed if str(t).strip()]
    except Exception:
        pass
    return [t.strip() for t in text.split(",") if t.strip()]


def select_relevant_schema(
    *,
    user_prompt: str,
    schema: dict[str, Any],
    max_tables: int = 8,
) -> dict[str, Any]:
    if not schema:
        return {}

    table_names = list(schema.keys())
    if len(table_names) <= max_tables:
        return schema

    payload = {
        "user_prompt": user_prompt,
        "table_names": table_names,
        "schema": schema,
        "max_tables": max_tables,
    }
    client = get_llm_client()
    response = client.messages.create(
        model=get_llm_model(),
        max_tokens=1200,
        system=_render_system_prompt(),
        messages=[
            {"role": "user", "content": json.dumps(payload, ensure_ascii=True)},
        ],
    )
    selected = _extract_table_names(get_response_text(response))
    valid_selected = [name for name in selected if name in schema]
    if not valid_selected:
        # Safe fallback: deterministic first N tables.
        valid_selected = table_names[:max_tables]
    return {name: schema[name] for name in valid_selected}
