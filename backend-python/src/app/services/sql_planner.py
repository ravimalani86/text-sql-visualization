from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jinja2 import Template

from app.services.llm_client import get_llm_client, get_llm_model, get_response_text


_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "sql_planner_system.j2"
_SQL_PLANNER_SYSTEM_PROMPT_TEMPLATE = Template(_TEMPLATE_PATH.read_text(encoding="utf-8"))


def _render_system_prompt() -> str:
    return _SQL_PLANNER_SYSTEM_PROMPT_TEMPLATE.render()


def generate_sql_plan(*, user_prompt: str, schema: dict[str, Any]) -> str:
    payload = {
        "user_prompt": user_prompt,
        "schema": schema,
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
    plan = get_response_text(response)
    return plan[:2000]
