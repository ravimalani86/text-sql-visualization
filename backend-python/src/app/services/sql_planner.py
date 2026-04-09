from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jinja2 import Template

from app.services.openai_client import get_openai_client, get_openai_model


_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "sql_planner_system.j2"
_SQL_PLANNER_SYSTEM_PROMPT_TEMPLATE = Template(_TEMPLATE_PATH.read_text(encoding="utf-8"))


def _render_system_prompt() -> str:
    return _SQL_PLANNER_SYSTEM_PROMPT_TEMPLATE.render()


def generate_sql_plan(*, user_prompt: str, schema: dict[str, Any]) -> str:
    payload = {
        "user_prompt": user_prompt,
        "schema": schema,
    }
    client = get_openai_client()
    response = client.responses.create(
        model=get_openai_model(),
        input=[
            {"role": "system", "content": _render_system_prompt()},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=True)},
        ],
    )
    plan = (response.output_text or "").strip()
    return plan[:2000]
