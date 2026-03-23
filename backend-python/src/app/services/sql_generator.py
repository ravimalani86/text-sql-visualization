from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jinja2 import Template

from app.services.openai_client import get_openai_client


_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "text_to_sql_system.j2"
_SQL_SYSTEM_PROMPT_TEMPLATE = Template(_TEMPLATE_PATH.read_text(encoding="utf-8"))


def render_sql_system_prompt(*, schema: dict[str, Any]) -> str:
    return _SQL_SYSTEM_PROMPT_TEMPLATE.render(schema_json=json.dumps(schema, indent=2))


def text_to_sql(user_prompt: str, schema: dict[str, Any]) -> str:
    system_prompt = render_sql_system_prompt(schema=schema)
    client = get_openai_client()

    response = client.responses.create(
        model="gpt-5",
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )

    return (response.output_text or "").strip()

