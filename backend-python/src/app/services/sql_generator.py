from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from jinja2 import Template

from app.services.llm_client import get_llm_client, get_llm_model, get_response_text


_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "text_to_sql_system.j2"
_SQL_SYSTEM_PROMPT_TEMPLATE = Template(_TEMPLATE_PATH.read_text(encoding="utf-8"))
_CORRECTION_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "sql_correction_system.j2"
_SQL_CORRECTION_SYSTEM_PROMPT_TEMPLATE = Template(_CORRECTION_TEMPLATE_PATH.read_text(encoding="utf-8"))


def render_sql_system_prompt(*, schema: dict[str, Any], reasoning_plan: str | None = None) -> str:
    return _SQL_SYSTEM_PROMPT_TEMPLATE.render(
        schema_json=json.dumps(schema, indent=2),
        reasoning_plan=(reasoning_plan or "").strip(),
    )


def render_sql_correction_system_prompt(*, schema: dict[str, Any]) -> str:
    return _SQL_CORRECTION_SYSTEM_PROMPT_TEMPLATE.render(schema_json=json.dumps(schema, indent=2))


def _extract_sql(response_text: str) -> str:
    text = (response_text or "").strip()
    if not text:
        return ""

    fenced = re.search(r"```(?:sql)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        text = fenced.group(1).strip()

    # Return only one statement.
    return text.split(";")[0].strip()


def text_to_sql(user_prompt: str, schema: dict[str, Any], reasoning_plan: str | None = None) -> str:
    system_prompt = render_sql_system_prompt(schema=schema, reasoning_plan=reasoning_plan)
    client = get_llm_client()

    response = client.messages.create(
        model=get_llm_model(),
        max_tokens=1500,
        system=system_prompt,
        messages=[
            {"role": "user", "content": user_prompt},
        ],
    )

    return _extract_sql(get_response_text(response))


def correct_sql(
    *,
    user_prompt: str,
    schema: dict[str, Any],
    invalid_sql: str,
    error_message: str,
    reasoning_plan: str | None = None,
) -> str:
    system_prompt = render_sql_correction_system_prompt(schema=schema)
    correction_payload = {
        "user_prompt": user_prompt,
        "invalid_sql": invalid_sql,
        "error_message": error_message,
        "reasoning_plan": (reasoning_plan or "").strip(),
    }
    client = get_llm_client()

    response = client.messages.create(
        model=get_llm_model(),
        max_tokens=1500,
        system=system_prompt,
        messages=[
            {"role": "user", "content": json.dumps(correction_payload, ensure_ascii=True)},
        ],
    )
    return _extract_sql(get_response_text(response))
