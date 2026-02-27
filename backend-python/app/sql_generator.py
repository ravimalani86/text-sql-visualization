from openai import OpenAI
import os
import json
from typing import Any

from jinja2 import Template

def _client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY environment variable is not set")
    return OpenAI(api_key=api_key)


SQL_SYSTEM_PROMPT_TEMPLATE = Template(
    """
You are a PostgreSQL expert.

Database schema (tables, columns, types):
{{ schema_json }}

STRICT RULES:
- Return ONLY ONE SQL statement (SELECT or WITH)
- Use ONLY tables and columns from the schema
- PostgreSQL syntax only
- No markdown, no comments, no explanations
- Use snake_case aliases
- LIMIT result to 500 rows unless user explicitly asks otherwise
""".strip()
)


def _render_sql_system_prompt(*, schema: dict[str, Any]) -> str:
    return SQL_SYSTEM_PROMPT_TEMPLATE.render(
        schema_json=json.dumps(schema, indent=2),
    )


def text_to_sql(user_prompt: str, schema: dict[str, Any]) -> str:
    system_prompt = _render_sql_system_prompt(schema=schema)

    response = _client().responses.create(
        model="gpt-5",
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
    )

    return response.output_text.strip()
