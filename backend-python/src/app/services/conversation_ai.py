from __future__ import annotations

from pathlib import Path

from jinja2 import Template

from app.services.openai_client import get_openai_client

_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "conversation_system.j2"
_SYSTEM_TEMPLATE = Template(_TEMPLATE_PATH.read_text(encoding="utf-8"))


def _render_system_prompt() -> str:
    return _SYSTEM_TEMPLATE.render()


def generate_conversation_reply(user_prompt: str) -> str:
    system_prompt = _render_system_prompt()
    client = get_openai_client()
    response = client.responses.create(
        model="gpt-5",
        input=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {"role": "user", "content": user_prompt},
        ],
    )
    return (response.output_text or "").strip() or "How can I help you today?"

