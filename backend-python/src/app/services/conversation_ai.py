from __future__ import annotations

from pathlib import Path

from jinja2 import Template

from app.services.llm_client import get_llm_client, get_llm_model, get_response_text

_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "conversation_system.j2"
_SYSTEM_TEMPLATE = Template(_TEMPLATE_PATH.read_text(encoding="utf-8"))


def _render_system_prompt() -> str:
    return _SYSTEM_TEMPLATE.render()


def generate_conversation_reply(user_prompt: str) -> str:
    system_prompt = _render_system_prompt()
    client = get_llm_client()
    response = client.messages.create(
        model=get_llm_model(),
        max_tokens=1200,
        system=system_prompt,
        messages=[
            {"role": "user", "content": user_prompt},
        ],
    )
    return get_response_text(response) or "How can I help you today?"

