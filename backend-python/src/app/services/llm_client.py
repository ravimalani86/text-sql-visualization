from __future__ import annotations

from anthropic import Anthropic

from app.core.config import get_settings


def get_llm_client() -> Anthropic:
    settings = get_settings()
    return Anthropic(api_key=settings.anthropic_api_key)


def get_llm_model() -> str:
    settings = get_settings()
    return settings.anthropic_model


def get_response_text(response: object) -> str:
    content = getattr(response, "content", None)
    if not isinstance(content, list):
        return ""

    parts: list[str] = []
    for block in content:
        if getattr(block, "type", None) == "text":
            text = getattr(block, "text", "")
            if text:
                parts.append(str(text))
    return "\n".join(parts).strip()
