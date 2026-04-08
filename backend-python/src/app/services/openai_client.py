from __future__ import annotations
from openai import OpenAI
from app.core.config import get_settings

def get_openai_client() -> OpenAI:
    settings = get_settings()
    return OpenAI(api_key=settings.openai_api_key)

