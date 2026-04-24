from __future__ import annotations

import json
from typing import Any, Optional

from app.core.config import get_settings
from app.db.cache import get_json, set_json


def get_schema_cache() -> Optional[dict[str, Any]]:
    settings = get_settings()
    return get_json(settings.schema_cache_key)


def set_schema_cache(value: dict[str, Any]) -> None:
    settings = get_settings()
    set_json(
        key=settings.schema_cache_key,
        value=value,
        ttl_seconds=int(settings.schema_cache_ttl_seconds),
    )


def query_cache_key(prompt: str) -> str:
    normalized = " ".join((prompt or "").strip().lower().split())
    return f"app:query_cache:v1:{normalized}"


def get_query_cache(prompt: str) -> Optional[dict[str, Any]]:
    return get_json(query_cache_key(prompt))


def set_query_cache(prompt: str, value: dict[str, Any], ttl_seconds: int = 300) -> None:
    set_json(key=query_cache_key(prompt), value=value, ttl_seconds=ttl_seconds)


def encode_json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=True)

