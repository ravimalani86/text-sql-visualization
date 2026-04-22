from __future__ import annotations

import json
from typing import Any, Optional

from redis import Redis
from redis.exceptions import RedisError

from app.core.config import get_settings

_settings = get_settings()
redis_client = Redis.from_url(_settings.redis_url, decode_responses=True)


def get_json(key: str) -> Optional[dict[str, Any]]:
    try:
        raw = redis_client.get(key)
        if not raw:
            return None
        return json.loads(raw)
    except (RedisError, json.JSONDecodeError):
        return None


def set_json(key: str, value: dict[str, Any], ttl_seconds: int) -> None:
    try:
        redis_client.set(name=key, value=json.dumps(value), ex=ttl_seconds)
    except RedisError:
        # Cache write failure should not break request flow.
        return
