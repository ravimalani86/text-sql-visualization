from __future__ import annotations

from typing import Any

import requests


_SKILL_TO_ENDPOINT = {
    "get_schema": "/mcp/get_schema",
    "generate_sql": "/mcp/generate_sql",
    "validate_sql": "/mcp/validate_sql",
    "execute_sql": "/mcp/execute_sql",
    "correct_sql": "/mcp/correct_sql",
    "generate_chart": "/mcp/generate_chart",
    "build_response": "/mcp/build_response",
}


def execute_skill(skill_name: str, payload: dict[str, Any], *, base_url: str) -> dict[str, Any]:
    endpoint = _SKILL_TO_ENDPOINT.get(skill_name)
    if not endpoint:
        raise ValueError(f"Unknown skill: {skill_name}")

    resp = requests.post(
        f"{base_url.rstrip('/')}{endpoint}",
        json={"payload": payload},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise ValueError(f"Skill {skill_name} returned non-object response")
    return data

