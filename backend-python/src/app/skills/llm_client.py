from __future__ import annotations

import json
import os
from typing import Any

from anthropic import Anthropic


def _anthropic_text(response: object) -> str:
    content = getattr(response, "content", None)
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if getattr(block, "type", None) == "text":
            txt = getattr(block, "text", "")
            if txt:
                parts.append(str(txt))
    return "\n".join(parts).strip()


def _extract_json_object(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass

    start = raw.find("{")
    if start == -1:
        return {}
    depth = 0
    for i in range(start, len(raw)):
        ch = raw[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = raw[start : i + 1]
                try:
                    parsed = json.loads(candidate)
                    return parsed if isinstance(parsed, dict) else {}
                except Exception:
                    return {}
    return {}


def generate_agent_step(
    *,
    user_prompt: str,
    tools: list[dict[str, Any]],
    steps: list[dict[str, Any]],
    max_steps: int = 6,
) -> dict[str, Any]:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    client = Anthropic(api_key=api_key)

    tool_list = ", ".join(t.get("name", "") for t in tools)
    print(tool_list)
    prompt = (
        "You are an autonomous analytics agent.\n"
        "Think step-by-step. At each step, return JSON ONLY with keys:\n"
        "- thought: string\n"
        "- action: one of tool names or 'final_answer'\n"
        "- input: object (required when action is a tool)\n"
        "- output: object (required when action is final_answer)\n\n"
        f"Available tools: {tool_list}\n"
        f"Maximum allowed steps: {max_steps}\n"
        "Use tools only when needed. If enough information is available, use action='final_answer'.\n"
        "Do not include markdown fences.\n\n"
        f"User prompt:\n{user_prompt}\n\n"
        f"Previous steps JSON:\n{json.dumps(steps, ensure_ascii=True)}"
    )
    resp = client.messages.create(
        model=os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001"),
        max_tokens=900,
        messages=[{"role": "user", "content": prompt}],
    )
    return _extract_json_object(_anthropic_text(resp))

