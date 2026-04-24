from __future__ import annotations

import json
from pathlib import Path
from typing import Any


_DEF_DIR = Path(__file__).resolve().parent / "definitions"


def load_skill_definitions() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in sorted(_DEF_DIR.glob("*.json")):
        with p.open("r", encoding="utf-8") as f:
            out.append(json.load(f))
    return out


def load_tool_specs() -> list[dict[str, Any]]:
    """Return tool schemas compatible with LLM tool-calling APIs."""
    defs = load_skill_definitions()
    tools: list[dict[str, Any]] = []
    for d in defs:
        tools.append(
            {
                "name": d["name"],
                "description": d.get("description", ""),
                "input_schema": d.get("input_schema", {"type": "object", "properties": {}}),
            }
        )
    return tools

