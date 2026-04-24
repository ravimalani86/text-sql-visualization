from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Template

from app.services.llm_client import get_llm_client, get_llm_model, get_response_text


ALLOWED_CHART_TYPES = {
    "area",
    "bar",
    "column",
    "combo",
    "grouped_bar",
    "horizontal_bar",
    "line",
    "scatter",
    "pie",
    "stacked_area",
    "stacked_bar",
    "step",
}

ALLOWED_COMPARISON_MODES = {"series", "multi_metric"}

_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "chart_intent_system.j2"
_SYSTEM_TEMPLATE = Template(_TEMPLATE_PATH.read_text(encoding="utf-8"))


def _render_system_prompt() -> str:
    chart_types = sorted(ALLOWED_CHART_TYPES)
    return _SYSTEM_TEMPLATE.render(
        allowed_chart_types=chart_types,
        chart_type_union="|".join(chart_types),
    )


def _extract_json_object(text: str) -> Dict[str, Any]:
    if not text:
        return {"make_chart": False}
    start = text.find("{")
    if start == -1:
        return {"make_chart": False}
    depth = 0
    for i in range(start, len(text)):
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = text[start : i + 1]
                try:
                    obj = json.loads(candidate)
                    return obj if isinstance(obj, dict) else {"make_chart": False}
                except Exception:
                    return {"make_chart": False}
    return {"make_chart": False}


def _clean_intent(raw: Dict[str, Any], *, available_columns: List[str]) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {"make_chart": False}
    make_chart = bool(raw.get("make_chart"))
    if not make_chart:
        return {"make_chart": False}

    chart_type = (raw.get("chart_type") or raw.get("type") or "bar")
    chart_type = str(chart_type).lower().strip().replace(" ", "_")
    if chart_type not in ALLOWED_CHART_TYPES:
        chart_type = "bar"

    def _pick_col(v: Any) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        return s if s in available_columns else None

    x = _pick_col(raw.get("x"))
    y = _pick_col(raw.get("y"))
    series = _pick_col(raw.get("series"))
    y_fields_raw = raw.get("y_fields")
    y_fields: List[str] = []
    if isinstance(y_fields_raw, list):
        for item in y_fields_raw:
            col = _pick_col(item)
            if col and col not in y_fields:
                y_fields.append(col)
    elif y:
        y_fields = [y]

    comparison_mode_raw = raw.get("comparison_mode")
    comparison_mode: Optional[str] = None
    if isinstance(comparison_mode_raw, str):
        mode = comparison_mode_raw.strip().lower()
        if mode in ALLOWED_COMPARISON_MODES:
            comparison_mode = mode

    title = raw.get("title")

    intent: Dict[str, Any] = {
        "make_chart": True,
        "chart_type": chart_type,
        "x": x,
        "y": y,
        "y_fields": y_fields if y_fields else None,
        "series": series,
        "comparison_mode": comparison_mode,
        "title": str(title).strip() if isinstance(title, str) and title.strip() else None,
    }

    # Drop Nones for cleanliness
    return {k: v for k, v in intent.items() if v is not None} | {"make_chart": True}


def suggest_chart_intent(*, user_prompt: str, sql: str, columns: List[str]) -> Dict[str, Any]:
    system_prompt = _render_system_prompt()
    payload = {
        "user_prompt": (user_prompt or "").strip(),
        "generated_sql": (sql or "").strip(),
        "available_columns": columns,
    }

    client = get_llm_client()
    resp = client.messages.create(
        model=get_llm_model(),
        max_tokens=1200,
        system=system_prompt,
        messages=[
            {"role": "user", "content": json.dumps(payload)},
        ],
    )

    text = get_response_text(resp)
    try:
        raw = json.loads(text)
        if isinstance(raw, dict):
            return _clean_intent(raw, available_columns=columns)
    except Exception:
        pass

    raw = _extract_json_object(text)
    return _clean_intent(raw, available_columns=columns)
