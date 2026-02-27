from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from jinja2 import Template
from openai import OpenAI

def _client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY environment variable is not set")
    return OpenAI(api_key=api_key)


ALLOWED_CHART_TYPES = {
    "bar",
    "grouped_bar",
    "stacked_bar",
    "horizontal_bar",
    "line",
    "area",
    "scatter",
    "pie",
}

CHART_INTENT_SYSTEM_PROMPT_TEMPLATE = Template(
    """
You are a data visualization planner.
Return ONLY JSON. No markdown. No explanations.

You DO NOT receive any raw data values. You must only decide a logical chart intent based on:
- user_prompt
- generated_sql
- available_columns

Allowed chart_type values:
{{ allowed_chart_types }}

Return JSON with this exact shape (keys only from this list):
{
  "make_chart": true|false,
  "chart_type": "{{ chart_type_union }}",
  "x": "<column name>",
  "y": "<column name>",
  "series": "<optional column name for grouping>" ,
  "title": "<optional short title>"
}

Rules:
- Use ONLY column names from available_columns for x/y/series.
- NEVER include any data arrays, sample rows, values, labels, or datasets.
- If a chart doesn't make sense, set make_chart=false.
- Prefer:
  - line/area when x looks like time (date, month, year)
  - pie only when comparing parts of a whole (top categories) with a single numeric y
  - grouped_bar when series grouping exists (e.g. product, category)
""".strip()
)


def _render_chart_intent_system_prompt() -> str:
    chart_types = sorted(ALLOWED_CHART_TYPES)
    return CHART_INTENT_SYSTEM_PROMPT_TEMPLATE.render(
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


def _clean_intent(
    raw: Dict[str, Any],
    *,
    available_columns: List[str],
) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {"make_chart": False}

    make_chart = bool(raw.get("make_chart"))
    if not make_chart:
        return {"make_chart": False}

    chart_type = (raw.get("chart_type") or raw.get("type") or "bar")  # tolerate "type"
    chart_type = str(chart_type).lower().strip().replace(" ", "_")
    if chart_type not in ALLOWED_CHART_TYPES:
        chart_type = "bar"

    x = raw.get("x")
    y = raw.get("y")
    series = raw.get("series")
    title = raw.get("title")

    def _pick_col(v: Any) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        return s if s in available_columns else None

    intent: Dict[str, Any] = {
        "make_chart": True,
        "chart_type": chart_type,
        "x": _pick_col(x),
        "y": _pick_col(y),
        "series": _pick_col(series),
        "title": str(title).strip() if isinstance(title, str) and title.strip() else None,
    }

    # Drop Nones for cleanliness
    return {k: v for k, v in intent.items() if v is not None} | {"make_chart": True}


def suggest_chart_intent(
    *,
    user_prompt: str,
    sql: str,
    columns: List[str],
) -> Dict[str, Any]:
    """
    LLM #2: Return ONLY a *logical* chart intent JSON.

    SECURITY / WORKFLOW CONSTRAINT:
    - Never send raw result rows (or any values) to the LLM.
    - The LLM must choose chart_type + column roles only.
    """
    system_prompt = _render_chart_intent_system_prompt()

    payload = {
        "user_prompt": (user_prompt or "").strip(),
        "generated_sql": (sql or "").strip(),
        "available_columns": columns,
    }

    resp = _client().responses.create(
        model="gpt-5",
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(payload)},
        ],
    )

    text = (getattr(resp, "output_text", None) or "").strip()
    try:
        raw = json.loads(text)
        if isinstance(raw, dict):
            return _clean_intent(raw, available_columns=columns)
    except Exception:
        pass

    raw = _extract_json_object(text)
    return _clean_intent(raw, available_columns=columns)

