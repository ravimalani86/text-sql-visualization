from __future__ import annotations

import datetime as dt
import math
from typing import Any, Dict, List, Optional, Tuple


def _is_number(v: Any) -> bool:
    if v is None or isinstance(v, bool):
        return False
    if isinstance(v, (int, float)):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return False
        return True
    try:
        x = float(str(v).replace(",", "").strip())
        return not (math.isnan(x) or math.isinf(x))
    except Exception:
        return False


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    try:
        x = float(str(v).replace(",", "").strip())
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except Exception:
        return None


def _to_label(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    return str(v)


def _pick_default_xy(columns: List[str], rows: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    if not columns or not rows:
        return None, None

    numeric_cols: List[str] = []
    for c in columns:
        sample = next((r.get(c) for r in rows if r.get(c) is not None), None)
        if sample is not None and _is_number(sample):
            numeric_cols.append(c)

    y = numeric_cols[0] if numeric_cols else None
    if not y:
        return None, None

    for c in columns:
        if c == y:
            continue
        sample = next((r.get(c) for r in rows if r.get(c) is not None), None)
        if isinstance(sample, (dt.datetime, dt.date)):
            return c, y

    for c in columns:
        if c == y:
            continue
        sample = next((r.get(c) for r in rows if r.get(c) is not None), None)
        if sample is not None and not _is_number(sample):
            return c, y

    return None, y


def build_plotly_figure(
    *,
    intent: Dict[str, Any],
    columns: List[str],
    rows: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not rows or not columns:
        return None
    if not isinstance(intent, dict) or not intent.get("make_chart"):
        return None

    chart_type = str(intent.get("chart_type") or "bar").lower().strip().replace(" ", "_")
    x_col = intent.get("x") if intent.get("x") in columns else None
    y_col = intent.get("y") if intent.get("y") in columns else None
    series_col = intent.get("series") if intent.get("series") in columns else None

    if not x_col or not y_col:
        dx, dy = _pick_default_xy(columns, rows)
        x_col = x_col or dx
        y_col = y_col or dy

    if not y_col:
        return None

    title = intent.get("title") if isinstance(intent.get("title"), str) else None
    title = title or "Query Result"

    layout: Dict[str, Any] = {
        "title": {"text": title},
        "margin": {"l": 56, "r": 20, "t": 52, "b": 48},
        "xaxis": {"title": {"text": x_col or ""}, "automargin": True},
        "yaxis": {"title": {"text": y_col}, "automargin": True},
        "legend": {"orientation": "h"},
    }

    if chart_type == "pie":
        if not x_col:
            return None
        labels = [_to_label(r.get(x_col)) for r in rows]
        values = [_to_float(r.get(y_col)) for r in rows]
        pairs = [(lab, val) for lab, val in zip(labels, values) if val is not None]
        if not pairs:
            return None
        labels2, values2 = zip(*pairs)
        return {
            "data": [
                {
                    "type": "pie",
                    "labels": list(labels2),
                    "values": list(values2),
                    "textinfo": "label+percent",
                    "hoverinfo": "label+value+percent",
                }
            ],
            "layout": {"title": {"text": title}, "margin": layout["margin"], "legend": layout["legend"]},
        }

    if not x_col:
        x_vals: List[Any] = list(range(1, len(rows) + 1))
        layout["xaxis"]["title"]["text"] = "index"
    else:
        x_vals = [r.get(x_col) for r in rows]

    def build_trace(name: str, x_list: List[Any], y_list: List[Optional[float]]) -> Optional[Dict[str, Any]]:
        pts = [(x, y) for x, y in zip(x_list, y_list) if y is not None]
        if not pts:
            return None
        xs, ys = zip(*pts)
        if chart_type in ("line", "area"):
            t: Dict[str, Any] = {"type": "scatter", "mode": "lines+markers", "x": list(xs), "y": list(ys), "name": name}
            if chart_type == "area":
                t["fill"] = "tozeroy"
            return t
        if chart_type == "scatter":
            return {"type": "scatter", "mode": "markers", "x": list(xs), "y": list(ys), "name": name}
        if chart_type in ("horizontal_bar",):
            layout["margin"]["l"] = max(int(layout["margin"].get("l", 56) or 56), 120)
            return {"type": "bar", "orientation": "h", "x": list(ys), "y": [str(_to_label(v)) for v in xs], "name": name}
        return {"type": "bar", "x": [str(_to_label(v)) for v in xs], "y": list(ys), "name": name}

    traces: List[Dict[str, Any]] = []
    if series_col:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            key = _to_label(r.get(series_col))
            groups.setdefault(key, []).append(r)
        for key in sorted(groups.keys()):
            grp = groups[key]
            xs = [g.get(x_col) if x_col else None for g in grp] if x_col else list(range(1, len(grp) + 1))
            ys = [_to_float(g.get(y_col)) for g in grp]
            tr = build_trace(key, xs, ys)
            if tr:
                traces.append(tr)
    else:
        ys = [_to_float(r.get(y_col)) for r in rows]
        tr = build_trace(y_col, x_vals, ys)
        if tr:
            traces.append(tr)

    if not traces:
        return None

    if chart_type == "stacked_bar":
        layout["barmode"] = "stack"
    elif chart_type == "grouped_bar":
        layout["barmode"] = "group"

    return {"data": traces, "layout": layout}

