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


def _column_is_numeric(col: str, rows: List[Dict[str, Any]]) -> bool:
    if not col:
        return False
    sample = next((r.get(col) for r in rows if r.get(col) is not None), None)
    return sample is not None and _is_number(sample)


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


def _base_plotly_config() -> Dict[str, Any]:
    return {
        "responsive": True,
        "displayModeBar": False,
        "displaylogo": False,
        "scrollZoom": False,
    }


def _base_layout(*, title: str, x_title: str, y_title: str, horizontal: bool = False) -> Dict[str, Any]:
    axis_common: Dict[str, Any] = {
        "automargin": True,
        "showgrid": True,
        "gridcolor": "rgba(148, 163, 184, 0.25)",
        "zeroline": False,
        "showline": True,
        "linecolor": "rgba(148, 163, 184, 0.45)",
        "ticks": "outside",
        "ticklen": 4,
        "tickcolor": "rgba(148, 163, 184, 0.6)",
        "tickfont": {"size": 12, "color": "#0f172a"},
        "title": {"text": ""},
    }

    xaxis = {**axis_common, "title": {"text": x_title}}
    yaxis = {**axis_common, "title": {"text": y_title}}
    # Helps avoid label overlap/clipping on smaller chart sizes.
    xaxis["ticklabeloverflow"] = "hide past domain"
    yaxis["ticklabeloverflow"] = "hide past domain"
    if horizontal:
        xaxis["title"]["text"] = y_title
        yaxis["title"]["text"] = x_title

    return {
        "template": "plotly_white",
        "paper_bgcolor": "#ffffff",
        "plot_bgcolor": "#ffffff",
        "font": {"family": "Inter, Segoe UI, Roboto, Arial, sans-serif", "size": 13, "color": "#0f172a"},
        "title": {
            "text": title,
            "x": 0.5,
            "xanchor": "center",
            "y": 0.98,
            "yanchor": "top",
            "font": {"size": 15, "color": "#0f172a"},
            "pad": {"t": 6, "b": 10},
            "automargin": True,
        },
        # Keep margins compact; specific chart types can override.
        "margin": {"l": 64 if horizontal else 56, "r": 18, "t": 56, "b": 92},
        "showlegend": True,
        "legend": {
            "orientation": "h",
            "x": 0,
            "y": -0.25,
            "xanchor": "left",
            "yanchor": "top",
            "bgcolor": "rgba(255,255,255,0)",
        },
        # Professional, high-separation palette for clearer multi-series distinction
        "colorway": [
            "#2563EB", 
            "#0EA5E9",
            "#14B8A6",
            "#22C55E",
            "#84CC16",
            "#F59E0B",
            "#F97316",
            "#EF4444",
            "#A855F7",
            "#EC4899",
            "#1D4ED8",
            "#0F766E",
            "#0891B2",
            "#0284C7",
            "#4B5563" 
        ],
        "hoverlabel": {"bgcolor": "#0f172a", "font": {"color": "white"}},
        "xaxis": xaxis,
        "yaxis": yaxis,
    }


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
    if chart_type == "column":
        chart_type = "bar"
    x_col = intent.get("x") if intent.get("x") in columns else None
    y_col = intent.get("y") if intent.get("y") in columns else None
    series_col = intent.get("series") if intent.get("series") in columns else None

    if not x_col or not y_col:
        dx, dy = _pick_default_xy(columns, rows)
        x_col = x_col or dx
        y_col = y_col or dy

    if not y_col:
        return None

    # For horizontal bars, Plotly expects numeric values on the x-axis.
    # If the intent swapped x/y (common in LLM output), correct it using actual row types.
    if chart_type == "horizontal_bar" and x_col and y_col:
        x_is_num = _column_is_numeric(x_col, rows)
        y_is_num = _column_is_numeric(y_col, rows)
        if x_is_num and not y_is_num:
            x_col, y_col = y_col, x_col

    title = intent.get("title") if isinstance(intent.get("title"), str) else None
    title = title or ""

    layout: Dict[str, Any] = _base_layout(
        title=title,
        x_title=x_col or "",
        y_title=y_col,
        horizontal=(chart_type == "horizontal_bar"),
    )
    config: Dict[str, Any] = _base_plotly_config()

    # --- UI polish for ALL charts ---
    max_x_label_len = 0
    if x_col:
        for r in rows[:200]:
            v = _to_label(r.get(x_col))
            if len(v) > max_x_label_len:
                max_x_label_len = len(v)

    # Avoid clipped category labels for horizontal bars.
    if chart_type == "horizontal_bar" and x_col:
        layout["margin"]["l"] = max(int(layout["margin"].get("l", 56) or 56), min(280, 64 + max_x_label_len * 7))

    # If x labels are long and chart is likely categorical, rotate and add bottom margin.
    if chart_type in ("bar", "grouped_bar", "stacked_bar", "combo") and x_col and max_x_label_len >= 10:
        layout["xaxis"]["tickangle"] = -35
        layout["margin"]["b"] = max(int(layout["margin"].get("b", 54) or 54), min(140, 54 + max_x_label_len * 3))

    # If too many x categories, reduce tick font a bit.
    if x_col and len(rows) >= 16:
        try:
            layout["xaxis"]["tickfont"]["size"] = 11
        except Exception:
            pass
        # Also reduce the number of ticks to avoid collisions when space is tight.
        layout["xaxis"]["nticks"] = 6
    elif x_col and len(rows) >= 10:
        layout["xaxis"]["nticks"] = 8

    if chart_type == "pie":
        if not x_col:
            return None
        labels = [_to_label(r.get(x_col)) for r in rows]
        values = [_to_float(r.get(y_col)) for r in rows]
        pairs = [(lab, val) for lab, val in zip(labels, values) if val is not None]
        if not pairs:
            return None
        labels2, values2 = zip(*pairs)
        # Pie traces do not use cartesian axes; keeping xaxis/yaxis as null
        # can break rendering in some Plotly wrapper setups.
        pie_layout = {k: v for k, v in layout.items() if k not in ("xaxis", "yaxis")}
        return {
            "data": [
                {
                    "type": "pie",
                    "labels": list(labels2),
                    "values": list(values2),
                    "textinfo": "label+percent",
                    "hoverinfo": "label+value+percent",
                    "marker": {"line": {"color": "#ffffff", "width": 1}},
                }
            ],
            "layout": pie_layout,
            "config": config,
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
        if chart_type in ("line", "area", "step"):
            t: Dict[str, Any] = {
                "type": "scatter",
                "mode": "lines+markers",
                "x": list(xs),
                "y": list(ys),
                "name": name,
                "line": {"width": 2.5},
                "marker": {"size": 6},
            }
            if chart_type == "area":
                t["fill"] = "tozeroy"
            if chart_type == "step":
                t["line"] = {"shape": "hv"}
            return t
        if chart_type == "scatter":
            return {"type": "scatter", "mode": "markers", "x": list(xs), "y": list(ys), "name": name, "marker": {"size": 7}}
        if chart_type in ("horizontal_bar",):
            layout["margin"]["l"] = max(int(layout["margin"].get("l", 56) or 56), 120)
            return {
                "type": "bar",
                "orientation": "h",
                "x": list(ys),
                "y": [str(_to_label(v)) for v in xs],
                "name": name,
                "marker": {"line": {"width": 0}},
            }
        return {"type": "bar", "x": [str(_to_label(v)) for v in xs], "y": list(ys), "name": name, "marker": {"line": {"width": 0}}}

    traces: List[Dict[str, Any]] = []
    if chart_type == "stacked_area":
        if not series_col:
            return None
        layout["yaxis"]["title"]["text"] = y_col

        groups: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            key = _to_label(r.get(series_col))
            groups.setdefault(key, []).append(r)

        for key in sorted(groups.keys()):
            grp = groups[key]
            xs = [g.get(x_col) if x_col else None for g in grp] if x_col else list(range(1, len(grp) + 1))
            ys = [_to_float(g.get(y_col)) for g in grp]
            pts = [(x, y) for x, y in zip(xs, ys) if y is not None]
            if not pts:
                continue
            xs2, ys2 = zip(*pts)
            traces.append(
                {
                    "type": "scatter",
                    "mode": "lines",
                    "x": [str(_to_label(v)) for v in xs2],
                    "y": list(ys2),
                    "name": key,
                    "stackgroup": "one",
                }
            )
        if not traces:
            return None
        return {"data": traces, "layout": layout, "config": config}

    if chart_type == "combo":
        ys = [_to_float(r.get(y_col)) for r in rows]
        pts = [(x, y) for x, y in zip(x_vals, ys) if y is not None]
        if not pts:
            return None
        xs2, ys2 = zip(*pts)
        x_labels = [str(_to_label(v)) for v in xs2]
        traces = [
            {"type": "bar", "x": x_labels, "y": list(ys2), "name": y_col},
            {"type": "scatter", "mode": "lines+markers", "x": x_labels, "y": list(ys2), "name": y_col},
        ]
        traces[0]["marker"] = {"line": {"width": 0}}
        traces[1]["line"] = {"width": 2.5}
        traces[1]["marker"] = {"size": 6}
        return {"data": traces, "layout": layout, "config": config}

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

    # Keep legend at bottom for all chart types (requested).
    # Ensure enough space for longer legends.
    if len(traces) >= 6:
        layout["margin"]["b"] = max(int(layout["margin"].get("b", 86) or 86), 120)

    # Keep legend always visible (requested). If spacing is tight, rely on legend positioning/margins only.
    if len(traces) <= 1:
        layout["margin"]["t"] = max(int(layout["margin"].get("t", 46) or 46), 40)

    if chart_type == "stacked_bar":
        layout["barmode"] = "stack"
    elif chart_type == "grouped_bar":
        layout["barmode"] = "group"

    return {"data": traces, "layout": layout, "config": config}

