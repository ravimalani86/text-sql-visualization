from __future__ import annotations

import datetime as dt
import math
from typing import Any, Dict, List, Optional, Tuple


_DEFAULT_COLORWAY: List[str] = [
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
    "#4B5563",
]


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    h = (hex_color or "").strip().lstrip("#")
    if len(h) != 6:
        return f"rgba(14,165,233,{alpha})"
    try:
        r = int(h[0:2], 16)
        g = int(h[2:4], 16)
        b = int(h[4:6], 16)
        a = max(0.0, min(1.0, float(alpha)))
        return f"rgba({r},{g},{b},{a})"
    except Exception:
        return f"rgba(14,165,233,{alpha})"


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


def _pick_numeric_columns(columns: List[str], rows: List[Dict[str, Any]], *, exclude: Optional[set[str]] = None) -> List[str]:
    if not columns or not rows:
        return []
    excluded = exclude or set()
    numeric_cols: List[str] = []
    for c in columns:
        if c in excluded:
            continue
        sample = next((r.get(c) for r in rows if r.get(c) is not None), None)
        if sample is not None and _is_number(sample):
            numeric_cols.append(c)
    return numeric_cols


def _is_chartjs_config(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    if "type" not in obj or not isinstance(obj.get("type"), str):
        return False
    data = obj.get("data")
    return isinstance(data, dict) and isinstance(data.get("datasets"), list)


def _is_legacy_figure(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    data = obj.get("data")
    return isinstance(data, list) and any(isinstance(t, dict) and isinstance(t.get("type"), str) for t in data)


def normalize_chart_config(config: Any) -> Any:
    """
    Backwards/forwards compatible chart configuration normalization.

    - Old values: legacy figure dicts: {data: [...], layout?: {...}, config?: {...}}
    - New values: Chart.js config dicts: {type: 'bar'|'line'|..., data: {labels, datasets}, options?: {...}}
    """
    if _is_chartjs_config(config):
        return config
    if _is_legacy_figure(config):
        return _legacy_figure_to_chartjs(config)
    return config


def _merge_labels(label_lists: List[List[str]]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for labels in label_lists:
        for lab in labels:
            if lab in seen:
                continue
            seen.add(lab)
            out.append(lab)
    return out


def _legacy_figure_to_chartjs(fig: Dict[str, Any]) -> Dict[str, Any]:
    data = fig.get("data")
    layout = fig.get("layout") if isinstance(fig.get("layout"), dict) else {}
    if not isinstance(data, list):
        return {"type": "bar", "data": {"labels": [], "datasets": []}, "options": {"responsive": True}}

    title = ""
    try:
        title = str(((layout or {}).get("title") or {}).get("text") or "")
    except Exception:
        title = ""

    palette_raw = (layout or {}).get("colorway")
    palette = [str(c) for c in palette_raw] if isinstance(palette_raw, list) and palette_raw else _DEFAULT_COLORWAY

    # Pie is a special case (no cartesian axes).
    if len(data) == 1 and isinstance(data[0], dict) and data[0].get("type") == "pie":
        labels_raw = data[0].get("labels")
        values_raw = data[0].get("values")
        labels = [str(_to_label(v)) for v in labels_raw] if isinstance(labels_raw, list) else []
        values = [_to_float(v) for v in values_raw] if isinstance(values_raw, list) else []
        pairs = [(lab, val) for lab, val in zip(labels, values) if val is not None]
        labels2 = [p[0] for p in pairs]
        values2 = [p[1] for p in pairs]
        return {
            "type": "pie",
            "data": {
                "labels": labels2,
                "datasets": [
                    {
                        "label": title or "Series",
                        "data": values2,
                        "backgroundColor": [palette[i % len(palette)] for i in range(len(values2))],
                        "borderColor": "#ffffff",
                        "borderWidth": 1,
                    }
                ],
            },
            "options": {
                "responsive": True,
                "maintainAspectRatio": False,
                "plugins": {
                    "legend": {"display": True, "position": "bottom", "align": "start"},
                    "title": {"display": bool(title), "text": title},
                },
            },
        }

    horizontal = any(isinstance(t, dict) and t.get("orientation") == "h" for t in data)
    barmode = str((layout or {}).get("barmode") or "").lower()
    stacked = barmode == "stack" or any(isinstance(t, dict) and t.get("stackgroup") for t in data)

    has_bar = any(isinstance(t, dict) and t.get("type") == "bar" for t in data)
    has_lineish = any(
        isinstance(t, dict) and t.get("type") == "scatter" and "lines" in str(t.get("mode") or "")
        for t in data
    )

    base_type = "bar" if has_bar else ("line" if has_lineish else "scatter")

    x_title = ""
    y_title = ""
    try:
        x_title = str((((layout or {}).get("xaxis") or {}).get("title") or {}).get("text") or "")
    except Exception:
        x_title = ""
    try:
        y_title = str((((layout or {}).get("yaxis") or {}).get("title") or {}).get("text") or "")
    except Exception:
        y_title = ""

    if base_type in ("bar", "line"):
        label_lists: List[List[str]] = []
        for t in data:
            if not isinstance(t, dict):
                continue
            t_type = t.get("type")
            if t_type not in ("bar", "scatter"):
                continue
            if horizontal and t_type == "bar" and t.get("orientation") == "h":
                cats = t.get("y")
            else:
                cats = t.get("x")
            if isinstance(cats, list):
                label_lists.append([str(_to_label(v)) for v in cats])
        labels = _merge_labels(label_lists)

        datasets: List[Dict[str, Any]] = []
        for idx, t in enumerate([tt for tt in data if isinstance(tt, dict)]):
            t_type = t.get("type")
            name = str(t.get("name") or f"Series {idx + 1}")
            color = palette[idx % len(palette)]

            if t_type == "bar":
                if horizontal and t.get("orientation") == "h":
                    cats_raw = t.get("y")
                    vals_raw = t.get("x")
                else:
                    cats_raw = t.get("x")
                    vals_raw = t.get("y")
                cats = [str(_to_label(v)) for v in cats_raw] if isinstance(cats_raw, list) else []
                vals = [_to_float(v) for v in vals_raw] if isinstance(vals_raw, list) else []
                mapping = {c: v for c, v in zip(cats, vals) if v is not None}
                datasets.append(
                    {
                        "type": "bar" if base_type == "bar" else "bar",
                        "label": name,
                        "data": [mapping.get(lab) for lab in labels],
                        "backgroundColor": _hex_to_rgba(color, 0.35),
                        "borderColor": color,
                        "borderWidth": 1,
                    }
                )
                continue

            if t_type == "scatter":
                mode = str(t.get("mode") or "")
                if "lines" not in mode:
                    # If the chart is overall a line/bar, treat markers-only as a line dataset so it still renders.
                    if base_type != "scatter":
                        mode = "lines+markers"

                cats_raw = t.get("x")
                vals_raw = t.get("y")
                cats = [str(_to_label(v)) for v in cats_raw] if isinstance(cats_raw, list) else []
                vals = [_to_float(v) for v in vals_raw] if isinstance(vals_raw, list) else []
                mapping = {c: v for c, v in zip(cats, vals) if v is not None}

                line_shape = (t.get("line") or {}).get("shape") if isinstance(t.get("line"), dict) else None
                stepped = line_shape == "hv"
                fill = bool(t.get("fill") == "tozeroy" or t.get("stackgroup"))

                datasets.append(
                    {
                        "type": "line",
                        "label": name,
                        "data": [mapping.get(lab) for lab in labels],
                        "borderColor": color,
                        "backgroundColor": _hex_to_rgba(color, 0.18) if fill else _hex_to_rgba(color, 0.0),
                        "borderWidth": 2,
                        "pointRadius": 3,
                        "tension": 0.25,
                        "stepped": stepped,
                        "fill": fill,
                    }
                )
                continue

        options: Dict[str, Any] = {
            "responsive": True,
            "maintainAspectRatio": False,
            "plugins": {
                "legend": {"display": True, "position": "bottom", "align": "start"},
                "title": {"display": bool(title), "text": title},
            },
            "scales": {
                "x": {
                    "title": {"display": bool(x_title), "text": x_title},
                    "grid": {"color": "rgba(148, 163, 184, 0.25)"},
                    "ticks": {"color": "#0f172a"},
                },
                "y": {
                    "title": {"display": bool(y_title), "text": y_title},
                    "grid": {"color": "rgba(148, 163, 184, 0.25)"},
                    "ticks": {"color": "#0f172a"},
                },
            },
            "interaction": {"mode": "index", "intersect": False},
        }

        if horizontal:
            options["indexAxis"] = "y"

        if stacked:
            try:
                options["scales"]["x"]["stacked"] = True
                options["scales"]["y"]["stacked"] = True
            except Exception:
                pass

        return {"type": "bar" if has_bar else "line", "data": {"labels": labels, "datasets": datasets}, "options": options}

    # Scatter config uses explicit points.
    datasets2: List[Dict[str, Any]] = []
    all_x_numeric = True
    for idx, t in enumerate([tt for tt in data if isinstance(tt, dict) and tt.get("type") == "scatter"]):
        name = str(t.get("name") or f"Series {idx + 1}")
        color = palette[idx % len(palette)]
        xs_raw = t.get("x")
        ys_raw = t.get("y")
        xs = xs_raw if isinstance(xs_raw, list) else []
        ys = ys_raw if isinstance(ys_raw, list) else []
        pts: List[Dict[str, Any]] = []
        for x, y in zip(xs, ys):
            yv = _to_float(y)
            if yv is None:
                continue
            if _is_number(x):
                xv: Any = _to_float(x)
            else:
                all_x_numeric = False
                xv = _to_label(x)
            pts.append({"x": xv, "y": yv})
        datasets2.append(
            {
                "label": name,
                "data": pts,
                "borderColor": color,
                "backgroundColor": _hex_to_rgba(color, 0.25),
                "pointRadius": 4,
            }
        )

    x_scale: Dict[str, Any] = {"grid": {"color": "rgba(148, 163, 184, 0.25)"}, "ticks": {"color": "#0f172a"}}
    if all_x_numeric:
        x_scale["type"] = "linear"
    else:
        x_scale["type"] = "category"

    return {
        "type": "scatter",
        "data": {"datasets": datasets2},
        "options": {
            "responsive": True,
            "maintainAspectRatio": False,
            "plugins": {
                "legend": {"display": True, "position": "bottom", "align": "start"},
                "title": {"display": bool(title), "text": title},
            },
            "scales": {
                "x": {**x_scale, "title": {"display": bool(x_title), "text": x_title}},
                "y": {
                    "grid": {"color": "rgba(148, 163, 184, 0.25)"},
                    "ticks": {"color": "#0f172a"},
                    "title": {"display": bool(y_title), "text": y_title},
                },
            },
        },
    }


def _base_chart_render_config() -> Dict[str, Any]:
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
        "template": "chartjs_white",
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


def build_chart_config(
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
    y_fields_raw = intent.get("y_fields")
    y_fields: List[str] = []
    if isinstance(y_fields_raw, list):
        y_fields = [str(c) for c in y_fields_raw if isinstance(c, str) and c in columns]

    if not x_col or not y_col:
        dx, dy = _pick_default_xy(columns, rows)
        x_col = x_col or dx
        y_col = y_col or dy

    if not y_col:
        return None

    if y_col and y_col not in y_fields:
        y_fields = [y_col] + [c for c in y_fields if c != y_col]
    y_fields = [c for c in y_fields if _column_is_numeric(c, rows)]
    if not y_fields and y_col and _column_is_numeric(y_col, rows):
        y_fields = [y_col]

    # For horizontal bars, ChartJs expects numeric values on the x-axis.
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
    config: Dict[str, Any] = _base_chart_render_config()

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
        pie_value_col = y_fields[0] if y_fields else y_col
        if not pie_value_col:
            return None
        labels = [_to_label(r.get(x_col)) for r in rows]
        values = [_to_float(r.get(pie_value_col)) for r in rows]
        pairs = [(lab, val) for lab, val in zip(labels, values) if val is not None]
        if not pairs:
            return None
        labels2, values2 = zip(*pairs)
        # Pie traces do not use cartesian axes; keeping xaxis/yaxis as null
        # can break rendering in some ChartJs wrapper setups.
        pie_layout = {k: v for k, v in layout.items() if k not in ("xaxis", "yaxis")}
        return _legacy_figure_to_chartjs(
            {
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
        )

    if not x_col:
        x_vals: List[Any] = list(range(1, len(rows) + 1))
        layout["xaxis"]["title"]["text"] = "index"
    else:
        x_vals = [r.get(x_col) for r in rows]

    multi_metric_types = {"line", "area", "step", "scatter", "bar", "grouped_bar", "stacked_bar", "stacked_area", "combo"}
    if not series_col and chart_type in multi_metric_types and len(y_fields) <= 1:
        inferred = _pick_numeric_columns(columns, rows, exclude={x_col} if x_col else set())
        if inferred:
            y_fields = inferred
            y_col = y_fields[0]
            layout["yaxis"]["title"]["text"] = "value"

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
        if series_col and y_col:
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
        else:
            if not y_fields:
                return None
            layout["yaxis"]["title"]["text"] = "value"
            for metric in y_fields:
                ys = [_to_float(r.get(metric)) for r in rows]
                pts = [(x, y) for x, y in zip(x_vals, ys) if y is not None]
                if not pts:
                    continue
                xs2, ys2 = zip(*pts)
                traces.append(
                    {
                        "type": "scatter",
                        "mode": "lines",
                        "x": [str(_to_label(v)) for v in xs2],
                        "y": list(ys2),
                        "name": metric,
                        "stackgroup": "one",
                    }
                )
        if not traces:
            return None
        return _legacy_figure_to_chartjs({"data": traces, "layout": layout, "config": config})

    if chart_type == "combo":
        combo_fields = y_fields if y_fields else ([y_col] if y_col else [])
        combo_fields = [c for c in combo_fields if c]
        if not combo_fields:
            return None
        traces = []
        for idx, metric in enumerate(combo_fields):
            ys = [_to_float(r.get(metric)) for r in rows]
            pts = [(x, y) for x, y in zip(x_vals, ys) if y is not None]
            if not pts:
                continue
            xs2, ys2 = zip(*pts)
            x_labels = [str(_to_label(v)) for v in xs2]
            if idx == 0:
                traces.append({"type": "bar", "x": x_labels, "y": list(ys2), "name": metric, "marker": {"line": {"width": 0}}})
            else:
                traces.append(
                    {
                        "type": "scatter",
                        "mode": "lines+markers",
                        "x": x_labels,
                        "y": list(ys2),
                        "name": metric,
                        "line": {"width": 2.5},
                        "marker": {"size": 6},
                    }
                )
        if not traces:
            return None
        return _legacy_figure_to_chartjs({"data": traces, "layout": layout, "config": config})

    if series_col:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            key = _to_label(r.get(series_col))
            groups.setdefault(key, []).append(r)
        for key in sorted(groups.keys()):
            grp = groups[key]
            xs = [g.get(x_col) if x_col else None for g in grp] if x_col else list(range(1, len(grp) + 1))
            series_metrics = y_fields if y_fields else ([y_col] if y_col else [])
            for metric in series_metrics:
                ys = [_to_float(g.get(metric)) for g in grp]
                trace_name = key if len(series_metrics) == 1 else f"{key} - {metric}"
                tr = build_trace(trace_name, xs, ys)
                if tr:
                    traces.append(tr)
    else:
        single_or_multi_metrics = y_fields if y_fields else ([y_col] if y_col else [])
        for metric in single_or_multi_metrics:
            ys = [_to_float(r.get(metric)) for r in rows]
            tr = build_trace(metric, x_vals, ys)
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

    return _legacy_figure_to_chartjs({"data": traces, "layout": layout, "config": config})

