from __future__ import annotations

from typing import Any, Dict, List, Optional


def _prompt_has(prompt: str, keywords: List[str]) -> bool:
    p = (prompt or "").lower()
    return any(k in p for k in keywords)


def _format_value(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, float):
        return f"{v:,.2f}"
    return str(v)


def build_assistant_text(
    *,
    prompt: str,
    columns: List[str],
    rows: List[Dict[str, Any]],
    chart_intent: Optional[Dict[str, Any]],
) -> str:
    if not rows:
        return "I ran the analysis, but no rows matched this request."

    row_count = len(rows)
    chart_type = (chart_intent or {}).get("chart_type")

    if row_count == 1 and len(columns) >= 1:
        first = rows[0]
        preview = ", ".join(f"{c}: {_format_value(first.get(c))}" for c in columns[:4])
        return f"I found one matching result. {preview}"

    if len(columns) == 2 and row_count <= 10:
        left, right = columns[0], columns[1]
        top = rows[0]
        return (
            f"I found {row_count} rows. "
            f"The top item is `{_format_value(top.get(left))}` with `{_format_value(top.get(right))}`."
        )

    if chart_type:
        return f"I found {row_count} rows and prepared a `{chart_type}` chart to make the trend/comparison easier to read."

    return f"I found {row_count} rows for your request. I can also visualize this if you want a chart."


def build_response_blocks(
    *,
    prompt: str,
    sql: str,
    columns: List[str],
    rows: List[Dict[str, Any]],
    chart_intent: Optional[Dict[str, Any]],
    plotly: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []

    text_block = {
        "type": "text",
        "content": build_assistant_text(
            prompt=prompt,
            columns=columns,
            rows=rows,
            chart_intent=chart_intent,
        ),
    }
    blocks.append(text_block)

    wants_sql = _prompt_has(prompt, ["sql", "query", "statement", "show query", "generated query"])
    wants_chart = _prompt_has(prompt, ["chart", "graph", "plot", "trend", "visual"])
    wants_table = _prompt_has(prompt, ["table", "list", "rows", "detail", "show all", "top"])

    if sql and (wants_sql or not wants_chart):
        blocks.append({"type": "sql", "sql": sql})

    if rows:
        table_rows = rows[:50]
        if wants_table or (not wants_chart) or len(rows) <= 30:
            blocks.append(
                {
                    "type": "table",
                    "columns": columns,
                    "rows": table_rows,
                    "meta": {"row_count": len(rows), "shown_rows": len(table_rows)},
                }
            )

    if plotly and (wants_chart or len(rows) >= 3):
        blocks.append(
            {
                "type": "chart",
                "chart_type": (chart_intent or {}).get("chart_type"),
                "plotly": plotly,
            }
        )

    return blocks
