from __future__ import annotations

from typing import Any, Dict, List, Optional


def truncate(text: str, max_len: int) -> str:
    value = (text or "").strip()
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


def looks_incomplete_followup(prompt: str) -> bool:
    text_low = (prompt or "").strip().lower()
    if not text_low:
        return False

    explicit_metric_words = [
        "sales",
        "revenue",
        "profit",
        "amount",
        "quantity",
        "count",
        "sum",
        "avg",
        "average",
        "max",
        "min",
    ]
    explicit_change_words = [
        "where",
        "filter",
        "between",
        "before",
        "after",
        "for ",
        "in ",
        "by ",
        "group by",
        "order by",
    ]

    has_metric = any(w in text_low for w in explicit_metric_words)
    has_explicit_change = any(w in text_low for w in explicit_change_words)
    short_request = len(text_low.split()) <= 8
    return short_request and not has_metric and not has_explicit_change


def build_effective_prompt(prompt: str, latest_turns: Optional[List[Dict[str, Any]]]) -> str:
    if not latest_turns:
        return prompt

    turns = list(reversed(latest_turns))
    turns_context = []
    for i, t in enumerate(turns, start=1):
        tp = truncate(t.get("prompt") or "", 160)
        tsql = truncate(t.get("sql") or "", 320)
        tcols = t.get("columns") or []
        if not isinstance(tcols, list):
            tcols = []
        turns_context.append(
            f"Turn {i} - Previous user request: {tp}\n"
            f"- Previous generated SQL: {tsql}\n"
            f"- Previous result columns: {', '.join(str(c) for c in tcols[:10])}"
        )

    continuation_mode = "incomplete follow-up" if looks_incomplete_followup(prompt) else "follow-up"
    return (
        "You are continuing an existing analytics conversation.\n"
        f"Continuation mode: {continuation_mode}\n\n"
        "Previous turns context:\n"
        + "\n\n".join(turns_context)
        + "\n\n"
        "Follow-up SQL rules:\n"
        "1) Treat current request as continuation of previous analysis.\n"
        "2) If current request is incomplete/ambiguous, REUSE the previous metric, aggregation, grouping, and sorting.\n"
        "3) Only change the parts explicitly requested now (e.g., LIMIT, chart category, time filter).\n"
        "4) Do NOT change the metric unless the user clearly specifies a new metric.\n"
        "5) Keep table/column references valid for the current schema.\n\n"
        f"Current user request:\n{prompt}"
    )

