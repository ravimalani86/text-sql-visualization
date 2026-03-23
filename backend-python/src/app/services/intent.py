from __future__ import annotations


def is_chart_only_prompt(prompt: str) -> bool:
    text_low = (prompt or "").strip().lower()
    if not text_low:
        return False

    chart_words = [
        "chart",
        "graph",
        "plot",
        "visual",
        "bar",
        "line",
        "pie",
        "area",
        "scatter",
        "horizontal",
        "stacked",
        "grouped",
    ]
    data_change_words = [
        "total",
        "sum",
        "avg",
        "average",
        "count",
        "max",
        "min",
        "top",
        "bottom",
        "where",
        "filter",
        "between",
        "before",
        "after",
        "2024",
        "2025",
        "2026",
        "month",
        "year",
        "product",
        "category",
        "region",
        "customer",
        "sales",
        "revenue",
        "profit",
        "group by",
        "order by",
    ]

    has_chart_word = any(w in text_low for w in chart_words)
    has_data_change_word = any(w in text_low for w in data_change_words)
    return has_chart_word and not has_data_change_word


def classify_intent(prompt: str) -> str:
    text_low = (prompt or "").strip().lower()
    if not text_low:
        return "CONVERSATION"

    conversation_phrases = {
        "hi",
        "hello",
        "hey",
        "how are you",
        "thanks",
        "thank you",
        "good morning",
        "good afternoon",
        "good evening",
        "bye",
    }

    data_words = [
        "data",
        "database",
        "sql",
        "query",
        "table",
        "report",
        "analytics",
        "analysis",
        "sales",
        "revenue",
        "profit",
        "chart",
        "graph",
        "plot",
        "top",
        "count",
        "sum",
        "average",
        "avg",
        "total",
        "group by",
        "order by",
        "show me",
        "list",
        "month",
        "year",
        "customer",
        "product",
        "category",
        "region",
    ]

    has_data_word = any(w in text_low for w in data_words)
    if not has_data_word:
        if text_low in conversation_phrases:
            return "CONVERSATION"
        if len(text_low.split()) <= 4 and any(p in text_low for p in conversation_phrases):
            return "CONVERSATION"

    return "DATA_QUERY" if has_data_word else "CONVERSATION"

