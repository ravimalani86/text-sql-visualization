from typing import Any, Callable, Dict, Optional

from app.core.config import DEFAULT_PAGE_SIZE
from app.langgraph.types import AnalyzeState


def create_initial_state(
    *,
    prompt: str,
    conversation_id: Optional[str],
    emit_event: Optional[Callable[[Dict[str, Any]], None]],
) -> AnalyzeState:
    return {
        "prompt": prompt,
        "conversation_id": conversation_id,
        "emit_event": emit_event,
        "intent_type": None,
        "latest_turns": None,
        "effective_prompt": None,
        "sql": None,
        "result_columns": None,
        "result_rows": None,
        "total_count": None,
        "chart_intent": None,
        "chart_config": None,
        "assistant_text": None,
        "response_blocks": None,
        "response_source": "llm",
        "prompt_mentions_chart": False,
        "page_size": DEFAULT_PAGE_SIZE,
        "schema": None,
        "selected_schema": None,
        "sql_plan": None,
        "error_message": "",
        "attempt": 0,
        "agent_steps": [],
        "turn_id": None,
    }


def create_baseline_state(prompt: str) -> AnalyzeState:
    return create_initial_state(prompt=prompt, conversation_id=None, emit_event=None)
