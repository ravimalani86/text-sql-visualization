import logging
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from app.langgraph.graph import compiled_graph
from app.langgraph.state_factory import create_initial_state

logger = logging.getLogger(__name__)


def analyze_with_langgraph(
    *,
    prompt: str,
    conversation_id: Optional[str] = None,
    emit_event: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    initial_state = create_initial_state(
        prompt=prompt,
        conversation_id=conversation_id,
        emit_event=emit_event,
    )

    try:
        final_state = compiled_graph.invoke(initial_state)
        logger.info(
            "[WORKFLOW] Finalized conversation_id=%s intent=%s status=success",
            final_state.get("conversation_id"),
            final_state.get("intent_type"),
        )
        return {
            "conversation_id": final_state["conversation_id"],
            "turn_id": final_state.get("turn_id"),
            "prompt": final_state["prompt"],
            "intent_type": final_state["intent_type"],
            "sql": final_state["sql"],
            "columns": final_state["result_columns"] or [],
            "data": final_state["result_rows"][: final_state["page_size"]] if final_state["result_rows"] else [],
            "total_count": final_state["total_count"],
            "chart_intent": final_state["chart_intent"] or {"make_chart": False},
            "chart_config": final_state["chart_config"],
            "assistant_text": final_state["assistant_text"],
            "response_blocks": final_state["response_blocks"] or [],
            "status": "success",
            "source": final_state["response_source"],
            "created_at": datetime.utcnow().isoformat(),
        }
    except Exception as exc:
        return {
            "conversation_id": conversation_id,
            "turn_id": None,
            "prompt": prompt,
            "intent_type": "DATA_QUERY",
            "sql": None,
            "columns": [],
            "data": [],
            "chart_intent": {"make_chart": False},
            "chart_config": None,
            "assistant_text": None,
            "response_blocks": [],
            "status": "failed",
            "source": "llm",
            "created_at": datetime.utcnow().isoformat(),
            "error": str(exc),
        }
