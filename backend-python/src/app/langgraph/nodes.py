import logging
from typing import Dict

from app.core.config import DEFAULT_PAGE_SIZE, get_settings
from app.db.engine import engine
from app.langgraph.agent_orchestrator import call_llm_with_tools, run_agent_loop
from app.langgraph.logging_utils import log_llm_input, log_llm_output
from app.langgraph.types import AnalyzeState
from app.repositories.history_repo import (
    conversation_exists,
    create_conversation,
    get_latest_success_turns,
    save_turn,
)
from app.services.conversation_ai import generate_conversation_reply
from app.services.intent import classify_intent
from app.services.prompt_context import build_effective_prompt

logger = logging.getLogger(__name__)


def emit(state: AnalyzeState, payload: Dict[str, object]) -> None:
    if state.get("emit_event"):
        state["emit_event"](payload)


def validate_and_setup(state: AnalyzeState) -> AnalyzeState:
    settings = get_settings()
    user_prompt = (state["prompt"] or "").strip()
    if not user_prompt:
        raise ValueError("Prompt is required")

    if state["conversation_id"] and not conversation_exists(engine, state["conversation_id"]):
        raise ValueError("Conversation not found")

    conversation_id = state["conversation_id"] or create_conversation(
        engine,
        title=user_prompt[:120],
    )
    logger.info("[validate_and_setup] conversation_id=%s", conversation_id)
    state["conversation_id"] = conversation_id
    emit(state, {"type": "meta", "conversation_id": conversation_id})

    latest_turns = get_latest_success_turns(engine, conversation_id, limit=settings.max_turns_in_conversation)
    state["latest_turns"] = latest_turns
    effective_prompt = build_effective_prompt(user_prompt, latest_turns)
    state["effective_prompt"] = effective_prompt

    state["prompt_mentions_chart"] = any(
        k in (user_prompt or "").lower()
        for k in ("chart", "graph", "plot", "visual", "line", "bar", "area", "pie", "scatter", "stacked", "grouped")
    )
    state["page_size"] = DEFAULT_PAGE_SIZE
    return state


def classify_intent_node(state: AnalyzeState) -> AnalyzeState:
    log_llm_input("classify_intent", {"prompt": state["prompt"]})
    intent_type = classify_intent(state["prompt"])
    log_llm_output("classify_intent", {"intent_type": intent_type})
    state["intent_type"] = intent_type
    emit(state, {"type": "stage", "name": "intent_classified", "intent_type": intent_type})
    return state


def handle_conversation(state: AnalyzeState) -> AnalyzeState:
    log_llm_input("generate_conversation_reply", {"prompt": state["prompt"]})
    assistant_text = generate_conversation_reply(state["prompt"])
    log_llm_output("generate_conversation_reply", {"assistant_text": assistant_text})
    emit(state, {"type": "stage", "name": "assistant_ready", "assistant_text": assistant_text})

    response_blocks = [{"type": "text", "content": assistant_text}]
    state["response_blocks"] = response_blocks
    state["assistant_text"] = assistant_text
    state["sql"] = None
    state["result_columns"] = []
    state["result_rows"] = []
    state["total_count"] = None
    state["chart_intent"] = {"make_chart": False}
    state["chart_config"] = None
    return state


def skill_orchestrator(state: AnalyzeState) -> AnalyzeState:
    state["error_message"] = ""
    state["attempt"] = 0
    state["response_source"] = "llm"
    logger.info("[skill_orchestrator]")
    emit(state, {"type": "stage", "name": "searching"})
    try:
        tool_result = run_agent_loop(state["effective_prompt"] or state["prompt"], state)
    except Exception as exc:
        logger.error("[skill_orchestrator][ERROR] error=%s", str(exc), exc_info=True)
        tool_result = call_llm_with_tools(state["effective_prompt"] or state["prompt"])

    state["schema"] = tool_result.get("schema")
    state["selected_schema"] = tool_result.get("selected_schema")
    if state["selected_schema"]:
        emit(state, {"type": "stage", "name": "searching_done", "retrieved_tables": list(state["selected_schema"].keys())})
    state["sql_plan"] = tool_result.get("sql_plan")
    if state["sql_plan"]:
        emit(state, {"type": "stage", "name": "planning_done", "sql_generation_reasoning": state["sql_plan"]})
    state["sql"] = tool_result.get("sql")
    state["result_columns"] = tool_result.get("result_columns") or []
    state["result_rows"] = tool_result.get("result_rows") or []
    state["total_count"] = tool_result.get("total_count")
    state["chart_intent"] = tool_result.get("chart_intent")
    state["chart_config"] = tool_result.get("chart_config")
    state["assistant_text"] = tool_result.get("assistant_text")
    state["response_blocks"] = tool_result.get("response_blocks") or []
    state["attempt"] = int(tool_result.get("attempt") or 0)
    state["error_message"] = str(tool_result.get("error_message") or "")

    if state.get("chart_intent") is None:
        state["chart_intent"] = {"make_chart": False}
    emit(state, {"type": "stage", "name": "chart_intent_ready", "chart_intent": state["chart_intent"]})
    if state.get("chart_config"):
        emit(state, {"type": "stage", "name": "chart_ready", "chart_config": state["chart_config"]})
    emit(state, {"type": "stage", "name": "assistant_ready", "assistant_text": state.get("assistant_text")})
    return state


def save_turn_node(state: AnalyzeState) -> AnalyzeState:
    turn_id = save_turn(
        engine,
        conversation_id=state["conversation_id"],
        prompt=state["prompt"],
        context_prompt=state["effective_prompt"] if state["effective_prompt"] != state["prompt"] else None,
        sql=state["sql"],
        columns=state["result_columns"],
        data=state["result_rows"],
        chart_intent=state["chart_intent"],
        chart_config=state["chart_config"],
        assistant_text=state["assistant_text"],
        response_blocks=state["response_blocks"],
        status="success",
        error=None,
        total_count=state["total_count"],
    )
    state["turn_id"] = turn_id
    return state
