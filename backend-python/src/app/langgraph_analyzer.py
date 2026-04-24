import os
import logging
from typing import TypedDict, Optional, List, Dict, Any, Callable
from langgraph.graph import StateGraph, END
from datetime import datetime
from fastapi import HTTPException

from app.core.config import get_settings, DEFAULT_PAGE_SIZE
from app.db.engine import engine
from app.repositories.history_repo import (
    conversation_exists,
    create_conversation,
    get_latest_success_turns,
    save_turn,
)
from app.services.conversation_ai import generate_conversation_reply
from app.services.intent import classify_intent
from app.services.prompt_context import build_effective_prompt
from app.services.sql_runtime import execute_count, execute_sql, normalize_and_validate_sql
from app.skills.llm_client import generate_agent_step
from app.skills.skill_executor import execute_skill
from app.skills.skill_registry import load_tool_specs

logger = logging.getLogger(__name__)


class AnalyzeState(TypedDict):
    prompt: str
    conversation_id: Optional[str]
    emit_event: Optional[Callable[[Dict[str, Any]], None]]
    intent_type: Optional[str]
    latest_turns: Optional[List[Dict[str, Any]]]
    effective_prompt: Optional[str]
    sql: Optional[str]
    out_columns: Optional[List[str]]
    out_rows: Optional[List[Dict[str, Any]]]
    total_count: Optional[int]
    chart_intent: Optional[Dict[str, Any]]
    fig: Optional[Dict[str, Any]]
    assistant_text: Optional[str]
    response_blocks: Optional[List[Dict[str, Any]]]
    chart_only_intent: bool
    response_source: str
    prompt_cache_hit: bool
    prompt_mentions_chart: bool
    page_size: int
    schema: Optional[Dict[str, Any]]
    selected_schema: Optional[Dict[str, Any]]
    reasoning_plan: Optional[str]
    last_error_message: str
    attempt: int
    agent_steps: List[Dict[str, Any]]
    error: Optional[str]
    turn_id: Optional[str]


def emit(state: AnalyzeState, payload: Dict[str, Any]) -> None:
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
    logger.info(
        "[REQUEST] conversation_id=%s prompt=%s",
        conversation_id,
        user_prompt[:240],
    )
    state["conversation_id"] = conversation_id
    emit(state, {"type": "meta", "conversation_id": conversation_id})

    latest_turns = get_latest_success_turns(engine, conversation_id, limit=settings.max_turns_in_conversation)
    state["latest_turns"] = latest_turns
    effective_prompt = build_effective_prompt(user_prompt, latest_turns)
    state["effective_prompt"] = effective_prompt

    state["prompt_mentions_chart"] = any(k in (user_prompt or "").lower() for k in ("chart", "graph", "plot", "visual", "line", "bar", "area", "pie", "scatter", "stacked", "grouped"))
    state["page_size"] = DEFAULT_PAGE_SIZE
    return state


def classify_intent_node(state: AnalyzeState) -> AnalyzeState:
    intent_type = classify_intent(state["prompt"])
    state["intent_type"] = intent_type
    emit(state, {"type": "stage", "name": "intent_classified", "intent_type": intent_type})
    return state


def handle_conversation(state: AnalyzeState) -> AnalyzeState:
    assistant_text = generate_conversation_reply(state["prompt"])
    emit(state, {"type": "stage", "name": "assistant_ready", "assistant_text": assistant_text})

    response_blocks = [{"type": "text", "content": assistant_text}]
    state["response_blocks"] = response_blocks
    state["assistant_text"] = assistant_text
    state["sql"] = None
    state["out_columns"] = []
    state["out_rows"] = []
    state["total_count"] = None
    state["chart_intent"] = {"make_chart": False}
    state["fig"] = None
    return state


def call_llm_with_tools(prompt: str) -> Dict[str, Any]:
    """
    Centralized skill-style orchestration over existing services.
    This keeps business logic in service modules and makes LangGraph thin.
    """
    settings = get_settings()
    base_url = os.environ.get("MCP_BASE_URL", "http://127.0.0.1:8000").strip()

    baseline_context: Dict[str, Any] = {
        "prompt": prompt,
        "page_size": DEFAULT_PAGE_SIZE,
        "selected_schema": None,
        "sql": None,
        "out_columns": [],
        "out_rows": [],
        "chart_intent": None,
        "fig": None,
        "total_count": None,
    }
    baseline_state: AnalyzeState = {
        "prompt": prompt,
        "conversation_id": None,
        "emit_event": None,
        "intent_type": None,
        "latest_turns": None,
        "effective_prompt": None,
        "sql": None,
        "out_columns": None,
        "out_rows": None,
        "total_count": None,
        "chart_intent": None,
        "fig": None,
        "assistant_text": None,
        "response_blocks": None,
        "chart_only_intent": False,
        "response_source": "llm",
        "prompt_cache_hit": False,
        "prompt_mentions_chart": False,
        "page_size": DEFAULT_PAGE_SIZE,
        "schema": None,
        "selected_schema": None,
        "reasoning_plan": None,
        "last_error_message": "",
        "attempt": 0,
        "agent_steps": [],
        "error": None,
        "turn_id": None,
    }

    def _exec(skill_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        normalized_payload = normalize_tool_input(skill_name, payload, baseline_context, baseline_state)
        logger.info("[WORKFLOW] Tool called: %s", skill_name)
        try:
            result = execute_skill(skill_name, normalized_payload, base_url=base_url)
            return result
        except Exception as exc:
            logger.error("[ERROR] workflow_tool=%s error=%s", skill_name, str(exc), exc_info=True)
            raise

    # Deterministic MCP flow used as fallback baseline.
    schema_result = _exec(
        "get_schema",
        {"user_prompt": prompt, "max_tables": settings.schema_search_max_tables},
    )
    schema = schema_result.get("schema") or {}
    selected_schema = schema_result.get("selected_schema") or {}
    baseline_context["selected_schema"] = selected_schema
    sql_result = _exec(
        "generate_sql",
        {"user_prompt": prompt, "selected_schema": selected_schema},
    )
    reasoning_plan = sql_result.get("reasoning_plan")
    sql = _exec("validate_sql", {"sql": sql_result.get("sql")}).get("sql")
    baseline_context["sql"] = sql

    out_columns: List[str] = []
    out_rows: List[Dict[str, Any]] = []
    total_count: Optional[int] = None
    attempt = 0
    last_error_message = ""
    while attempt <= settings.text_to_sql_max_correction_retries:
        try:
            exe = _exec("execute_sql", {"sql": sql, "max_rows": settings.max_result_rows})
            out_columns = exe.get("columns") or []
            out_rows = exe.get("rows") or []
            total_count = exe.get("total_count")
            baseline_context["out_columns"] = out_columns
            baseline_context["out_rows"] = out_rows
            baseline_context["total_count"] = total_count
            break
        except Exception as exc:
            last_error_message = str(exc)
            logger.warning("[RETRY] attempt=%s error=%s", attempt + 1, last_error_message)
            if attempt >= settings.text_to_sql_max_correction_retries:
                raise ValueError(f"Could not generate executable SQL. Last error: {last_error_message}")
            corrected = _exec(
                "correct_sql",
                {
                    "user_prompt": prompt,
                    "selected_schema": selected_schema,
                    "invalid_sql": sql,
                    "error_message": last_error_message,
                    "reasoning_plan": reasoning_plan,
                },
            )
            sql = _exec("validate_sql", {"sql": corrected.get("sql")}).get("sql")
            baseline_context["sql"] = sql
            attempt += 1

    chart = _exec(
        "generate_chart",
        {
            "user_prompt": prompt,
            "sql": sql,
            "columns": out_columns,
            "rows": out_rows,
            "force_chart": False,
        },
    )
    chart_intent = chart.get("chart_intent") or {"make_chart": False}
    fig = chart.get("fig")
    baseline_context["chart_intent"] = chart_intent
    baseline_context["fig"] = fig

    response = _exec(
        "build_response",
        {
            "prompt": prompt,
            "sql": sql,
            "columns": out_columns,
            "rows": out_rows,
            "chart_intent": chart_intent,
            "fig": fig,
            "total_count": total_count,
            "page_size": DEFAULT_PAGE_SIZE,
        },
    )
    return {
        "schema": schema,
        "selected_schema": selected_schema,
        "reasoning_plan": reasoning_plan,
        "sql": sql,
        "out_columns": out_columns,
        "out_rows": out_rows,
        "total_count": total_count,
        "chart_intent": chart_intent,
        "fig": fig,
        "assistant_text": response.get("assistant_text"),
        "response_blocks": response.get("response_blocks") or [],
        "attempt": attempt,
        "last_error_message": last_error_message,
    }


def _fallback_agent_action(context: Dict[str, Any]) -> tuple[str, dict[str, Any]]:
    if not context.get("selected_schema"):
        return "get_schema", {"user_prompt": context["prompt"], "max_tables": context["schema_search_max_tables"]}
    if not context.get("sql"):
        return "generate_sql", {"user_prompt": context["prompt"], "selected_schema": context["selected_schema"]}
    if not context.get("validated_sql"):
        return "validate_sql", {"sql": context["sql"]}
    if not context.get("executed"):
        return "execute_sql", {"sql": context["sql"], "max_rows": context["max_result_rows"]}
    if context.get("chart_intent") is None and not context.get("chart_attempted"):
        return "generate_chart", {
            "user_prompt": context["prompt"],
            "sql": context["sql"],
            "columns": context["out_columns"],
            "rows": context["out_rows"],
            "force_chart": bool(context.get("prompt_mentions_chart")),
        }
    if context.get("chart_intent") is None:
        context["chart_intent"] = {"make_chart": False}
        context["fig"] = None
    if context.get("response_blocks") is None:
        return "build_response", {
            "prompt": context["original_prompt"],
            "sql": context["sql"],
            "columns": context["out_columns"],
            "rows": context["out_rows"],
            "chart_intent": context["chart_intent"],
            "fig": context["fig"],
            "total_count": context["total_count"],
            "page_size": context["page_size"],
        }
    return "final_answer", {}


def normalize_tool_input(
    action: str,
    action_input: Dict[str, Any],
    context: Dict[str, Any],
    state: AnalyzeState,
) -> Dict[str, Any]:
    payload = action_input if isinstance(action_input, dict) else {}

    if action == "generate_sql":
        user_prompt = (
            payload.get("user_request")
            or payload.get("user_prompt")
            or state.get("prompt")
            or context.get("prompt")
            or ""
        )
        return {
            "user_prompt": str(user_prompt).strip(),
            "selected_schema": payload.get("schema")
            or payload.get("selected_schema")
            or context.get("selected_schema"),
        }

    if action == "generate_chart":
        return {
            "user_prompt": state.get("prompt") or context.get("prompt") or "",
            "sql": context.get("sql"),
            "columns": context.get("out_columns"),
            "rows": context.get("out_rows"),
            "force_chart": True,
        }

    if action == "build_response":
        return {
            "prompt": state.get("prompt") or context.get("prompt") or "",
            "sql": context.get("sql"),
            "columns": context.get("out_columns"),
            "rows": context.get("out_rows"),
            "chart_intent": context.get("chart_intent"),
            "fig": context.get("fig"),
            "total_count": context.get("total_count"),
            "page_size": context.get("page_size"),
        }

    if action == "get_schema":
        user_prompt = payload.get("user_prompt") or context.get("prompt") or state.get("prompt") or ""
        return {
            **payload,
            "user_prompt": str(user_prompt).strip(),
        }

    if "user_prompt" in payload and not str(payload.get("user_prompt") or "").strip():
        fallback_prompt = state.get("prompt") or context.get("prompt") or ""
        return {
            **payload,
            "user_prompt": str(fallback_prompt).strip(),
        }

    return payload


def run_agent_loop(prompt: str, state: AnalyzeState) -> dict[str, Any]:
    settings = get_settings()
    base_url = os.environ.get("MCP_BASE_URL", "http://127.0.0.1:8000").strip()
    tools = load_tool_specs()
    max_steps = 6

    context: Dict[str, Any] = {
        "prompt": prompt,
        "original_prompt": state["prompt"],
        "page_size": state["page_size"],
        "schema_search_max_tables": settings.schema_search_max_tables,
        "max_result_rows": settings.max_result_rows,
        "schema": None,
        "selected_schema": None,
        "reasoning_plan": None,
        "sql": None,
        "validated_sql": False,
        "out_columns": [],
        "out_rows": [],
        "total_count": None,
        "chart_intent": None,
        "fig": None,
        "assistant_text": None,
        "response_blocks": None,
        "attempt": 0,
        "last_error_message": "",
        "executed": False,
        "chart_attempted": False,
    }

    for _ in range(max_steps):
        decision = generate_agent_step(
            user_prompt=prompt,
            tools=tools,
            steps=state["agent_steps"],
            max_steps=max_steps,
        )
        thought = str(decision.get("thought") or "").strip()
        action = str(decision.get("action") or "").strip()
        action_input = decision.get("input") if isinstance(decision.get("input"), dict) else {}

        if not action:
            action, action_input = _fallback_agent_action(context)
            thought = thought or f"Fallback action selected: {action}"

        if action == "generate_chart" and context.get("chart_attempted"):
            action = "build_response"
            action_input = {}
            thought = thought or "Chart generation already attempted; moving to response assembly."

        emit(state, {"type": "agent_thought", "thought": thought})
        logger.info(
            "[AGENT] conversation_id=%s action=%s",
            state.get("conversation_id"),
            action,
        )
        emit(state, {"type": "agent_action", "action": action})

        if action == "final_answer":
            output = decision.get("output") if isinstance(decision.get("output"), dict) else {}
            if output.get("sql"):
                context["sql"] = output.get("sql")
            if isinstance(output.get("data"), list):
                context["out_rows"] = output.get("data")
            break

        observation: Dict[str, Any]
        try:
            normalized_input = normalize_tool_input(action, action_input, context, state)
            logger.info("[WORKFLOW] Tool called: %s", action)
            observation = execute_skill(action, normalized_input, base_url=base_url)
            action_input = normalized_input
        except Exception as exc:
            logger.error(
                "[ERROR] conversation_id=%s action=%s skill_error=%s",
                state.get("conversation_id"),
                action,
                str(exc),
                exc_info=True,
            )
            # If agent chooses an invalid action or malformed input, take deterministic next step.
            fallback_action, fallback_input = _fallback_agent_action(context)
            if fallback_action == "final_answer":
                break
            normalized_fallback_input = normalize_tool_input(fallback_action, fallback_input, context, state)
            logger.info("[WORKFLOW] Tool called: %s", fallback_action)
            observation = execute_skill(fallback_action, normalized_fallback_input, base_url=base_url)
            action = fallback_action
            action_input = normalized_fallback_input

        emit(state, {"type": "agent_observation", "result": observation})
        state["agent_steps"].append(
            {
                "thought": thought,
                "action": action,
                "input": action_input,
                "observation": observation,
            }
        )

        if action == "get_schema":
            context["schema"] = observation.get("schema")
            context["selected_schema"] = observation.get("selected_schema")
        elif action == "generate_sql":
            context["sql"] = observation.get("sql")
            context["reasoning_plan"] = observation.get("reasoning_plan")
            context["validated_sql"] = False
        elif action == "validate_sql":
            context["sql"] = observation.get("sql")
            context["validated_sql"] = True
        elif action == "execute_sql":
            context["out_columns"] = observation.get("columns") or []
            context["out_rows"] = observation.get("rows") or []
            context["total_count"] = observation.get("total_count")
            context["executed"] = True
        elif action == "correct_sql":
            context["sql"] = observation.get("sql")
            context["validated_sql"] = False
            context["attempt"] = int(context["attempt"]) + 1
        elif action == "generate_chart":
            context["chart_attempted"] = True
            chart_intent = observation.get("chart_intent")
            if not isinstance(chart_intent, dict):
                chart_intent = {"make_chart": bool(observation.get("make_chart"))}
            context["chart_intent"] = chart_intent or {"make_chart": False}
            context["fig"] = observation.get("fig")
        elif action == "build_response":
            context["assistant_text"] = observation.get("assistant_text")
            context["response_blocks"] = observation.get("response_blocks") or []
            break

    # Safety completion: make sure required fields are assembled.
    if context["response_blocks"] is None and context.get("sql"):
        if context["chart_intent"] is None and not context.get("chart_attempted"):
            chart_payload = normalize_tool_input("generate_chart", {
                "user_prompt": prompt,
                "sql": context["sql"],
                "columns": context["out_columns"],
                "rows": context["out_rows"],
                "force_chart": bool(state["prompt_mentions_chart"]),
            }, context, state)
            logger.info("[WORKFLOW] Tool called: generate_chart")
            chart = execute_skill(
                "generate_chart",
                chart_payload,
                base_url=base_url,
            )
            context["chart_attempted"] = True
            chart_intent = chart.get("chart_intent")
            if not isinstance(chart_intent, dict):
                chart_intent = {"make_chart": bool(chart.get("make_chart"))}
            context["chart_intent"] = chart_intent or {"make_chart": False}
            context["fig"] = chart.get("fig")
        elif context["chart_intent"] is None:
            context["chart_intent"] = {"make_chart": False}
            context["fig"] = None

        build_payload = normalize_tool_input("build_response", {
            "prompt": state["prompt"],
            "sql": context["sql"],
            "columns": context["out_columns"],
            "rows": context["out_rows"],
            "chart_intent": context["chart_intent"] or {"make_chart": False},
            "fig": context["fig"],
            "total_count": context["total_count"],
            "page_size": state["page_size"],
        }, context, state)
        logger.info("[WORKFLOW] Tool called: build_response")
        built = execute_skill(
            "build_response",
            build_payload,
            base_url=base_url,
        )
        context["assistant_text"] = built.get("assistant_text")
        context["response_blocks"] = built.get("response_blocks") or []

    return {
        "schema": context.get("schema"),
        "selected_schema": context.get("selected_schema"),
        "reasoning_plan": context.get("reasoning_plan"),
        "sql": context.get("sql"),
        "out_columns": context.get("out_columns") or [],
        "out_rows": context.get("out_rows") or [],
        "total_count": context.get("total_count"),
        "chart_intent": context.get("chart_intent") or {"make_chart": False},
        "fig": context.get("fig"),
        "assistant_text": context.get("assistant_text"),
        "response_blocks": context.get("response_blocks") or [],
        "attempt": int(context.get("attempt") or 0),
        "last_error_message": str(context.get("last_error_message") or ""),
    }


def skill_orchestrator(state: AnalyzeState) -> AnalyzeState:
    settings = get_settings()
    latest_turn = state["latest_turns"][0] if state["latest_turns"] else None
    state["last_error_message"] = ""
    state["attempt"] = 0

    prompt_cache_turn = None
    # if settings.reuse_sql_from_history_by_prompt:
    #     prompt_cache_turn = find_latest_success_by_prompt(engine, state["prompt"])
    #     state["prompt_cache_hit"] = bool(
    #         prompt_cache_turn
    #         and prompt_cache_turn.get("sql")
    #         and isinstance(prompt_cache_turn.get("columns"), list)
    #         and isinstance(prompt_cache_turn.get("data"), list)
    #     )
    # else:
    #     state["prompt_cache_hit"] = False

    # state["chart_only_intent"] = is_chart_only_prompt(state["prompt"]) and bool(
    #     latest_turn
    #     and latest_turn.get("sql")
    #     and isinstance(latest_turn.get("columns"), list)
    #     and isinstance(latest_turn.get("data"), list)
    # )
    state["prompt_cache_hit"] = False
    state["chart_only_intent"] = False
    if state["chart_only_intent"]:
        state["sql"] = str(latest_turn.get("sql") or "")
        state["out_columns"] = latest_turn.get("columns") or []
        state["out_rows"] = latest_turn.get("data") or []
        state["total_count"] = latest_turn.get("total_count") or len(state["out_rows"])
        emit(state, {"type": "stage", "name": "reused_previous_result", "columns": state["out_columns"], "row_count": len(state["out_rows"]), "total_count": state["total_count"], "preview_rows": state["out_rows"][:state["page_size"]], "page": 1, "page_size": state["page_size"]})
        state["response_source"] = "history_cache" if state["prompt_cache_hit"] else "previous_result"
    elif state["prompt_cache_hit"] and prompt_cache_turn:
        state["response_source"] = "history_cache"
        state["sql"] = normalize_and_validate_sql(str(prompt_cache_turn.get("sql") or ""))
        emit(state, {"type": "stage", "name": "prompt_cache_hit", "sql": state["sql"]})
        state["total_count"] = execute_count(engine=engine, base_sql=state["sql"])
        state["out_columns"], state["out_rows"] = execute_sql(engine=engine, sql=state["sql"], max_rows=settings.max_result_rows)
        emit(state, {"type": "stage", "name": "query_executed", "sql": state["sql"], "columns": state["out_columns"], "row_count": len(state["out_rows"]), "total_count": state["total_count"], "preview_rows": state["out_rows"][:state["page_size"]], "page": 1, "page_size": state["page_size"]})
    else:
        state["response_source"] = "llm"
        logger.info(
            "[REQUEST] conversation_id=%s prompt=%s source=llm",
            state.get("conversation_id"),
            (state.get("prompt") or "")[:240],
        )
        emit(state, {"type": "stage", "name": "searching"})
        try:
            tool_result = run_agent_loop(state["effective_prompt"] or state["prompt"], state)
        except Exception as exc:
            logger.error(
                "[ERROR] conversation_id=%s agent_loop_failed=%s",
                state.get("conversation_id"),
                str(exc),
                exc_info=True,
            )
            # Fallback to baseline deterministic flow.
            tool_result = call_llm_with_tools(state["effective_prompt"] or state["prompt"])
        state["schema"] = tool_result.get("schema")
        state["selected_schema"] = tool_result.get("selected_schema")
        if state["selected_schema"]:
            emit(state, {"type": "stage", "name": "searching_done", "retrieved_tables": list(state["selected_schema"].keys())})
        state["reasoning_plan"] = tool_result.get("reasoning_plan")
        if state["reasoning_plan"]:
            emit(state, {"type": "stage", "name": "planning_done", "sql_generation_reasoning": state["reasoning_plan"]})
        state["sql"] = tool_result.get("sql")
        state["out_columns"] = tool_result.get("out_columns") or []
        state["out_rows"] = tool_result.get("out_rows") or []
        state["total_count"] = tool_result.get("total_count")
        state["chart_intent"] = tool_result.get("chart_intent")
        state["fig"] = tool_result.get("fig")
        state["assistant_text"] = tool_result.get("assistant_text")
        state["response_blocks"] = tool_result.get("response_blocks") or []
        state["attempt"] = int(tool_result.get("attempt") or 0)
        state["last_error_message"] = str(tool_result.get("last_error_message") or "")

    if state.get("chart_intent") is None:
        state["chart_intent"] = {"make_chart": False}
    emit(state, {"type": "stage", "name": "chart_intent_ready", "chart_intent": state["chart_intent"]})
    if state.get("fig"):
        emit(state, {"type": "stage", "name": "chart_ready", "chart_config": state["fig"]})
    emit(state, {"type": "stage", "name": "assistant_ready", "assistant_text": state.get("assistant_text")})
    return state


def save_turn_node(state: AnalyzeState) -> AnalyzeState:
    save_turn(
        engine,
        conversation_id=state["conversation_id"],
        prompt=state["prompt"],
        context_prompt=state["effective_prompt"] if state["effective_prompt"] != state["prompt"] else None,
        sql=state["sql"],
        columns=state["out_columns"],
        data=state["out_rows"],
        chart_intent=state["chart_intent"],
        chart_config=state["fig"],
        assistant_text=state["assistant_text"],
        response_blocks=state["response_blocks"],
        status="success",
        error=None,
        total_count=state["total_count"],
    )
    return state


# Build the graph
graph = StateGraph(AnalyzeState)

graph.add_node("validate", validate_and_setup)
graph.add_node("classify", classify_intent_node)
graph.add_node("conversation", handle_conversation)
graph.add_node("skill_orchestrator", skill_orchestrator)
graph.add_node("save", save_turn_node)

graph.set_entry_point("validate")

graph.add_edge("validate", "classify")

def route_after_classify(state: AnalyzeState):
    if state["intent_type"] == "CONVERSATION":
        return "conversation"
    else:
        return "skill_orchestrator"

graph.add_conditional_edges("classify", route_after_classify)

graph.add_edge("conversation", "save")
graph.add_edge("skill_orchestrator", "save")

graph.set_finish_point("save")

compiled_graph = graph.compile()


def analyze_with_langgraph(
    *,
    prompt: str,
    conversation_id: Optional[str] = None,
    emit_event: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    initial_state: AnalyzeState = {
        "prompt": prompt,
        "conversation_id": conversation_id,
        "emit_event": emit_event,
        "intent_type": None,
        "latest_turns": None,
        "effective_prompt": None,
        "sql": None,
        "out_columns": None,
        "out_rows": None,
        "total_count": None,
        "chart_intent": None,
        "fig": None,
        "assistant_text": None,
        "response_blocks": None,
        "chart_only_intent": False,
        "response_source": "llm",
        "prompt_cache_hit": False,
        "prompt_mentions_chart": False,
        "page_size": DEFAULT_PAGE_SIZE,
        "schema": None,
        "selected_schema": None,
        "reasoning_plan": None,
        "last_error_message": "",
        "attempt": 0,
        "agent_steps": [],
        "error": None,
    }

    try:
        final_state = compiled_graph.invoke(initial_state)
        logger.info(
            "[WORKFLOW] Finalized conversation_id=%s intent=%s status=success",
            final_state.get("conversation_id"),
            final_state.get("intent_type"),
        )
        return {
            "conversation_id": final_state["conversation_id"],
            "turn_id": "generated",  # Placeholder, since save_turn returns it, but in graph we don't capture
            "prompt": final_state["prompt"],
            "intent_type": final_state["intent_type"],
            "sql": final_state["sql"] if not final_state["chart_only_intent"] else None,
            "columns": (final_state["out_columns"] or []) if not final_state["chart_only_intent"] else [],
            "data": (final_state["out_rows"][:final_state["page_size"]] if final_state["out_rows"] else []) if not final_state["chart_only_intent"] else [],
            "total_count": final_state["total_count"],
            "chart_intent": final_state["chart_intent"] or {"make_chart": False},
            "chart_config": final_state["fig"],
            "assistant_text": final_state["assistant_text"],
            "response_blocks": final_state["response_blocks"] or [],
            "status": "success",
            "source": final_state["response_source"],
            "created_at": datetime.utcnow().isoformat(),
        }
    except Exception as exc:
        # Handle errors similarly
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