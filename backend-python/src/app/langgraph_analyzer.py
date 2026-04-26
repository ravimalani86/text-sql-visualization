import os
import json
import logging
from typing import TypedDict, Optional, List, Dict, Any, Callable
from langgraph.graph import StateGraph
from datetime import datetime

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
from app.skills.llm_client import generate_agent_step
from app.skills.skill_executor import execute_skill
from app.skills.skill_registry import load_tool_specs

logger = logging.getLogger(__name__)
LLM_LOG_MAX_CHARS = 4000


def _truncate_for_log(value: Any, max_chars: int = LLM_LOG_MAX_CHARS) -> str:
    try:
        text = json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        text = repr(value)
    if len(text) > max_chars:
        return f"{text[:max_chars]}... [truncated {len(text) - max_chars} chars]"
    return text


def _log_llm_input(llm_name: str, payload: Dict[str, Any]) -> None:
    logger.info("[LLM][INPUT] model=%s payload=%s", llm_name, _truncate_for_log(payload))


def _log_llm_output(llm_name: str, payload: Any) -> None:
    logger.info("[LLM][OUTPUT] model=%s payload=%s", llm_name, _truncate_for_log(payload))


class AnalyzeState(TypedDict):
    prompt: str
    conversation_id: Optional[str]
    emit_event: Optional[Callable[[Dict[str, Any]], None]]
    intent_type: Optional[str]
    latest_turns: Optional[List[Dict[str, Any]]]
    effective_prompt: Optional[str]
    sql: Optional[str]
    result_columns: Optional[List[str]]
    result_rows: Optional[List[Dict[str, Any]]]
    total_count: Optional[int]
    chart_intent: Optional[Dict[str, Any]]
    chart_config: Optional[Dict[str, Any]]
    assistant_text: Optional[str]
    response_blocks: Optional[List[Dict[str, Any]]]
    response_source: str
    prompt_mentions_chart: bool
    page_size: int
    schema: Optional[Dict[str, Any]]
    selected_schema: Optional[Dict[str, Any]]
    sql_plan: Optional[str]
    error_message: str
    attempt: int
    agent_steps: List[Dict[str, Any]]
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
    logger.info("[validate_and_setup] conversation_id=%s", conversation_id)
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
    _log_llm_input("classify_intent", {"prompt": state["prompt"]})
    intent_type = classify_intent(state["prompt"])
    _log_llm_output("classify_intent", {"intent_type": intent_type})
    state["intent_type"] = intent_type
    emit(state, {"type": "stage", "name": "intent_classified", "intent_type": intent_type})
    return state


def handle_conversation(state: AnalyzeState) -> AnalyzeState:
    _log_llm_input("generate_conversation_reply", {"prompt": state["prompt"]})
    assistant_text = generate_conversation_reply(state["prompt"])
    _log_llm_output("generate_conversation_reply", {"assistant_text": assistant_text})
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


def call_llm_with_tools(prompt: str) -> Dict[str, Any]:
    """
    Centralized skill-style orchestration over existing services.
    This keeps business logic in service modules and makes LangGraph thin.
    """
    settings = get_settings()
    base_url = os.environ.get("MCP_BASE_URL", "").strip()

    baseline_context: Dict[str, Any] = {
        "prompt": prompt,
        "page_size": DEFAULT_PAGE_SIZE,
        "selected_schema": None,
        "sql": None,
        "result_columns": [],
        "result_rows": [],
        "chart_intent": None,
        "chart_config": None,
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

    def _exec(skill_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        normalized_payload = normalize_tool_input(skill_name, payload, baseline_context, baseline_state)
        logger.info("[WORKFLOW] Tool called: skill_name=%s", skill_name)
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
    sql_plan = sql_result.get("reasoning_plan")
    sql = _exec("validate_sql", {"sql": sql_result.get("sql")}).get("sql")
    baseline_context["sql"] = sql

    result_columns: List[str] = []
    result_rows: List[Dict[str, Any]] = []
    total_count: Optional[int] = None
    attempt = 0
    error_message = ""
    while attempt <= settings.text_to_sql_max_correction_retries:
        try:
            exe = _exec("execute_sql", {"sql": sql, "max_rows": settings.max_result_rows})
            result_columns = exe.get("columns") or []
            result_rows = exe.get("rows") or []
            total_count = exe.get("total_count")
            baseline_context["result_columns"] = result_columns
            baseline_context["result_rows"] = result_rows
            baseline_context["total_count"] = total_count
            break
        except Exception as exc:
            error_message = str(exc)
            logger.warning("[RETRY] attempt=%s error=%s", attempt + 1, error_message)
            if attempt >= settings.text_to_sql_max_correction_retries:
                raise ValueError(f"Could not generate executable SQL. Last error: {error_message}")
            corrected = _exec(
                "correct_sql",
                {
                    "user_prompt": prompt,
                    "selected_schema": selected_schema,
                    "invalid_sql": sql,
                    "error_message": error_message,
                    "reasoning_plan": sql_plan,
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
            "columns": result_columns,
            "rows": result_rows,
            "force_chart": False,
        },
    )
    chart_intent = chart.get("chart_intent") or {"make_chart": False}
    chart_config = chart.get("fig")
    baseline_context["chart_intent"] = chart_intent
    baseline_context["chart_config"] = chart_config

    response = _exec(
        "build_response",
        {
            "prompt": prompt,
            "sql": sql,
            "columns": result_columns,
            "rows": result_rows,
            "chart_intent": chart_intent,
            "fig": chart_config,
            "total_count": total_count,
            "page_size": DEFAULT_PAGE_SIZE,
        },
    )
    return {
        "schema": schema,
        "selected_schema": selected_schema,
        "sql_plan": sql_plan,
        "sql": sql,
        "result_columns": result_columns,
        "result_rows": result_rows,
        "total_count": total_count,
        "chart_intent": chart_intent,
        "chart_config": chart_config,
        "assistant_text": response.get("assistant_text"),
        "response_blocks": response.get("response_blocks") or [],
        "attempt": attempt,
        "error_message": error_message,
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
            "columns": context["result_columns"],
            "rows": context["result_rows"],
            "force_chart": bool(context.get("prompt_mentions_chart")),
        }
    if context.get("chart_intent") is None:
        context["chart_intent"] = {"make_chart": False}
        context["chart_config"] = None
    if context.get("response_blocks") is None:
        return "build_response", {
            "prompt": context["original_prompt"],
            "sql": context["sql"],
            "columns": context["result_columns"],
            "rows": context["result_rows"],
            "chart_intent": context["chart_intent"],
            "chart_config": context["chart_config"],
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
            "columns": context.get("result_columns"),
            "rows": context.get("result_rows"),
            "force_chart": True,
        }

    if action == "build_response":
        return {
            "prompt": state.get("prompt") or context.get("prompt") or "",
            "sql": context.get("sql"),
            "columns": context.get("result_columns"),
            "rows": context.get("result_rows"),
            "chart_intent": context.get("chart_intent"),
            "fig": context.get("chart_config"),
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
    base_url = os.environ.get("MCP_BASE_URL", "").strip()
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
        "sql_plan": None,
        "sql": None,
        "validated_sql": False,
        "result_columns": [],
        "result_rows": [],
        "total_count": None,
        "chart_intent": None,
        "chart_config": None,
        "assistant_text": None,
        "response_blocks": None,
        "attempt": 0,
        "error_message": "",
        "executed": False,
        "chart_attempted": False,
    }

    for _ in range(max_steps):
        agent_payload = {
            "user_prompt": prompt,
            "tools_count": len(tools),
            "steps_count": len(state["agent_steps"]),
            "max_steps": max_steps,
        }
        _log_llm_input("generate_agent_step", agent_payload)
        try:
            decision = generate_agent_step(
                user_prompt=prompt,
                tools=tools,
                steps=state["agent_steps"],
                max_steps=max_steps,
            )
        except Exception as exc:
            logger.error("[LLM][ERROR] model=generate_agent_step error=%s", str(exc), exc_info=True)
            raise
        _log_llm_output("generate_agent_step", decision)
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
        emit(state, {"type": "agent_action", "action": action})

        if action == "final_answer":
            output = decision.get("output") if isinstance(decision.get("output"), dict) else {}
            if output.get("sql"):
                context["sql"] = output.get("sql")
            if isinstance(output.get("data"), list):
                context["result_rows"] = output.get("data")
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
            context["sql_plan"] = observation.get("reasoning_plan")
            context["validated_sql"] = False
        elif action == "validate_sql":
            context["sql"] = observation.get("sql")
            context["validated_sql"] = True
        elif action == "execute_sql":
            context["result_columns"] = observation.get("columns") or []
            context["result_rows"] = observation.get("rows") or []
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
            context["chart_config"] = observation.get("fig")
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
                "columns": context["result_columns"],
                "rows": context["result_rows"],
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
            context["chart_config"] = chart.get("fig")
        elif context["chart_intent"] is None:
            context["chart_intent"] = {"make_chart": False}
            context["chart_config"] = None

        build_payload = normalize_tool_input("build_response", {
            "prompt": state["prompt"],
            "sql": context["sql"],
            "columns": context["result_columns"],
            "rows": context["result_rows"],
            "chart_intent": context["chart_intent"] or {"make_chart": False},
            "fig": context["chart_config"],
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
        "sql_plan": context.get("sql_plan"),
        "sql": context.get("sql"),
        "result_columns": context.get("result_columns") or [],
        "result_rows": context.get("result_rows") or [],
        "total_count": context.get("total_count"),
        "chart_intent": context.get("chart_intent") or {"make_chart": False},
        "chart_config": context.get("chart_config"),
        "assistant_text": context.get("assistant_text"),
        "response_blocks": context.get("response_blocks") or [],
        "attempt": int(context.get("attempt") or 0),
        "error_message": str(context.get("error_message") or ""),
    }


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
        # Fallback to baseline deterministic flow.
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
            "data": final_state["result_rows"][:final_state["page_size"]] if final_state["result_rows"] else [],
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
