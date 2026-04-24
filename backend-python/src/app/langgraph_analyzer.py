from typing import TypedDict, Optional, List, Dict, Any, Callable
from langgraph.graph import StateGraph, END
from datetime import datetime
from fastapi import HTTPException

from app.core.config import get_settings, DEFAULT_PAGE_SIZE
from app.db.engine import engine
from app.repositories.history_repo import (
    conversation_exists,
    create_conversation,
    find_latest_success_by_prompt,
    get_latest_success_turns,
    save_turn,
)
from app.services.chart_intent_ai import suggest_chart_intent
from app.services.conversation_ai import generate_conversation_reply
from app.services.intent import classify_intent, is_chart_only_prompt
from app.services.plotly_mapper import build_plotly_figure, normalize_chart_config
from app.services.prompt_context import build_effective_prompt
from app.services.response_builder import build_assistant_text, build_response_blocks
from app.services.schema_cache import get_cached_schema
from app.services.schema_selector import select_relevant_schema
from app.services.sql_planner import generate_sql_plan
from app.services.sql_generator import correct_sql, text_to_sql
from app.services.sql_runtime import execute_count, execute_sql, normalize_and_validate_sql


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


def check_cache_and_reuse(state: AnalyzeState) -> AnalyzeState:
    settings = get_settings()
    latest_turn = state["latest_turns"][0] if state["latest_turns"] else None

    if settings.reuse_sql_from_history_by_prompt:
        prompt_cache_turn = find_latest_success_by_prompt(engine, state["prompt"])
        state["prompt_cache_hit"] = bool(
            prompt_cache_turn
            and prompt_cache_turn.get("sql")
            and isinstance(prompt_cache_turn.get("columns"), list)
            and isinstance(prompt_cache_turn.get("data"), list)
        )
    else:
        prompt_cache_turn = None
        state["prompt_cache_hit"] = False

    state["chart_only_intent"] = is_chart_only_prompt(state["prompt"]) and bool(
        latest_turn
        and latest_turn.get("sql")
        and isinstance(latest_turn.get("columns"), list)
        and isinstance(latest_turn.get("data"), list)
    )

    if state["chart_only_intent"]:
        state["sql"] = str(latest_turn.get("sql") or "")
        state["out_columns"] = latest_turn.get("columns") or []
        state["out_rows"] = latest_turn.get("data") or []
        state["total_count"] = latest_turn.get("total_count") or len(state["out_rows"])
        emit(state, {"type": "stage", "name": "reused_previous_result", "columns": state["out_columns"], "row_count": len(state["out_rows"]), "total_count": state["total_count"], "preview_rows": state["out_rows"][:state["page_size"]], "page": 1, "page_size": state["page_size"]})
        state["response_source"] = "history_cache" if state["prompt_cache_hit"] else "previous_result"
    elif state["prompt_cache_hit"]:
        state["response_source"] = "history_cache"
        state["sql"] = str(prompt_cache_turn.get("sql") or "")
        state["sql"] = normalize_and_validate_sql(state["sql"])
        emit(state, {"type": "stage", "name": "prompt_cache_hit", "sql": state["sql"]})
        state["total_count"] = execute_count(engine=engine, base_sql=state["sql"])
        state["out_columns"], state["out_rows"] = execute_sql(engine=engine, sql=state["sql"], max_rows=settings.max_result_rows)
        emit(state, {"type": "stage", "name": "query_executed", "sql": state["sql"], "columns": state["out_columns"], "row_count": len(state["out_rows"]), "total_count": state["total_count"], "preview_rows": state["out_rows"][:state["page_size"]], "page": 1, "page_size": state["page_size"]})
    else:
        state["response_source"] = "llm"
        state["schema"] = get_cached_schema()
        if not state["schema"]:
            raise ValueError("No tables found in database")

        emit(state, {"type": "stage", "name": "searching", "table_count": len(state["schema"])})
        state["selected_schema"] = select_relevant_schema(
            user_prompt=state["effective_prompt"],
            schema=state["schema"],
            max_tables=settings.schema_search_max_tables,
        )
        emit(state, {"type": "stage", "name": "searching_done", "retrieved_tables": list(state["selected_schema"].keys())})

        if settings.enable_sql_planning:
            emit(state, {"type": "stage", "name": "planning"})
            state["reasoning_plan"] = generate_sql_plan(
                user_prompt=state["effective_prompt"],
                schema=state["selected_schema"],
            )
            emit(state, {"type": "stage", "name": "planning_done", "sql_generation_reasoning": state["reasoning_plan"]})

        emit(state, {"type": "stage", "name": "generating"})
        state["sql"] = text_to_sql(
            state["effective_prompt"],
            state["selected_schema"],
            reasoning_plan=state["reasoning_plan"],
        )
        state["sql"] = normalize_and_validate_sql(state["sql"])
        emit(state, {"type": "stage", "name": "sql_generated", "sql": state["sql"], "attempt": 0})

    return state


def execute_and_correct_sql(state: AnalyzeState) -> AnalyzeState:
    settings = get_settings()
    state["last_error_message"] = ""
    state["attempt"] = 0

    while state["attempt"] <= settings.text_to_sql_max_correction_retries:
        try:
            state["total_count"] = execute_count(engine=engine, base_sql=state["sql"])
            state["out_columns"], state["out_rows"] = execute_sql(
                engine=engine,
                sql=state["sql"],
                max_rows=settings.max_result_rows,
            )
            emit(state, {"type": "stage", "name": "query_executed", "sql": state["sql"], "columns": state["out_columns"], "row_count": len(state["out_rows"]), "total_count": state["total_count"], "preview_rows": state["out_rows"][:state["page_size"]], "page": 1, "page_size": state["page_size"]})
            break
        except Exception as exc:
            state["last_error_message"] = str(exc)
            if state["attempt"] >= settings.text_to_sql_max_correction_retries:
                raise ValueError(f"Could not generate executable SQL. Last error: {state['last_error_message']}")
            emit(state, {"type": "stage", "name": "correcting", "attempt": state["attempt"] + 1, "invalid_sql": state["sql"], "error": state["last_error_message"]})
            state["sql"] = correct_sql(
                user_prompt=state["effective_prompt"],
                schema=state["selected_schema"],
                invalid_sql=state["sql"],
                error_message=state["last_error_message"],
                reasoning_plan=state["reasoning_plan"],
            )
            state["sql"] = normalize_and_validate_sql(state["sql"])
            emit(state, {"type": "stage", "name": "sql_generated", "sql": state["sql"], "attempt": state["attempt"] + 1})
            state["attempt"] += 1

    return state


def handle_chart_intent(state: AnalyzeState) -> AnalyzeState:
    chart_prompt = state["prompt"] or f"Visualize this query result: {state['sql'][:300]}"
    state["chart_intent"] = suggest_chart_intent(
        user_prompt=chart_prompt,
        sql=state["sql"],
        columns=state["out_columns"],
    )
    if state["prompt_mentions_chart"] and not state["chart_intent"].get("make_chart"):
        # Fallback logic here, simplified
        state["chart_intent"] = {"make_chart": True, "chart_type": "bar"}  # Placeholder
    emit(state, {"type": "stage", "name": "chart_intent_ready", "chart_intent": state["chart_intent"]})
    if state["chart_intent"].get("make_chart"):
        state["fig"] = build_plotly_figure(intent=state["chart_intent"], columns=state["out_columns"], rows=state["out_rows"])
        emit(state, {"type": "stage", "name": "chart_ready", "chart_config": state["fig"], "plotly": state["fig"]})
    return state


def build_response(state: AnalyzeState) -> AnalyzeState:
    state["assistant_text"] = build_assistant_text(
        prompt=state["prompt"],
        columns=state["out_columns"],
        rows=state["out_rows"],
        chart_intent=state["chart_intent"],
        total_count=state["total_count"],
    )
    emit(state, {"type": "stage", "name": "assistant_ready", "assistant_text": state["assistant_text"]})
    state["response_blocks"] = build_response_blocks(
        prompt=state["prompt"],
        sql=state["sql"],
        columns=state["out_columns"],
        rows=state["out_rows"],
        chart_intent=state["chart_intent"],
        plotly=state["fig"],
        total_count=state["total_count"],
        page=1,
        page_size=state["page_size"],
    )
    if state["fig"]:
        state["response_blocks"].append(
            {
                "type": "pin_action",
                "title": state["prompt"][:120] or "Pinned chart",
                "sql": state["sql"],
                "chart_type": state["chart_intent"].get("chart_type"),
                "x_field": state["chart_intent"].get("x"),
                "y_field": state["chart_intent"].get("y"),
                "series_field": state["chart_intent"].get("series"),
            }
        )
    return state


def save_turn_node(state: AnalyzeState) -> AnalyzeState:
    turn_id = save_turn(
        engine,
        conversation_id=state["conversation_id"],
        prompt=state["prompt"],
        context_prompt=state["effective_prompt"] if state["effective_prompt"] != state["prompt"] else None,
        sql=state["sql"],
        columns=state["out_columns"],
        data=state["out_rows"],
        chart_intent=state["chart_intent"],
        plotly=state["fig"],
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
graph.add_node("check_cache", check_cache_and_reuse)
graph.add_node("execute_sql", execute_and_correct_sql)
graph.add_node("chart", handle_chart_intent)
graph.add_node("response", build_response)
graph.add_node("save", save_turn_node)

graph.set_entry_point("validate")

graph.add_edge("validate", "classify")

def route_after_classify(state: AnalyzeState):
    if state["intent_type"] == "CONVERSATION":
        return "conversation"
    else:
        return "check_cache"

graph.add_conditional_edges("classify", route_after_classify)

graph.add_edge("conversation", "save")

def route_after_check(state: AnalyzeState):
    if state["chart_only_intent"] or state["prompt_cache_hit"]:
        return "chart"
    else:
        return "execute_sql"

graph.add_conditional_edges("check_cache", route_after_check)

graph.add_edge("execute_sql", "chart")
graph.add_edge("chart", "response")
graph.add_edge("response", "save")

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
        "error": None,
    }

    try:
        final_state = compiled_graph.invoke(initial_state)
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
            "plotly": final_state["fig"],
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
            "plotly": None,
            "chart_config": None,
            "assistant_text": None,
            "response_blocks": [],
            "status": "failed",
            "source": "llm",
            "created_at": datetime.utcnow().isoformat(),
            "error": str(exc),
        }