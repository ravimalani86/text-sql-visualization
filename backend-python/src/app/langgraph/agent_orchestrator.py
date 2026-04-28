import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from app.core.config import DEFAULT_PAGE_SIZE, get_settings
from app.langgraph.constants import (
    ACTION_BUILD_RESPONSE,
    ACTION_CORRECT_SQL,
    ACTION_EXECUTE_SQL,
    ACTION_FINAL_ANSWER,
    ACTION_GENERATE_CHART,
    ACTION_GENERATE_SQL,
    ACTION_GET_SCHEMA,
    ACTION_VALIDATE_SQL,
    MAX_AGENT_STEPS,
)
from app.langgraph.logging_utils import log_llm_input, log_llm_output
from app.langgraph.state_factory import create_baseline_state
from app.langgraph.tool_adapters import fallback_agent_action, normalize_tool_input
from app.langgraph.types import AnalyzeState
from app.skills.llm_client import generate_agent_step
from app.skills.skill_executor import execute_skill
from app.skills.skill_registry import load_tool_specs

logger = logging.getLogger(__name__)


def _execute_skill_with_normalization(
    action: str,
    action_input: Dict[str, Any],
    context: Dict[str, Any],
    state: AnalyzeState,
    *,
    base_url: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    normalized_input = normalize_tool_input(action, action_input, context, state)
    logger.info("[WORKFLOW] Tool called: %s", action)
    observation = execute_skill(action, normalized_input, base_url=base_url)
    return observation, normalized_input


def _apply_observation_to_context(action: str, observation: Dict[str, Any], context: Dict[str, Any]) -> None:
    if action == ACTION_GET_SCHEMA:
        context["schema"] = observation.get("schema")
        context["selected_schema"] = observation.get("selected_schema")
    elif action == ACTION_GENERATE_SQL:
        context["sql"] = observation.get("sql")
        context["sql_plan"] = observation.get("reasoning_plan")
        context["validated_sql"] = False
    elif action == ACTION_VALIDATE_SQL:
        context["sql"] = observation.get("sql")
        context["validated_sql"] = True
    elif action == ACTION_EXECUTE_SQL:
        context["result_columns"] = observation.get("columns") or []
        context["result_rows"] = observation.get("rows") or []
        context["total_count"] = observation.get("total_count")
        context["executed"] = True
    elif action == ACTION_CORRECT_SQL:
        context["sql"] = observation.get("sql")
        context["validated_sql"] = False
        context["attempt"] = int(context["attempt"]) + 1
    elif action == ACTION_GENERATE_CHART:
        context["chart_attempted"] = True
        chart_intent = observation.get("chart_intent")
        if not isinstance(chart_intent, dict):
            chart_intent = {"make_chart": bool(observation.get("make_chart"))}
        context["chart_intent"] = chart_intent or {"make_chart": False}
        context["chart_config"] = observation.get("fig")
    elif action == ACTION_BUILD_RESPONSE:
        context["assistant_text"] = observation.get("assistant_text")
        context["response_blocks"] = observation.get("response_blocks") or []


def _finalize_response_if_needed(
    prompt: str,
    context: Dict[str, Any],
    state: AnalyzeState,
    *,
    base_url: str,
) -> None:
    if context["response_blocks"] is not None or not context.get("sql"):
        return

    if context["chart_intent"] is None and not context.get("chart_attempted"):
        chart_payload = normalize_tool_input(
            ACTION_GENERATE_CHART,
            {
                "user_prompt": prompt,
                "sql": context["sql"],
                "columns": context["result_columns"],
                "rows": context["result_rows"],
                "force_chart": bool(state["prompt_mentions_chart"]),
            },
            context,
            state,
        )
        logger.info("[WORKFLOW] Tool called: generate_chart")
        chart = execute_skill(ACTION_GENERATE_CHART, chart_payload, base_url=base_url)
        context["chart_attempted"] = True
        chart_intent = chart.get("chart_intent")
        if not isinstance(chart_intent, dict):
            chart_intent = {"make_chart": bool(chart.get("make_chart"))}
        context["chart_intent"] = chart_intent or {"make_chart": False}
        context["chart_config"] = chart.get("fig")
    elif context["chart_intent"] is None:
        context["chart_intent"] = {"make_chart": False}
        context["chart_config"] = None

    build_payload = normalize_tool_input(
        ACTION_BUILD_RESPONSE,
        {
            "prompt": state["prompt"],
            "sql": context["sql"],
            "columns": context["result_columns"],
            "rows": context["result_rows"],
            "chart_intent": context["chart_intent"] or {"make_chart": False},
            "fig": context["chart_config"],
            "total_count": context["total_count"],
            "page_size": state["page_size"],
        },
        context,
        state,
    )
    logger.info("[WORKFLOW] Tool called: build_response")
    built = execute_skill(ACTION_BUILD_RESPONSE, build_payload, base_url=base_url)
    context["assistant_text"] = built.get("assistant_text")
    context["response_blocks"] = built.get("response_blocks") or []


def call_llm_with_tools(prompt: str) -> Dict[str, Any]:
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
    baseline_state = create_baseline_state(prompt)

    def _exec(skill_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized_payload = normalize_tool_input(skill_name, payload, baseline_context, baseline_state)
        logger.info("[WORKFLOW] Tool called: skill_name=%s", skill_name)
        try:
            return execute_skill(skill_name, normalized_payload, base_url=base_url)
        except Exception as exc:
            logger.error("[ERROR] workflow_tool=%s error=%s", skill_name, str(exc), exc_info=True)
            raise

    schema_result = _exec(
        ACTION_GET_SCHEMA,
        {"user_prompt": prompt, "max_tables": settings.schema_search_max_tables},
    )
    schema = schema_result.get("schema") or {}
    selected_schema = schema_result.get("selected_schema") or {}
    baseline_context["selected_schema"] = selected_schema
    sql_result = _exec(
        ACTION_GENERATE_SQL,
        {"user_prompt": prompt, "selected_schema": selected_schema},
    )
    sql_plan = sql_result.get("reasoning_plan")
    sql = _exec(ACTION_VALIDATE_SQL, {"sql": sql_result.get("sql")}).get("sql")
    baseline_context["sql"] = sql

    result_columns: List[str] = []
    result_rows: List[Dict[str, Any]] = []
    total_count: Optional[int] = None
    attempt = 0
    error_message = ""

    while attempt <= settings.text_to_sql_max_correction_retries:
        try:
            exe = _exec(ACTION_EXECUTE_SQL, {"sql": sql, "max_rows": settings.max_result_rows})
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
                ACTION_CORRECT_SQL,
                {
                    "user_prompt": prompt,
                    "selected_schema": selected_schema,
                    "invalid_sql": sql,
                    "error_message": error_message,
                    "reasoning_plan": sql_plan,
                },
            )
            sql = _exec(ACTION_VALIDATE_SQL, {"sql": corrected.get("sql")}).get("sql")
            baseline_context["sql"] = sql
            attempt += 1

    chart = _exec(
        ACTION_GENERATE_CHART,
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
        ACTION_BUILD_RESPONSE,
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


def run_agent_loop(prompt: str, state: AnalyzeState) -> Dict[str, Any]:
    settings = get_settings()
    base_url = os.environ.get("MCP_BASE_URL", "").strip()
    tools = load_tool_specs()

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

    for _ in range(MAX_AGENT_STEPS):
        agent_payload = {
            "user_prompt": prompt,
            "tools_count": len(tools),
            "steps_count": len(state["agent_steps"]),
            "max_steps": MAX_AGENT_STEPS,
        }
        log_llm_input("generate_agent_step", agent_payload)
        try:
            decision = generate_agent_step(
                user_prompt=prompt,
                tools=tools,
                steps=state["agent_steps"],
                max_steps=MAX_AGENT_STEPS,
            )
        except Exception as exc:
            logger.error("[LLM][ERROR] model=generate_agent_step error=%s", str(exc), exc_info=True)
            raise
        log_llm_output("generate_agent_step", decision)

        thought = str(decision.get("thought") or "").strip()
        action = str(decision.get("action") or "").strip()
        action_input = decision.get("input") if isinstance(decision.get("input"), dict) else {}

        if not action:
            action, action_input = fallback_agent_action(context)
            thought = thought or f"Fallback action selected: {action}"

        if action == ACTION_GENERATE_CHART and context.get("chart_attempted"):
            action = ACTION_BUILD_RESPONSE
            action_input = {}
            thought = thought or "Chart generation already attempted; moving to response assembly."

        emit = state.get("emit_event")
        if emit:
            emit({"type": "agent_thought", "thought": thought})
            emit({"type": "agent_action", "action": action})

        if action == ACTION_FINAL_ANSWER:
            output = decision.get("output") if isinstance(decision.get("output"), dict) else {}
            if output.get("sql"):
                context["sql"] = output.get("sql")
            if isinstance(output.get("data"), list):
                context["result_rows"] = output.get("data")
            break

        try:
            observation, normalized_input = _execute_skill_with_normalization(
                action,
                action_input,
                context,
                state,
                base_url=base_url,
            )
            action_input = normalized_input
        except Exception as exc:
            logger.error(
                "[ERROR] conversation_id=%s action=%s skill_error=%s",
                state.get("conversation_id"),
                action,
                str(exc),
                exc_info=True,
            )
            fallback_action, fallback_input = fallback_agent_action(context)
            if fallback_action == ACTION_FINAL_ANSWER:
                break
            observation, normalized_input = _execute_skill_with_normalization(
                fallback_action,
                fallback_input,
                context,
                state,
                base_url=base_url,
            )
            action = fallback_action
            action_input = normalized_input

        emit = state.get("emit_event")
        if emit:
            emit({"type": "agent_observation", "result": observation})
        state["agent_steps"].append(
            {
                "thought": thought,
                "action": action,
                "input": action_input,
                "observation": observation,
            }
        )
        _apply_observation_to_context(action, observation, context)

        if action == ACTION_BUILD_RESPONSE:
            break

    _finalize_response_if_needed(prompt, context, state, base_url=base_url)

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
