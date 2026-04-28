from typing import Any, Dict, Tuple

from app.langgraph.constants import (
    ACTION_BUILD_RESPONSE,
    ACTION_EXECUTE_SQL,
    ACTION_FINAL_ANSWER,
    ACTION_GENERATE_CHART,
    ACTION_GENERATE_SQL,
    ACTION_GET_SCHEMA,
    ACTION_VALIDATE_SQL,
)
from app.langgraph.types import AnalyzeState


def fallback_agent_action(context: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    if not context.get("selected_schema"):
        return ACTION_GET_SCHEMA, {
            "user_prompt": context["prompt"],
            "max_tables": context["schema_search_max_tables"],
        }
    if not context.get("sql"):
        return ACTION_GENERATE_SQL, {
            "user_prompt": context["prompt"],
            "selected_schema": context["selected_schema"],
        }
    if not context.get("validated_sql"):
        return ACTION_VALIDATE_SQL, {"sql": context["sql"]}
    if not context.get("executed"):
        return ACTION_EXECUTE_SQL, {
            "sql": context["sql"],
            "max_rows": context["max_result_rows"],
        }
    if context.get("chart_intent") is None and not context.get("chart_attempted"):
        return ACTION_GENERATE_CHART, {
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
        return ACTION_BUILD_RESPONSE, {
            "prompt": context["original_prompt"],
            "sql": context["sql"],
            "columns": context["result_columns"],
            "rows": context["result_rows"],
            "chart_intent": context["chart_intent"],
            "chart_config": context["chart_config"],
            "total_count": context["total_count"],
            "page_size": context["page_size"],
        }
    return ACTION_FINAL_ANSWER, {}


def normalize_tool_input(
    action: str,
    action_input: Dict[str, Any],
    context: Dict[str, Any],
    state: AnalyzeState,
) -> Dict[str, Any]:
    payload = action_input if isinstance(action_input, dict) else {}

    if action == ACTION_GENERATE_SQL:
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

    if action == ACTION_GENERATE_CHART:
        return {
            "user_prompt": state.get("prompt") or context.get("prompt") or "",
            "sql": context.get("sql"),
            "columns": context.get("result_columns"),
            "rows": context.get("result_rows"),
            "force_chart": True,
        }

    if action == ACTION_BUILD_RESPONSE:
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

    if action == ACTION_GET_SCHEMA:
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
