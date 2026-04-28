from typing import Any, Callable, Dict, List, Optional, TypedDict


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
