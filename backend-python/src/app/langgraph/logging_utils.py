import json
import logging
from typing import Any, Dict

from app.langgraph.constants import LLM_LOG_MAX_CHARS

logger = logging.getLogger(__name__)


def truncate_for_log(value: Any, max_chars: int = LLM_LOG_MAX_CHARS) -> str:
    try:
        text = json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        text = repr(value)
    if len(text) > max_chars:
        return f"{text[:max_chars]}... [truncated {len(text) - max_chars} chars]"
    return text


def log_llm_input(llm_name: str, payload: Dict[str, Any]) -> None:
    logger.info("[LLM][INPUT] model=%s payload=%s", llm_name, truncate_for_log(payload))


def log_llm_output(llm_name: str, payload: Any) -> None:
    logger.info("[LLM][OUTPUT] model=%s payload=%s", llm_name, truncate_for_log(payload))
