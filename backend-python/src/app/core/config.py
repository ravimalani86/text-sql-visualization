from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_PAGE_SIZE = int(os.environ.get("DEFAULT_PAGE_SIZE", "10"))


def _env_bool(raw: str | None, *, default: bool) -> bool:
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    database_url: str
    openai_api_key: str
    openai_model: str
    cors_allow_origins: list[str]
    max_result_rows: int
    max_turns_in_conversation: int
    schema_cache_ttl_seconds: float
    default_page_size: int
    schema_search_max_tables: int
    enable_sql_planning: bool
    text_to_sql_max_correction_retries: int
    # When True, reuse SQL from any past successful turn with the same prompt (see find_latest_success_by_prompt).
    reuse_sql_from_history_by_prompt: bool
    redis_url: str
    schema_cache_key: str


def get_settings() -> Settings:
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    openai_api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not openai_api_key:
        raise RuntimeError("OPENAI_API_KEY environment variable is not set")

    # Keep default compatible with current frontend
    cors_allow_origins = ["http://localhost:3000"]

    return Settings(
        database_url=database_url,
        openai_api_key=openai_api_key,
        openai_model=os.environ.get("OPENAI_MODEL", "gpt-5").strip() or "gpt-5",
        cors_allow_origins=cors_allow_origins,
        max_result_rows=int(os.environ.get("MAX_RESULT_ROWS", "500")),
        max_turns_in_conversation=int(os.environ.get("MAX_TURNS_IN_CONVERSATION", "2")),
        schema_cache_ttl_seconds=float(os.environ.get("SCHEMA_CACHE_TTL_SECONDS", "300")),
        default_page_size=DEFAULT_PAGE_SIZE,
        schema_search_max_tables=int(os.environ.get("SCHEMA_SEARCH_MAX_TABLES", "8")),
        enable_sql_planning=_env_bool(os.environ.get("ENABLE_SQL_PLANNING"), default=True),
        text_to_sql_max_correction_retries=int(
            os.environ.get("TEXT_TO_SQL_MAX_CORRECTION_RETRIES", "2")
        ),
        reuse_sql_from_history_by_prompt=_env_bool(
            os.environ.get("REUSE_SQL_FROM_HISTORY_BY_PROMPT"),
            default=True,
        ),
        redis_url=os.environ.get("REDIS_URL", "redis://redis:6379/0").strip(),
        schema_cache_key=os.environ.get("SCHEMA_CACHE_KEY", "app:db_schema:v1").strip(),
    )
