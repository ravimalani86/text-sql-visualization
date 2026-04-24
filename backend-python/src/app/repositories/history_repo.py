from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import json
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import Engine, text

from app.services.chartjs_mapper import normalize_chart_config

def _new_id() -> str:
    return str(uuid.uuid4())


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    return str(value)


def _to_json(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, default=_json_default)


def _normalize_prompt(prompt: str) -> str:
    return " ".join((prompt or "").strip().lower().split())


def init_history_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS conversations (
                    id UUID PRIMARY KEY,
                    title TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS conversation_turns (
                    id UUID PRIMARY KEY,
                    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
                    prompt TEXT NOT NULL,
                    context_prompt TEXT,
                    sql TEXT,
                    columns JSONB,
                    data JSONB,
                    chart_intent JSONB,
                    chart_config JSONB,
                    assistant_text TEXT,
                    response_blocks JSONB,
                    status TEXT NOT NULL,
                    error TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(text("ALTER TABLE conversation_turns ADD COLUMN IF NOT EXISTS assistant_text TEXT"))
        conn.execute(text("ALTER TABLE conversation_turns ADD COLUMN IF NOT EXISTS response_blocks JSONB"))
        conn.execute(text("ALTER TABLE conversation_turns ADD COLUMN IF NOT EXISTS prompt_normalized TEXT"))
        conn.execute(text("ALTER TABLE conversation_turns ADD COLUMN IF NOT EXISTS total_count INTEGER"))
        conn.execute(
            text(
                """
                UPDATE conversation_turns
                SET prompt_normalized = LOWER(REGEXP_REPLACE(TRIM(prompt), '\\s+', ' ', 'g'))
                WHERE prompt_normalized IS NULL
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_turns_conversation_created
                ON conversation_turns (conversation_id, created_at DESC)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_turns_prompt_normalized_success_created
                ON conversation_turns (prompt_normalized, created_at DESC)
                WHERE status = 'success' AND prompt_normalized IS NOT NULL
                """
            )
        )


def create_conversation(engine: Engine, *, title: Optional[str] = None) -> str:
    conversation_id = _new_id()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO conversations (id, title)
                VALUES (CAST(:id AS UUID), :title)
                """
            ),
            {"id": conversation_id, "title": (title or "").strip() or None},
        )
    return conversation_id


def conversation_exists(engine: Engine, conversation_id: str) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT 1 FROM conversations WHERE id = CAST(:id AS UUID)"),
            {"id": conversation_id},
        ).first()
    return bool(row)


def save_turn(
    engine: Engine,
    *,
    conversation_id: str,
    prompt: str,
    context_prompt: Optional[str],
    sql: Optional[str],
    columns: Optional[List[str]],
    data: Optional[List[Dict[str, Any]]],
    chart_intent: Optional[Dict[str, Any]],
    chart_config: Optional[Dict[str, Any]],
    assistant_text: Optional[str],
    response_blocks: Optional[List[Dict[str, Any]]],
    status: str,
    error: Optional[str] = None,
    total_count: Optional[int] = None,
) -> str:
    turn_id = _new_id()
    normalized_prompt = _normalize_prompt(prompt)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO conversation_turns (
                    id, conversation_id, prompt, prompt_normalized, context_prompt, sql,
                    columns, data, chart_intent, chart_config, assistant_text, response_blocks,
                    status, error, total_count
                )
                VALUES (
                    CAST(:id AS UUID), CAST(:conversation_id AS UUID), :prompt, :prompt_normalized, :context_prompt, :sql,
                    CAST(:columns AS JSONB), CAST(:data AS JSONB),
                    CAST(:chart_intent AS JSONB), CAST(:chart_config AS JSONB),
                    :assistant_text, CAST(:response_blocks AS JSONB),
                    :status, :error, :total_count
                )
                """
            ),
            {
                "id": turn_id,
                "conversation_id": conversation_id,
                "prompt": prompt,
                "prompt_normalized": normalized_prompt,
                "context_prompt": context_prompt,
                "sql": sql,
                "columns": _to_json(columns),
                "data": _to_json(data),
                "chart_intent": _to_json(chart_intent),
                "chart_config": _to_json(chart_config),
                "assistant_text": assistant_text,
                "response_blocks": _to_json(response_blocks),
                "status": status,
                "error": error,
                "total_count": total_count,
            },
        )
        conn.execute(
            text(
                """
                UPDATE conversations
                SET updated_at = NOW()
                WHERE id = CAST(:id AS UUID)
                """
            ),
            {"id": conversation_id},
        )
    return turn_id


def get_latest_success_turns(engine: Engine, conversation_id: str, limit: int = 5) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id::text, prompt, sql, columns, data, chart_intent, chart_config, total_count, created_at
                FROM conversation_turns
                WHERE conversation_id = CAST(:id AS UUID) AND status = 'success'
                ORDER BY created_at DESC
                LIMIT :limit
                """
            ),
            {"id": conversation_id, "limit": limit},
        ).mappings().all()
    return [dict(r) for r in rows]


def find_latest_success_by_prompt(engine: Engine, prompt: str) -> Optional[Dict[str, Any]]:
    normalized_prompt = _normalize_prompt(prompt)
    if not normalized_prompt:
        return None

    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id::text, conversation_id::text, prompt, sql, columns, data, chart_intent, chart_config, total_count, created_at
                FROM conversation_turns
                WHERE status = 'success'
                  AND prompt_normalized = :prompt_normalized
                  AND sql IS NOT NULL
                  AND JSONB_TYPEOF(columns) = 'array'
                  AND JSONB_TYPEOF(data) = 'array'
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"prompt_normalized": normalized_prompt},
        ).mappings().first()

    return dict(row) if row else None


def list_conversations(engine: Engine) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    c.id::text AS id,
                    c.title,
                    c.created_at,
                    c.updated_at,
                    lt.prompt AS last_prompt,
                    lt.status AS last_status,
                    COALESCE(tc.turn_count, 0) AS turn_count
                FROM conversations c
                LEFT JOIN LATERAL (
                    SELECT prompt, status
                    FROM conversation_turns t
                    WHERE t.conversation_id = c.id
                    ORDER BY t.created_at DESC
                    LIMIT 1
                ) lt ON TRUE
                LEFT JOIN (
                    SELECT conversation_id, COUNT(*) AS turn_count
                    FROM conversation_turns
                    GROUP BY conversation_id
                ) tc ON tc.conversation_id = c.id
                ORDER BY c.updated_at DESC
                """
            )
        ).mappings().all()
    return [dict(r) for r in rows]


def get_conversation_with_turns(engine: Engine, conversation_id: str) -> Optional[Dict[str, Any]]:
    with engine.connect() as conn:
        conv = conn.execute(
            text(
                """
                SELECT id::text AS id, title, created_at, updated_at
                FROM conversations
                WHERE id = CAST(:id AS UUID)
                """
            ),
            {"id": conversation_id},
        ).mappings().first()
        if not conv:
            return None

        turns = conn.execute(
            text(
                """
                SELECT
                    id::text AS id,
                    prompt,
                    context_prompt,
                    sql,
                    columns,
                    data,
                    chart_intent,
                    chart_config,
                    assistant_text,
                    response_blocks,
                    status,
                    error,
                    total_count,
                    created_at
                FROM conversation_turns
                WHERE conversation_id = CAST(:id AS UUID)
                ORDER BY created_at ASC
                """
            ),
            {"id": conversation_id},
        ).mappings().all()

    out_turns: List[Dict[str, Any]] = []
    for t in turns:
        d = dict(t)
        d["chart_config"] = normalize_chart_config(d.get("chart_config"))
        out_turns.append(d)

    return {"conversation": dict(conv), "turns": out_turns}


def get_turn_by_id(engine: Engine, turn_id: str) -> Optional[Dict[str, Any]]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id::text, sql, columns, total_count
                FROM conversation_turns
                WHERE id = CAST(:id AS UUID)
                  AND status = 'success'
                  AND sql IS NOT NULL
                """
            ),
            {"id": turn_id},
        ).mappings().first()
    return dict(row) if row else None
