from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import Engine, text


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
                    plotly JSONB,
                    assistant_text TEXT,
                    response_blocks JSONB,
                    status TEXT NOT NULL,
                    error TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE conversation_turns
                ADD COLUMN IF NOT EXISTS assistant_text TEXT
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE conversation_turns
                ADD COLUMN IF NOT EXISTS response_blocks JSONB
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
                CREATE TABLE IF NOT EXISTS charts (
                    id UUID PRIMARY KEY,
                    title TEXT NOT NULL,
                    sql_query TEXT NOT NULL,
                    chart_type TEXT NOT NULL,
                    x_field TEXT,
                    y_field TEXT,
                    sort_order INT NOT NULL DEFAULT 0,
                    width_units INT NOT NULL DEFAULT 1,
                    height_px INT NOT NULL DEFAULT 320,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(text("ALTER TABLE charts ADD COLUMN IF NOT EXISTS sort_order INT NOT NULL DEFAULT 0"))
        conn.execute(text("ALTER TABLE charts ADD COLUMN IF NOT EXISTS width_units INT NOT NULL DEFAULT 1"))
        conn.execute(text("ALTER TABLE charts ADD COLUMN IF NOT EXISTS height_px INT NOT NULL DEFAULT 320"))


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
    plotly: Optional[Dict[str, Any]],
    assistant_text: Optional[str],
    response_blocks: Optional[List[Dict[str, Any]]],
    status: str,
    error: Optional[str] = None,
) -> str:
    turn_id = _new_id()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO conversation_turns (
                    id, conversation_id, prompt, context_prompt, sql,
                    columns, data, chart_intent, plotly, assistant_text, response_blocks, status, error
                )
                VALUES (
                    CAST(:id AS UUID), CAST(:conversation_id AS UUID), :prompt, :context_prompt, :sql,
                    CAST(:columns AS JSONB), CAST(:data AS JSONB),
                    CAST(:chart_intent AS JSONB), CAST(:plotly AS JSONB),
                    :assistant_text, CAST(:response_blocks AS JSONB),
                    :status, :error
                )
                """
            ),
            {
                "id": turn_id,
                "conversation_id": conversation_id,
                "prompt": prompt,
                "context_prompt": context_prompt,
                "sql": sql,
                "columns": _to_json(columns),
                "data": _to_json(data),
                "chart_intent": _to_json(chart_intent),
                "plotly": _to_json(plotly),
                "assistant_text": assistant_text,
                "response_blocks": _to_json(response_blocks),
                "status": status,
                "error": error,
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


def get_latest_success_turn(engine: Engine, conversation_id: str) -> Optional[Dict[str, Any]]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id::text, prompt, sql, columns, data, chart_intent, plotly, created_at
                FROM conversation_turns
                WHERE conversation_id = CAST(:id AS UUID) AND status = 'success'
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"id": conversation_id},
        ).mappings().first()
    return dict(row) if row else None


def get_latest_success_turns(engine: Engine, conversation_id: str, limit: int = 5) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id::text, prompt, sql, columns, data, chart_intent, plotly, created_at
                FROM conversation_turns
                WHERE conversation_id = CAST(:id AS UUID) AND status = 'success'
                ORDER BY created_at DESC
                LIMIT :limit
                """
            ),
            {"id": conversation_id, "limit": limit},
        ).mappings().all()
    return [dict(r) for r in rows]


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
                    plotly,
                    assistant_text,
                    response_blocks,
                    status,
                    error,
                    created_at
                FROM conversation_turns
                WHERE conversation_id = CAST(:id AS UUID)
                ORDER BY created_at ASC
                """
            ),
            {"id": conversation_id},
        ).mappings().all()

    return {"conversation": dict(conv), "turns": [dict(t) for t in turns]}


def pin_chart(
    engine: Engine,
    *,
    title: str,
    sql_query: str,
    chart_type: str,
    x_field: Optional[str],
    y_field: Optional[str],
) -> Dict[str, Any]:
    chart_id = _new_id()
    with engine.begin() as conn:
        max_order = conn.execute(text("SELECT COALESCE(MAX(sort_order), -1) FROM charts")).scalar()
        next_order = int(max_order) + 1
        row = conn.execute(
            text(
                """
                INSERT INTO charts (id, title, sql_query, chart_type, x_field, y_field, sort_order, width_units, height_px)
                VALUES (CAST(:id AS UUID), :title, :sql_query, :chart_type, :x_field, :y_field, :sort_order, 1, 320)
                RETURNING id::text AS id, title, sql_query, chart_type, x_field, y_field, sort_order, width_units, height_px, created_at
                """
            ),
            {
                "id": chart_id,
                "title": title,
                "sql_query": sql_query,
                "chart_type": chart_type,
                "x_field": x_field,
                "y_field": y_field,
                "sort_order": next_order,
            },
        ).mappings().first()
    return dict(row)


def list_pinned_charts(engine: Engine) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id::text AS id, title, sql_query, chart_type, x_field, y_field, sort_order, width_units, height_px, created_at
                FROM charts
                ORDER BY sort_order ASC, created_at ASC
                """
            )
        ).mappings().all()
    return [dict(r) for r in rows]


def get_pinned_chart(engine: Engine, chart_id: str) -> Optional[Dict[str, Any]]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id::text AS id, title, sql_query, chart_type, x_field, y_field, sort_order, width_units, height_px, created_at
                FROM charts
                WHERE id = CAST(:id AS UUID)
                """
            ),
            {"id": chart_id},
        ).mappings().first()
    return dict(row) if row else None


def delete_pinned_chart(engine: Engine, chart_id: str) -> bool:
    with engine.begin() as conn:
        deleted = conn.execute(
            text("DELETE FROM charts WHERE id = CAST(:id AS UUID)"),
            {"id": chart_id},
        )
    return bool((deleted.rowcount or 0) > 0)


def update_pinned_chart_layout(
    engine: Engine,
    *,
    chart_id: str,
    sort_order: Optional[int] = None,
    width_units: Optional[int] = None,
    height_px: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    updates: List[str] = []
    params: Dict[str, Any] = {"id": chart_id}

    if sort_order is not None:
        updates.append("sort_order = :sort_order")
        params["sort_order"] = int(sort_order)
    if width_units is not None:
        updates.append("width_units = :width_units")
        params["width_units"] = max(1, min(2, int(width_units)))
    if height_px is not None:
        updates.append("height_px = :height_px")
        params["height_px"] = max(220, min(620, int(height_px)))

    if not updates:
        return get_pinned_chart(engine, chart_id)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                f"""
                UPDATE charts
                SET {", ".join(updates)}
                WHERE id = CAST(:id AS UUID)
                RETURNING id::text AS id, title, sql_query, chart_type, x_field, y_field, sort_order, width_units, height_px, created_at
                """
            ),
            params,
        ).mappings().first()
    return dict(row) if row else None
