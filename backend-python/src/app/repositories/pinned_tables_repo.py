from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import Engine, text


def init_pinned_tables_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS pinned_tables (
                    id UUID PRIMARY KEY,
                    title TEXT NOT NULL,
                    sql_query TEXT NOT NULL,
                    columns JSONB,
                    sort_order INT NOT NULL DEFAULT 0,
                    width_units INT NOT NULL DEFAULT 12,
                    height_px INT NOT NULL DEFAULT 400,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )


def pin_table(
    engine: Engine,
    *,
    title: str,
    sql_query: str,
    columns: List[str] | None = None,
) -> Dict[str, Any]:
    table_id = str(uuid.uuid4())
    with engine.begin() as conn:
        max_order = conn.execute(text("SELECT COALESCE(MAX(sort_order), -1) FROM pinned_tables")).scalar()
        next_order = int(max_order) + 1
        cols_json = json.dumps(columns) if columns else None
        row = conn.execute(
            text(
                """
                INSERT INTO pinned_tables (id, title, sql_query, columns, sort_order)
                VALUES (CAST(:id AS UUID), :title, :sql_query, CAST(:columns AS JSONB), :sort_order)
                RETURNING id::text AS id, title, sql_query, columns, sort_order, width_units, height_px, created_at
                """
            ),
            {
                "id": table_id,
                "title": title,
                "sql_query": sql_query,
                "columns": cols_json,
                "sort_order": next_order,
            },
        ).mappings().first()
    return dict(row)


def list_pinned_tables(engine: Engine) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id::text AS id, title, sql_query, columns, sort_order, width_units, height_px, created_at
                FROM pinned_tables
                ORDER BY sort_order ASC, created_at ASC
                """
            )
        ).mappings().all()
    return [dict(r) for r in rows]


def get_pinned_table(engine: Engine, table_id: str) -> Optional[Dict[str, Any]]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id::text AS id, title, sql_query, columns, sort_order, width_units, height_px, created_at
                FROM pinned_tables
                WHERE id = CAST(:id AS UUID)
                """
            ),
            {"id": table_id},
        ).mappings().first()
    return dict(row) if row else None


def delete_pinned_table(engine: Engine, table_id: str) -> bool:
    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM pinned_tables WHERE id = CAST(:id AS UUID)"),
            {"id": table_id},
        )
    return bool((result.rowcount or 0) > 0)


def update_pinned_table_layout(
    engine: Engine,
    *,
    table_id: str,
    sort_order: Optional[int] = None,
    width_units: Optional[int] = None,
    height_px: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    updates: List[str] = []
    params: Dict[str, Any] = {"id": table_id}

    if sort_order is not None:
        updates.append("sort_order = :sort_order")
        params["sort_order"] = int(sort_order)
    if width_units is not None:
        updates.append("width_units = :width_units")
        params["width_units"] = max(1, min(12, int(width_units)))
    if height_px is not None:
        updates.append("height_px = :height_px")
        params["height_px"] = max(220, min(1200, int(height_px)))

    if not updates:
        return get_pinned_table(engine, table_id)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                f"""
                UPDATE pinned_tables
                SET {", ".join(updates)}
                WHERE id = CAST(:id AS UUID)
                RETURNING id::text AS id, title, sql_query, columns, sort_order, width_units, height_px, created_at
                """
            ),
            params,
        ).mappings().first()
    return dict(row) if row else None
