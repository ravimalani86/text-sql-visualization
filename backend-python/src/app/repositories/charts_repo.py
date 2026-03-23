from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import Engine, text


def init_charts_table(engine: Engine) -> None:
    with engine.begin() as conn:
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


def pin_chart(
    engine: Engine,
    *,
    title: str,
    sql_query: str,
    chart_type: str,
    x_field: Optional[str],
    y_field: Optional[str],
) -> Dict[str, Any]:
    import uuid

    chart_id = str(uuid.uuid4())
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
        params["width_units"] = max(1, min(12, int(width_units)))
    if height_px is not None:
        updates.append("height_px = :height_px")
        params["height_px"] = max(220, min(1000, int(height_px)))

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

