from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import Engine, text

ITEM_TYPE_CHART = "chart"
ITEM_TYPE_TABLE = "table"


def init_pinned_dashboard_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS pinned_dashboard (
                    id UUID PRIMARY KEY,
                    item_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    sql_query TEXT NOT NULL,
                    columns JSONB,
                    chart_type TEXT,
                    x_field TEXT,
                    y_field TEXT,
                    series_field TEXT,
                    sort_order INT NOT NULL DEFAULT 0,
                    width_units INT NOT NULL DEFAULT 12,
                    height_px INT NOT NULL DEFAULT 400,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT pinned_dashboard_item_type_chk CHECK (item_type IN ('chart', 'table'))
                )
                """
            )
        )
        conn.execute(text("ALTER TABLE pinned_dashboard ADD COLUMN IF NOT EXISTS item_type TEXT"))
        conn.execute(text("ALTER TABLE pinned_dashboard ADD COLUMN IF NOT EXISTS title TEXT"))
        conn.execute(text("ALTER TABLE pinned_dashboard ADD COLUMN IF NOT EXISTS sql_query TEXT"))
        conn.execute(text("ALTER TABLE pinned_dashboard ADD COLUMN IF NOT EXISTS columns JSONB"))
        conn.execute(text("ALTER TABLE pinned_dashboard ADD COLUMN IF NOT EXISTS chart_type TEXT"))
        conn.execute(text("ALTER TABLE pinned_dashboard ADD COLUMN IF NOT EXISTS x_field TEXT"))
        conn.execute(text("ALTER TABLE pinned_dashboard ADD COLUMN IF NOT EXISTS y_field TEXT"))
        conn.execute(text("ALTER TABLE pinned_dashboard ADD COLUMN IF NOT EXISTS series_field TEXT"))
        conn.execute(text("ALTER TABLE pinned_dashboard ADD COLUMN IF NOT EXISTS sort_order INT NOT NULL DEFAULT 0"))
        conn.execute(text("ALTER TABLE pinned_dashboard ADD COLUMN IF NOT EXISTS width_units INT NOT NULL DEFAULT 12"))
        conn.execute(text("ALTER TABLE pinned_dashboard ADD COLUMN IF NOT EXISTS height_px INT NOT NULL DEFAULT 400"))
        conn.execute(text("ALTER TABLE pinned_dashboard ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pinned_dashboard_type_order ON pinned_dashboard (item_type, sort_order ASC, created_at ASC)"))

        conn.execute(text("ALTER TABLE pinned_dashboard DROP CONSTRAINT IF EXISTS pinned_dashboard_item_type_chk"))
        conn.execute(
            text(
                """
                ALTER TABLE pinned_dashboard
                ADD CONSTRAINT pinned_dashboard_item_type_chk
                CHECK (item_type IN ('chart', 'table'))
                """
            )
        )


def pin_dashboard_chart(
    engine: Engine,
    *,
    title: str,
    sql_query: str,
    chart_type: str,
    x_field: Optional[str],
    y_field: Optional[str],
    series_field: Optional[str],
) -> Dict[str, Any]:
    item_id = str(uuid.uuid4())
    with engine.begin() as conn:
        max_order = conn.execute(text("SELECT COALESCE(MAX(sort_order), -1) FROM pinned_dashboard")).scalar()
        next_order = int(max_order) + 1
        row = conn.execute(
            text(
                """
                INSERT INTO pinned_dashboard (
                    id, item_type, title, sql_query, chart_type, x_field, y_field, series_field,
                    sort_order, width_units, height_px
                )
                VALUES (
                    CAST(:id AS UUID), :item_type, :title, :sql_query, :chart_type, :x_field, :y_field, :series_field,
                    :sort_order, :width_units, :height_px
                )
                RETURNING id::text AS id, title, sql_query, chart_type, x_field, y_field, series_field, sort_order, width_units, height_px, created_at
                """
            ),
            {
                "id": item_id,
                "item_type": ITEM_TYPE_CHART,
                "title": title,
                "sql_query": sql_query,
                "chart_type": chart_type,
                "x_field": x_field,
                "y_field": y_field,
                "series_field": series_field,
                "sort_order": next_order,
                "width_units": 1,
                "height_px": 320,
            },
        ).mappings().first()
    return dict(row)


def pin_dashboard_table(
    engine: Engine,
    *,
    title: str,
    sql_query: str,
    columns: List[str] | None = None,
) -> Dict[str, Any]:
    item_id = str(uuid.uuid4())
    with engine.begin() as conn:
        max_order = conn.execute(text("SELECT COALESCE(MAX(sort_order), -1) FROM pinned_dashboard")).scalar()
        next_order = int(max_order) + 1
        cols_json = json.dumps(columns) if columns else None
        row = conn.execute(
            text(
                """
                INSERT INTO pinned_dashboard (
                    id, item_type, title, sql_query, columns, sort_order, width_units, height_px
                )
                VALUES (
                    CAST(:id AS UUID), :item_type, :title, :sql_query, CAST(:columns AS JSONB), :sort_order, :width_units, :height_px
                )
                RETURNING id::text AS id, title, sql_query, columns, sort_order, width_units, height_px, created_at
                """
            ),
            {
                "id": item_id,
                "item_type": ITEM_TYPE_TABLE,
                "title": title,
                "sql_query": sql_query,
                "columns": cols_json,
                "sort_order": next_order,
                "width_units": 12,
                "height_px": 400,
            },
        ).mappings().first()
    return dict(row)


def list_dashboard_items(engine: Engine, item_type: str) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT *
                FROM pinned_dashboard
                WHERE item_type = :item_type
                ORDER BY sort_order ASC, created_at ASC
                """
            ),
            {"item_type": item_type},
        ).mappings().all()
    return [dict(r) for r in rows]


def get_dashboard_item(engine: Engine, item_id: str, item_type: str) -> Optional[Dict[str, Any]]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT *
                FROM pinned_dashboard
                WHERE id = CAST(:id AS UUID)
                  AND item_type = :item_type
                """
            ),
            {"id": item_id, "item_type": item_type},
        ).mappings().first()
    return dict(row) if row else None


def delete_dashboard_item(engine: Engine, item_id: str, item_type: str) -> bool:
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                DELETE FROM pinned_dashboard
                WHERE id = CAST(:id AS UUID)
                  AND item_type = :item_type
                """
            ),
            {"id": item_id, "item_type": item_type},
        )
    return bool((result.rowcount or 0) > 0)


def update_dashboard_item_layout(
    engine: Engine,
    *,
    item_id: str,
    item_type: str,
    sort_order: Optional[int] = None,
    width_units: Optional[int] = None,
    height_px: Optional[int] = None,
    width_min: int,
    width_max: int,
    height_max: int,
) -> Optional[Dict[str, Any]]:
    updates: List[str] = []
    params: Dict[str, Any] = {
        "id": item_id,
        "item_type": item_type,
    }

    if sort_order is not None:
        updates.append("sort_order = :sort_order")
        params["sort_order"] = int(sort_order)
    if width_units is not None:
        updates.append("width_units = :width_units")
        params["width_units"] = max(width_min, min(width_max, int(width_units)))
    if height_px is not None:
        updates.append("height_px = :height_px")
        params["height_px"] = max(220, min(height_max, int(height_px)))

    if not updates:
        return get_dashboard_item(engine, item_id, item_type)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                f"""
                UPDATE pinned_dashboard
                SET {", ".join(updates)}
                WHERE id = CAST(:id AS UUID)
                  AND item_type = :item_type
                RETURNING *
                """
            ),
            params,
        ).mappings().first()
    return dict(row) if row else None
