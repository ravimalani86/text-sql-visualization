from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import Engine

from app.repositories.pinned_dashboard_repo import (
    ITEM_TYPE_TABLE,
    delete_dashboard_item,
    get_dashboard_item,
    init_pinned_dashboard_table,
    list_dashboard_items,
    pin_dashboard_table,
    update_dashboard_item_layout,
)


def init_tables(engine: Engine) -> None:
    init_pinned_dashboard_table(engine)


def pin_table(
    engine: Engine,
    *,
    title: str,
    sql_query: str,
    columns: List[str] | None = None,
) -> Dict[str, Any]:
    return pin_dashboard_table(
        engine,
        title=title,
        sql_query=sql_query,
        columns=columns,
    )


def list_tables(engine: Engine) -> List[Dict[str, Any]]:
    return list_dashboard_items(engine, ITEM_TYPE_TABLE)


def get_table(engine: Engine, table_id: str) -> Optional[Dict[str, Any]]:
    return get_dashboard_item(engine, table_id, ITEM_TYPE_TABLE)


def delete_table(engine: Engine, table_id: str) -> bool:
    return delete_dashboard_item(engine, table_id, ITEM_TYPE_TABLE)


def update_table_layout(
    engine: Engine,
    *,
    table_id: str,
    sort_order: Optional[int] = None,
    width_units: Optional[int] = None,
    height_px: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    return update_dashboard_item_layout(
        engine,
        item_id=table_id,
        item_type=ITEM_TYPE_TABLE,
        sort_order=sort_order,
        width_units=width_units,
        height_px=height_px,
        width_min=1,
        width_max=12,
        height_max=1200,
    )
