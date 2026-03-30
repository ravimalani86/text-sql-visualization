from __future__ import annotations

from sqlalchemy import inspect
from sqlalchemy.engine import Engine


EXCLUDED_TABLES = {"pinned_tables", "pinned_charts", "conversations", "conversation_turns"}


def get_db_schema(engine: Engine) -> dict:
    inspector = inspect(engine)
    schema: dict = {}

    for table in inspector.get_table_names():
        if table in EXCLUDED_TABLES:
            continue
        columns = inspector.get_columns(table)
        schema[table] = [{"name": col["name"], "type": str(col["type"])} for col in columns]

    return schema

