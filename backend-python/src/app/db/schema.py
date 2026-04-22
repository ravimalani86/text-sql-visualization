from __future__ import annotations

from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from typing import List, Dict, Any


def get_db_schema(engine: Engine, tables: List[str]) -> Dict[str, Any]:
    print("get_db_schema")
    inspector = inspect(engine)
    schema: Dict[str, Any] = {}

    for table in tables:
        if table not in inspector.get_table_names():
            continue  # skip invalid table names

        columns = inspector.get_columns(table)
        schema[table] = [
            {
                "name": col["name"],
                "type": str(col["type"])
            }
            for col in columns
        ]

    return schema


def get_relationships(engine: Engine, tables: list[str]) -> list[str]:
    print("get_relationships")

    inspector = inspect(engine)
    relationships = []
    existing_tables = set(inspector.get_table_names())

    for table in tables:
        if table not in existing_tables:
            continue  # skip invalid table names
        fks = inspector.get_foreign_keys(table)
        print("fks: ", fks)
        for fk in fks:
            local_cols = fk.get("constrained_columns", [])
            ref_table = fk.get("referred_table")
            ref_cols = fk.get("referred_columns", [])

            for l, r in zip(local_cols, ref_cols):
                relationships.append(f"{table}.{l} = {ref_table}.{r}")

    return relationships