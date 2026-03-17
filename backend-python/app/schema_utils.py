from sqlalchemy import inspect

def get_db_schema(engine):
    inspector = inspect(engine)
    schema = {}

    # Internal app tables should not be exposed to SQL generation.
    excluded_tables = {"charts", "conversations", "conversation_turns"}

    for table in inspector.get_table_names():
        if table in excluded_tables:
            continue
        columns = inspector.get_columns(table)
        schema[table] = [
            {
                "name": col["name"],
                "type": str(col["type"])
            }
            for col in columns
        ]

    return schema
