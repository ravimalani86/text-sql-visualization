from sqlalchemy import inspect

def get_db_schema(engine):
    inspector = inspect(engine)
    schema = {}

    for table in inspector.get_table_names():
        columns = inspector.get_columns(table)
        schema[table] = [
            {
                "name": col["name"],
                "type": str(col["type"])
            }
            for col in columns
        ]

    return schema
