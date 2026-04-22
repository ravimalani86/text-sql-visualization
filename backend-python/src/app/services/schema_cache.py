from __future__ import annotations

from typing import Any, Dict

from app.core.config import get_settings
from app.db.cache import get_json, set_json
from app.db.engine import engine
from app.db.schema import get_db_schema, get_relationships

ALLOWED_TABLE = ["sales", "customers", "categories", "suppliers", "products"]

def get_cached_schema() -> Dict[str, Any]:
    settings = get_settings()
    cache_key = settings.schema_cache_key
    print("cache_key: ", cache_key)
    cached = get_json(cache_key)
    print("cached: ", cached)
    if cached is not None:
        return cached

    schema = get_db_schema(engine, ALLOWED_TABLE)
    print("schema: ", schema)
    
    relationships = get_relationships(engine, ALLOWED_TABLE)
    print("relationships: ", relationships)
    if schema:
        set_json(key=cache_key, value=schema, ttl_seconds=int(settings.schema_cache_ttl_seconds))
    return schema


def warm_schema_cache() -> None:
    print("warm_schema_cache")
    schema = get_cached_schema()
    print("schema: ", schema)
    if schema:
        print("schema is warm")
    else:
        print("schema is not warm")