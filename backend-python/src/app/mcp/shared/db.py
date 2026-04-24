from __future__ import annotations

from sqlalchemy.engine import Engine

from app.db.engine import engine


def get_engine() -> Engine:
    return engine

