from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class TablePinRequest(BaseModel):
    title: str
    sql: str
    columns: Optional[List[str]] = None


class TableLayoutRequest(BaseModel):
    sort_order: Optional[int] = None
    width_units: Optional[int] = None
    height_px: Optional[int] = None
