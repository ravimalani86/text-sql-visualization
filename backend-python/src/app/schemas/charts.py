from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class ChartPinRequest(BaseModel):
    title: str
    sql: str
    chart_type: str
    x_field: Optional[str] = None
    y_field: Optional[str] = None
    series_field: Optional[str] = None


class ChartLayoutRequest(BaseModel):
    sort_order: Optional[int] = None
    width_units: Optional[int] = None
    height_px: Optional[int] = None

