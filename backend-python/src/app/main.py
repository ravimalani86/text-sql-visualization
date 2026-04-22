from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.api.routes.analyze import router as analyze_router
from app.api.routes.charts import router as charts_router
from app.api.routes.history import router as history_router
from app.api.routes.tables import router as tables_router
from app.api.routes.table_data import router as table_data_router
from app.api.routes.upload import router as upload_router
from app.api.routes.export import router as export_router
from app.api.routes.followup import router as followup_router
from app.api.routes.asks import router as asks_router
from app.db.engine import engine
from app.repositories.history_repo import init_history_tables
from app.repositories.pinned_dashboard_repo import init_pinned_dashboard_table
from app.services.schema_cache import warm_schema_cache


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title="AI Analytics Backend")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.on_event("startup")
    async def _on_startup() -> None:
        print("on_startup")
        # init_history_tables(engine)
        # print("init_history_tables")
        # init_pinned_dashboard_table(engine)
        # print("init_pinned_dashboard_table")
        warm_schema_cache()
        print("warm_schema_cache")

    app.include_router(analyze_router)
    app.include_router(history_router)
    app.include_router(charts_router)
    app.include_router(tables_router)
    app.include_router(table_data_router)
    app.include_router(upload_router)
    app.include_router(export_router)
    app.include_router(followup_router)
    app.include_router(asks_router)

    return app


app = create_app()
