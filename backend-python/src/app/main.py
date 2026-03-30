from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.api.routes.analyze import router as analyze_router
from app.api.routes.charts import router as charts_router
from app.api.routes.history import router as history_router
from app.api.routes.pinned_tables import router as pinned_tables_router
from app.api.routes.table_data import router as table_data_router
from app.api.routes.upload import router as upload_router
from app.api.routes.export import router as export_router
from app.db.engine import engine
from app.repositories.charts_repo import init_charts_table
from app.repositories.history_repo import init_history_tables
from app.repositories.pinned_tables_repo import init_pinned_tables_table


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
        init_history_tables(engine)
        init_charts_table(engine)
        init_pinned_tables_table(engine)

    app.include_router(analyze_router)
    app.include_router(history_router)
    app.include_router(charts_router)
    app.include_router(pinned_tables_router)
    app.include_router(table_data_router)
    app.include_router(upload_router)
    app.include_router(export_router)

    return app


app = create_app()

