from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.api.routes.analyze import router as analyze_router
from app.api.routes.charts import router as charts_router
from app.api.routes.history import router as history_router
from app.api.routes.upload import router as upload_router
from app.db.engine import engine
from app.repositories.charts_repo import init_charts_table
from app.repositories.history_repo import init_history_tables


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

    app.include_router(analyze_router)
    app.include_router(history_router)
    app.include_router(charts_router)
    app.include_router(upload_router)

    return app


app = create_app()

