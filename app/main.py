"""FastAPI application entrypoint."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.errors import install_error_handlers
from app.api.routes import analytics, dependencies, health, ingest, nodes, search, sync, temporal
from app.config import Settings, get_settings
from app.container import Container
from app.logging_config import configure_logging

log = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.log_json)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        container = Container.build(settings)
        app.state.settings = settings
        app.state.container = container
        app.state.dbs = container.dbs
        container.init_storage()
        if settings.sync_worker_enabled:
            container.sync.start_worker()
        log.info("application started", extra={"env": settings.app_env})
        try:
            yield
        finally:
            container.close()
            log.info("application stopped")

    app = FastAPI(
        title="Temporal Dependency Search Engine",
        version="0.2.0",
        description=(
            "Bitemporal dependency graph storage, search and analytics.\n\n"
            "Convention: an edge **A -> B means A depends on B**.\n\n"
            "MongoDB is the source of truth; Neo4j is a projection kept in sync via a transactional outbox."
        ),
        lifespan=lifespan,
    )
    install_error_handlers(app)
    app.include_router(health.router)
    app.include_router(nodes.router)
    app.include_router(dependencies.router)
    app.include_router(search.router)
    app.include_router(temporal.router)
    app.include_router(analytics.router)
    app.include_router(ingest.router)
    app.include_router(sync.router)
    return app


app = create_app()
