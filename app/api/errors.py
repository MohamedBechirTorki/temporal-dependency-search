"""Exception -> HTTP mapping. Raw database exceptions are never exposed."""
from __future__ import annotations

import logging
import uuid

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from neo4j.exceptions import ServiceUnavailable, SessionExpired
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from app.domain.errors import DomainError

log = logging.getLogger(__name__)


def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", None) or uuid.uuid4().hex


def _error(status: int, code: str, message: str, request: Request, details: dict | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": {"code": code, "message": message, "details": details or {},
                           "request_id": _request_id(request)}},
    )


def install_error_handlers(app: FastAPI) -> None:
    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        request.state.request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        response = await call_next(request)
        response.headers["x-request-id"] = request.state.request_id
        return response

    @app.exception_handler(DomainError)
    async def domain_error(request: Request, exc: DomainError):
        return _error(exc.status_code, exc.code, exc.message, request, exc.details)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        errors = [{"loc": list(e.get("loc", [])), "msg": e.get("msg"), "type": e.get("type")} for e in exc.errors()]
        return _error(422, "validation_error", "request validation failed", request, {"errors": errors})

    @app.exception_handler(ConnectionFailure)
    @app.exception_handler(ServerSelectionTimeoutError)
    async def mongo_unavailable(request: Request, exc: Exception):
        log.error("mongodb unavailable", extra={"error": type(exc).__name__, "request_id": _request_id(request)})
        return _error(503, "storage_unavailable", "primary store (MongoDB) unavailable", request)

    @app.exception_handler(ServiceUnavailable)
    @app.exception_handler(SessionExpired)
    async def neo4j_unavailable(request: Request, exc: Exception):
        log.error("neo4j unavailable", extra={"error": type(exc).__name__, "request_id": _request_id(request)})
        return _error(503, "graph_unavailable", "graph store (Neo4j) unavailable", request)

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception):
        log.exception("unhandled error", extra={"request_id": _request_id(request)})
        return _error(500, "internal_error", "internal server error", request)
