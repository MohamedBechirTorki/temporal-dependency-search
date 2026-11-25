"""Health-check endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Request, Response, status

router = APIRouter(tags=["health"])


@router.get("/health/live")
def live() -> dict:
    """Process liveness (no DB access)."""
    return {"status": "ok"}


@router.get("/health/ready")
def ready(request: Request, response: Response) -> dict:
    """Readiness: both databases reachable."""
    dbs = request.app.state.dbs
    mongo = dbs.check_mongo()
    neo = dbs.check_neo4j()
    ok = mongo["status"] == "ok" and neo["status"] == "ok"
    if not ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ok" if ok else "degraded", "mongodb": mongo, "neo4j": neo}
