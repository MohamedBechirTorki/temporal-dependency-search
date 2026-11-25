"""Phase 1 smoke tests that do not require running databases."""
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def _settings() -> Settings:
    # Point at closed ports so readiness must report degraded quickly.
    return Settings(
        mongo_uri="mongodb://127.0.0.1:1/?directConnection=true",
        mongo_timeout_ms=300,
        neo4j_uri="bolt://127.0.0.1:1",
        neo4j_password="x",
        neo4j_timeout_s=0.5,
        log_json=False,
        sync_worker_enabled=False,
    )


def test_liveness():
    with TestClient(create_app(_settings())) as c:
        r = c.get("/health/live")
        assert r.status_code == 200 and r.json() == {"status": "ok"}


def test_readiness_reports_degraded_without_leaking_exceptions():
    with TestClient(create_app(_settings())) as c:
        r = c.get("/health/ready")
        assert r.status_code == 503
        body = r.json()
        assert body["status"] == "degraded"
        assert body["mongodb"]["status"] == "error"
        assert body["neo4j"]["status"] == "error"
        # only exception class name exposed, no messages/stack traces
        assert set(body["mongodb"]) == {"status", "error"}
