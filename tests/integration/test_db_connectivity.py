"""Integration test verifying real connectivity and replica set support."""
import pytest
from app.config import get_settings
from app.db import Databases

@pytest.mark.integration
def test_live_databases_healthy():
    settings = get_settings()
    dbs = Databases(settings)
    try:
        mongo_status = dbs.check_mongo()
        assert mongo_status["status"] == "ok"
        assert mongo_status["replica_set"] == "rs0"
        assert mongo_status["transactions_supported"] is True

        neo4j_status = dbs.check_neo4j()
        assert neo4j_status["status"] == "ok"
        assert "Neo4j" in neo4j_status["agent"]
    finally:
        dbs.close()
