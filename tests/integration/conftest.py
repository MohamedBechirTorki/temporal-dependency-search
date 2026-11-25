"""Integration test fixtures backed by live MongoDB and Neo4j."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.repositories.mongodb_repository import MongoRepository
from app.repositories.neo4j_repository import Neo4jRepository


def test_settings(**overrides) -> Settings:
    defaults = dict(
        mongo_db="temporal_deps_test",
        allow_tx_time_override=True,
        sync_worker_enabled=False,
        sync_inline=True,
        log_json=False,
    )
    defaults.update(overrides)
    return Settings(**defaults)


@pytest.fixture
def app_factory():
    clients: list[TestClient] = []

    def _make(**overrides) -> TestClient:
        s = test_settings(**overrides)
        app = create_app(s)
        client = TestClient(app)
        client.__enter__()
        clients.append(client)
        # Clean both stores before returning the client
        c = app.state.container
        c.mongo.drop_all()
        c.mongo.ensure_indexes()
        c.neo.wipe()
        c.sync.schema_ready = False
        c.sync.ensure_schema()
        return client

    yield _make

    for client in reversed(clients):
        client.__exit__(None, None, None)


@pytest.fixture
def client(app_factory) -> TestClient:
    return app_factory()


@pytest.fixture
def container(client: TestClient):
    return client.app.state.container
