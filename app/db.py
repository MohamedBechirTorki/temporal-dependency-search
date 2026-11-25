"""Database connection management (pooled clients, lifecycle, health)."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from neo4j import Driver, GraphDatabase
from pymongo import MongoClient
from pymongo.database import Database

from app.config import Settings

log = logging.getLogger(__name__)


@dataclass
class Databases:
    """Owns the pooled MongoDB client and Neo4j driver for the process."""

    settings: Settings
    mongo_client: MongoClient = field(init=False)
    neo4j_driver: Driver = field(init=False)

    def __post_init__(self) -> None:
        s = self.settings
        self.mongo_client = MongoClient(
            s.mongo_uri,
            maxPoolSize=s.mongo_max_pool_size,
            serverSelectionTimeoutMS=s.mongo_timeout_ms,
            tz_aware=True,
            uuidRepresentation="standard",
        )
        self.neo4j_driver = GraphDatabase.driver(
            s.neo4j_uri,
            auth=(s.neo4j_user, s.neo4j_password.get_secret_value()),
            max_connection_pool_size=s.neo4j_max_pool_size,
            connection_timeout=s.neo4j_timeout_s,
            max_transaction_retry_time=s.neo4j_max_retry_time_s,
        )

    @property
    def mongo(self) -> Database:
        return self.mongo_client[self.settings.mongo_db]

    @property
    def neo4j_database(self) -> str:
        return self.settings.neo4j_database

    # ------------------------------------------------------------------ health
    def check_mongo(self) -> dict[str, Any]:
        t0 = time.perf_counter()
        try:
            self.mongo_client.admin.command("ping")
            hello = self.mongo_client.admin.command("hello")
            return {
                "status": "ok",
                "latency_ms": round((time.perf_counter() - t0) * 1000, 3),
                "replica_set": hello.get("setName"),
                "transactions_supported": bool(hello.get("setName")),
            }
        except Exception as exc:  # report, never raise from health
            log.warning("mongo health check failed", extra={"error": str(exc)})
            return {"status": "error", "error": type(exc).__name__}

    def check_neo4j(self) -> dict[str, Any]:
        t0 = time.perf_counter()
        try:
            self.neo4j_driver.verify_connectivity()
            info = self.neo4j_driver.get_server_info()
            return {
                "status": "ok",
                "latency_ms": round((time.perf_counter() - t0) * 1000, 3),
                "agent": info.agent,
            }
        except Exception as exc:
            log.warning("neo4j health check failed", extra={"error": str(exc)})
            return {"status": "error", "error": type(exc).__name__}

    def close(self) -> None:
        try:
            self.neo4j_driver.close()
        finally:
            self.mongo_client.close()
