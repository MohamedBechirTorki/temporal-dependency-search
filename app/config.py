"""Environment-based configuration (no hardcoded credentials)."""
from __future__ import annotations

from enum import Enum
from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class CyclePolicy(str, Enum):
    """What to do when a new/modified dependency would close a cycle."""

    ALLOW = "allow"    # accept silently (cycle reported in response)
    WARN = "warn"      # accept, log a warning, report cycle in response
    REJECT = "reject"  # refuse with HTTP 409 and the cycle path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "development"
    log_level: str = "INFO"
    log_json: bool = True

    mongo_uri: str = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
    mongo_db: str = "temporal_deps"
    mongo_max_pool_size: int = 50
    mongo_timeout_ms: int = 5000

    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: SecretStr = SecretStr("")
    neo4j_database: str = "neo4j"
    neo4j_max_pool_size: int = 50
    neo4j_timeout_s: float = 10.0

    cycle_policy: CyclePolicy = CyclePolicy.ALLOW
    max_traversal_depth: int = Field(default=25, ge=1, le=1000)

    neo4j_max_retry_time_s: float = 5.0

    # Synchronisation (transactional outbox -> Neo4j projection)
    sync_worker_enabled: bool = True
    sync_inline: bool = True  # try to project right after the Mongo commit (read-your-writes)
    sync_poll_interval_seconds: float = 0.5
    sync_max_attempts: int = 8
    sync_batch_size: int = 200
    sync_lease_seconds: float = 30.0
    sync_backoff_base_seconds: float = 0.5
    sync_backoff_max_seconds: float = 60.0

    # Research/replay only: let trusted ingestion set transaction time
    # (``recorded_at``). Disabled by default because transaction time is
    # normally system-assigned.
    allow_tx_time_override: bool = False


@lru_cache
def get_settings() -> Settings:
    return Settings()
