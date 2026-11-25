"""Synchronisation admin schemas."""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class OutboxFailure(BaseModel):
    outbox_id: str
    aggregate_type: str
    aggregate_id: str
    attempts: int
    status: str
    last_error_type: str | None
    next_attempt_at: datetime | None
    created_at: datetime


class SyncStatus(BaseModel):
    counts: dict[str, int]
    oldest_pending_created_at: datetime | None
    pending_lag_seconds: float | None
    failures: list[OutboxFailure]
    worker_running: bool
    neo4j_schema_ready: bool


class ProcessReport(BaseModel):
    claimed: int = 0
    done: int = 0
    retried: int = 0
    dead_lettered: int = 0
    duration_ms: float = 0.0


class ReconcileReport(BaseModel):
    consistent: bool
    mongo_nodes: int
    neo4j_nodes: int
    mongo_dependencies: int
    mongo_versions: int
    neo4j_versions: int
    stale_nodes: list[str]
    missing_nodes: list[str]
    orphan_nodes: list[str]
    stale_dependencies: list[str]
    orphan_dependencies: list[str]
    repaired: bool = False
    duration_ms: float


class RebuildReport(BaseModel):
    nodes: int
    dependencies: int
    versions: int
    duration_ms: float
