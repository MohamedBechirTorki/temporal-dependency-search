"""Bulk ingestion API schemas."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from app.api.schemas.common import EntityId, Metadata, RelationshipType, UtcDatetime
from app.api.schemas.dependencies import CorrectionEvent, DependencyCreate
from app.api.schemas.nodes import NodeCreate


class BatchEventItem(BaseModel):
    dependency_id: EntityId
    type: Literal["created", "modified", "removed", "restored", "corrected"]
    effective_from: UtcDatetime | None = None
    valid_at: UtcDatetime | None = None
    weight: float | None = Field(default=None, ge=0.0, allow_inf_nan=False)
    metadata: Metadata | None = None
    kind: Literal["change", "correction"] | None = None
    retract_event_ids: list[str] = Field(default_factory=list)
    add_events: list[CorrectionEvent] = Field(default_factory=list)
    reason: str | None = None
    client_event_id: str | None = Field(default=None, max_length=128)
    recorded_at: UtcDatetime | None = None


class BatchIngestRequest(BaseModel):
    nodes: list[NodeCreate] = Field(default_factory=list, max_length=10000)
    dependencies: list[DependencyCreate] = Field(default_factory=list, max_length=10000)
    events: list[BatchEventItem] = Field(default_factory=list, max_length=10000)
    sync: bool = True
    validate_batch: bool = True


class BatchIngestResponse(BaseModel):
    nodes_ingested: int
    dependencies_ingested: int
    events_ingested: int
    versions_created: int
    versions_closed: int
    mongo_ingest_ms: float
    sync_ms: float
    total_ms: float
    sync_report: dict[str, Any] | None = None
