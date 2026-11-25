"""Dependency request/response schemas."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from app.api.schemas.common import EntityId, Metadata, RelationshipType, UtcDatetime, WriteMeta

Weight = Field(ge=0.0, allow_inf_nan=False, description="Non-negative edge weight (cost)")
ChangeKind = Literal["change", "correction"]


class DependencyCreate(BaseModel):
    """Create dependency ``source -> target`` meaning *source depends on target*."""

    id: EntityId | None = Field(default=None, description="Generated if omitted")
    source: EntityId
    target: EntityId
    relationship: RelationshipType = "depends_on"
    weight: float = Field(default=1.0, ge=0.0, allow_inf_nan=False)
    valid_from: UtcDatetime | None = Field(default=None, description="Defaults to transaction time")
    valid_to: UtcDatetime | None = Field(default=None, description="null = open-ended")
    metadata: Metadata = {}
    client_event_id: str | None = Field(default=None, max_length=128)
    recorded_at: UtcDatetime | None = None

    @model_validator(mode="after")
    def _interval(self):
        if self.valid_from and self.valid_to and self.valid_to <= self.valid_from:
            raise ValueError("valid_to must be strictly after valid_from")
        return self


class DependencyModify(BaseModel):
    """Attribute change taking effect at ``effective_from`` (valid time).

    The new values hold until a later recorded change of the same attribute.
    Source, target and relationship are immutable (they define identity).
    """

    weight: float | None = Field(default=None, ge=0.0, allow_inf_nan=False)
    metadata: Metadata | None = Field(default=None, description="Replaces the whole metadata object")
    effective_from: UtcDatetime | None = Field(default=None, description="Defaults to transaction time")
    kind: ChangeKind = "change"
    client_event_id: str | None = Field(default=None, max_length=128)
    recorded_at: UtcDatetime | None = None

    @model_validator(mode="after")
    def _non_empty(self):
        if self.weight is None and self.metadata is None:
            raise ValueError("at least one of weight, metadata must be provided")
        return self


class DependencyRestore(BaseModel):
    weight: float | None = Field(default=None, ge=0.0, allow_inf_nan=False)
    metadata: Metadata | None = None
    effective_from: UtcDatetime | None = None
    kind: ChangeKind = "change"
    client_event_id: str | None = Field(default=None, max_length=128)
    recorded_at: UtcDatetime | None = None


class CorrectionEvent(BaseModel):
    type: Literal["created", "modified", "removed", "restored"]
    valid_at: UtcDatetime
    weight: float | None = Field(default=None, ge=0.0, allow_inf_nan=False)
    metadata: Metadata | None = None


class DependencyCorrection(BaseModel):
    """Retract erroneous events and/or add events with arbitrary valid times.

    Applied atomically. Retracted events remain stored (with ``retracted_at``)
    so that earlier beliefs can still be reconstructed via ``known_at``.
    """

    retract_event_ids: list[str] = Field(default_factory=list, max_length=1000)
    add_events: list[CorrectionEvent] = Field(default_factory=list, max_length=1000)
    reason: str | None = Field(default=None, max_length=1024)
    client_event_id: str | None = Field(default=None, max_length=128)
    recorded_at: UtcDatetime | None = None

    @model_validator(mode="after")
    def _non_empty(self):
        if not self.retract_event_ids and not self.add_events:
            raise ValueError("a correction must retract or add at least one event")
        return self


class DependencyVersion(BaseModel):
    """One materialised bitemporal version row."""

    version_id: str
    dependency_id: str
    source: str
    target: str
    relationship: str
    weight: float
    metadata: dict[str, Any]
    valid_from: datetime
    valid_to: datetime | None
    recorded_at: datetime = Field(description="Transaction-time start (tx_from)")
    superseded_at: datetime | None = Field(description="Transaction-time end (tx_to); null = current knowledge")


class DependencyView(BaseModel):
    id: str
    source: str
    target: str
    relationship: str
    status: Literal["active", "inactive", "scheduled"] = Field(
        description="At valid_at: active = valid; scheduled = only valid in the future; inactive otherwise"
    )
    current: DependencyVersion | None = Field(description="Version valid at valid_at as known at known_at")
    timeline: list[DependencyVersion] = Field(description="Valid-time timeline as known at known_at")
    history: list[DependencyVersion] | None = Field(
        default=None, description="All versions including superseded ones (include_history=true)"
    )
    valid_at: datetime
    known_at: datetime
    created_at: datetime
    updated_at: datetime
    rev: int


class DependencySummary(BaseModel):
    id: str
    source: str
    target: str
    relationship: str
    created_at: datetime
    updated_at: datetime
    rev: int


class DependencyWriteResult(BaseModel):
    dependency: DependencyView
    meta: WriteMeta


class DependencyEventView(BaseModel):
    event_id: str
    dependency_id: str
    type: str
    kind: str
    valid_at: datetime | None
    tx_time: datetime
    seq: int
    fields: dict[str, Any]
    retracted_at: datetime | None = None
    retracted_by: str | None = None
    cause: str | None = None
    client_event_id: str | None = None
