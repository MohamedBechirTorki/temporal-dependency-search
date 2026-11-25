"""Shared API schema building blocks."""
from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Generic, Literal, TypeVar

from pydantic import AfterValidator, AwareDatetime, BaseModel, Field

from app.domain.time import normalize
from app.ingestion.validator import (
    ID_PATTERN,
    MAX_METADATA_BYTES,
    NODE_TYPE_PATTERN,
    RELATIONSHIP_PATTERN,
    validate_metadata,
)

EntityId = Annotated[str, Field(pattern=ID_PATTERN, examples=["component_A"])]
RelationshipType = Annotated[str, Field(pattern=RELATIONSHIP_PATTERN, examples=["depends_on"])]
UtcDatetime = Annotated[AwareDatetime, AfterValidator(normalize)]
Metadata = Annotated[dict[str, Any], AfterValidator(validate_metadata)]

T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    items: list[T]
    total: int
    limit: int
    offset: int


class SyncInfo(BaseModel):
    """Projection status of the write in Neo4j.

    synced   - the Neo4j projection already reflects this write
    pending  - committed in MongoDB; Neo4j will be updated by the sync worker
    failed   - projection exhausted its retries (see /api/v1/sync/status)
    deferred - inline sync disabled; background worker will project it
    none     - the write did not change the graph projection
    """

    status: Literal["synced", "pending", "failed", "deferred", "none"]
    outbox_ids: list[str] = []


class ErrorBody(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = {}
    request_id: str | None = None


class ErrorResponse(BaseModel):
    error: ErrorBody


class WriteMeta(BaseModel):
    event_ids: list[str]
    tx_time: datetime
    duplicate: bool = False
    changed: bool = True
    sync: SyncInfo
    cycle_detected: bool = False
    cycle_path: list[str] | None = None
