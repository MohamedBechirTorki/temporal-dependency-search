"""Node request/response schemas."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.api.schemas.common import NODE_TYPE_PATTERN, EntityId, Metadata, UtcDatetime, WriteMeta


class NodeCreate(BaseModel):
    id: EntityId
    name: str | None = Field(default=None, max_length=256, description="Defaults to id")
    type: str = Field(default="component", pattern=NODE_TYPE_PATTERN, description="Free-form node type")
    description: str | None = Field(default=None, max_length=4096)
    metadata: Metadata = {}
    valid_from: UtcDatetime | None = Field(
        default=None, description="Valid-time start of the node. null = since forever."
    )
    client_event_id: str | None = Field(default=None, max_length=128, description="Idempotency key")
    recorded_at: UtcDatetime | None = Field(
        default=None, description="Transaction-time override (only if ALLOW_TX_TIME_OVERRIDE=true)"
    )


class NodeUpdate(BaseModel):
    name: str | None = Field(default=None, max_length=256)
    type: str | None = Field(default=None, pattern=NODE_TYPE_PATTERN)
    description: str | None = Field(default=None, max_length=4096)
    metadata: Metadata | None = None
    client_event_id: str | None = Field(default=None, max_length=128)
    recorded_at: UtcDatetime | None = None


class NodeView(BaseModel):
    id: str
    name: str
    type: str
    description: str | None
    metadata: dict[str, Any]
    valid_from: datetime | None
    valid_to: datetime | None
    deleted: bool
    created_at: datetime
    updated_at: datetime
    rev: int


class NodeWriteResult(BaseModel):
    node: NodeView
    meta: WriteMeta
    affected_dependencies: list[str] = []


class NodeHistoryEntry(BaseModel):
    node_id: str
    rev: int
    operation: Literal["created", "updated", "deleted"]
    tx_time: datetime
    event_id: str
    snapshot: dict[str, Any]
