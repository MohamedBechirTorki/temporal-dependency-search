"""Temporal query schemas: as-of reconstruction, historical diff/compare, and full history."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.api.schemas.common import EntityId, RelationshipType, UtcDatetime
from app.api.schemas.dependencies import DependencyEventView, DependencySummary, DependencyVersion
from app.api.schemas.nodes import NodeHistoryEntry, NodeView
from app.graph.cypher import Direction


class AsOfRequest(BaseModel):
    node_id: EntityId | None = Field(default=None, description="If omitted, reconstructs the whole graph")
    valid_at: UtcDatetime
    known_at: UtcDatetime | None = Field(default=None, description="Transaction-time cutoff (default: now)")
    direction: Direction = "outgoing"
    max_depth: int = Field(default=3, ge=1, le=1000)
    relationship_types: list[RelationshipType] | None = None
    include_nodes: bool = True
    limit: int = Field(default=1000, ge=1, le=10000)
    offset: int = Field(default=0, ge=0)
    dataset: str = "default"


class AsOfNode(BaseModel):
    id: str
    name: str
    type: str
    description: str | None = None
    metadata: dict[str, Any] = {}
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    deleted: bool = False
    hops: int | None = None


class AsOfEdge(BaseModel):
    dependency_id: str
    version_id: str
    source: str
    target: str
    relationship: str
    weight: float
    metadata: dict[str, Any] = {}
    valid_from: datetime
    valid_to: datetime | None
    recorded_at: datetime


class AsOfResponse(BaseModel):
    node_id: str | None
    valid_at: datetime
    known_at: datetime
    node_active_at_time: bool | None = None
    nodes: list[AsOfNode]
    edges: list[AsOfEdge]
    total_nodes: int
    total_edges: int
    limit: int
    offset: int
    neo4j_query_ms: float
    execution_time_ms: float


class CompareRequest(BaseModel):
    from_valid_at: UtcDatetime
    to_valid_at: UtcDatetime
    known_at: UtcDatetime | None = Field(default=None, description="Shared transaction time if from/to omitted")
    from_known_at: UtcDatetime | None = None
    to_known_at: UtcDatetime | None = None
    node_id: EntityId | None = Field(default=None, description="Optional node scope")
    direction: Direction = "both"
    max_depth: int = Field(default=1, ge=1, le=1000)
    relationship_types: list[RelationshipType] | None = None
    dataset: str = "default"


class EdgeDiff(BaseModel):
    dependency_id: str
    source: str
    target: str
    relationship: str
    before: AsOfEdge
    after: AsOfEdge
    changed_attributes: list[str]
    attribute_changes: dict[str, Any]


class CompareSummary(BaseModel):
    from_edge_count: int
    to_edge_count: int
    added_count: int
    removed_count: int
    modified_count: int
    unchanged_count: int


class CompareResponse(BaseModel):
    from_valid_at: datetime
    to_valid_at: datetime
    from_known_at: datetime
    to_known_at: datetime
    node_id: str | None
    summary: CompareSummary
    added: list[AsOfEdge]
    removed: list[AsOfEdge]
    modified: list[EdgeDiff]
    neo4j_query_ms: float
    execution_time_ms: float


class NodeTimelineResponse(BaseModel):
    node: NodeView
    node_history: list[NodeHistoryEntry]
    dependencies: list[DependencySummary]
    versions: list[DependencyVersion]
    events: list[DependencyEventView]
