"""Search request and response schemas."""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from app.api.schemas.common import EntityId, RelationshipType, UtcDatetime
from app.graph.cypher import Direction


class SearchNodeSummary(BaseModel):
    id: str
    name: str
    type: str
    deleted: bool


class SearchEdge(BaseModel):
    dependency_id: str
    version_id: str
    source: str
    target: str
    relationship: str
    weight: float
    valid_from: datetime
    valid_to: datetime | None
    recorded_at: datetime


class DirectSearchRequest(BaseModel):
    node_id: EntityId
    direction: Direction = "outgoing"
    relationship_types: list[RelationshipType] | None = None
    valid_at: UtcDatetime | None = None
    known_at: UtcDatetime | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)
    dataset: str = "default"


class DirectSearchItem(BaseModel):
    direction: Literal["outgoing", "incoming", "self"]
    neighbor: SearchNodeSummary
    edge: SearchEdge


class DirectSearchResponse(BaseModel):
    node_id: str
    direction: Direction
    valid_at: datetime
    known_at: datetime
    items: list[DirectSearchItem]
    total: int
    limit: int
    offset: int
    neo4j_query_ms: float
    execution_time_ms: float


class MultiHopSearchRequest(BaseModel):
    node_id: EntityId
    direction: Direction = "outgoing"
    max_depth: int = Field(default=3, ge=1, le=1000)
    relationship_types: list[RelationshipType] | None = None
    valid_at: UtcDatetime | None = None
    known_at: UtcDatetime | None = None
    include_paths: bool = True
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)
    dataset: str = "default"


class MultiHopItem(BaseModel):
    node: SearchNodeSummary
    hops: int
    path_nodes: list[str] | None = None
    path_edges: list[SearchEdge] | None = None


class MultiHopSearchResponse(BaseModel):
    node_id: str
    direction: Direction
    max_depth: int
    valid_at: datetime
    known_at: datetime
    items: list[MultiHopItem]
    total: int
    limit: int
    offset: int
    neo4j_query_ms: float
    execution_time_ms: float


class ShortestPathRequest(BaseModel):
    source_id: EntityId
    target_id: EntityId
    direction: Direction = "outgoing"
    weighted: bool = False
    max_depth: int | None = Field(default=None, ge=1, le=1000)
    relationship_types: list[RelationshipType] | None = None
    valid_at: UtcDatetime | None = None
    known_at: UtcDatetime | None = None
    dataset: str = "default"


class ShortestPathResponse(BaseModel):
    source_id: str
    target_id: str
    direction: Direction
    weighted: bool
    found: bool
    hops: int | None
    total_weight: float | None
    nodes: list[SearchNodeSummary]
    edges: list[SearchEdge]
    algorithm: str
    valid_at: datetime
    known_at: datetime
    neo4j_query_ms: float
    execution_time_ms: float


class ReachabilityRequest(BaseModel):
    source_id: EntityId
    target_id: EntityId
    direction: Direction = "outgoing"
    max_depth: int | None = Field(default=None, ge=1, le=1000)
    relationship_types: list[RelationshipType] | None = None
    valid_at: UtcDatetime | None = None
    known_at: UtcDatetime | None = None
    include_path: bool = True
    dataset: str = "default"


class ReachabilityResponse(BaseModel):
    source_id: str
    target_id: str
    direction: Direction
    reachable: bool
    hops: int | None
    path_nodes: list[str] | None = None
    path_edges: list[SearchEdge] | None = None
    valid_at: datetime
    known_at: datetime
    neo4j_query_ms: float
    execution_time_ms: float


class CycleCheckRequest(BaseModel):
    source_id: EntityId
    target_id: EntityId
    relationship_types: list[RelationshipType] | None = None
    max_depth: int | None = Field(default=None, ge=1, le=1000)
    valid_at: UtcDatetime | None = None
    known_at: UtcDatetime | None = None
    dataset: str = "default"


class CycleCheckResponse(BaseModel):
    source_id: str
    target_id: str
    creates_cycle: bool
    cycle_length: int | None
    cycle_nodes: list[str]
    existing_edges: list[SearchEdge]
    valid_at: datetime
    known_at: datetime
    neo4j_query_ms: float
    execution_time_ms: float
