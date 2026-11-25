"""Analytics and impact analysis request/response schemas."""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from app.api.schemas.common import EntityId, RelationshipType, UtcDatetime
from app.api.schemas.search import SearchEdge, SearchNodeSummary


class NodeDegreeSummary(BaseModel):
    id: str
    name: str
    type: str
    in_degree: int
    out_degree: int
    total_degree: int


class AnalyticsOverviewResponse(BaseModel):
    valid_at: datetime
    known_at: datetime
    node_count: int
    active_edge_count: int
    unique_directed_pairs: int
    self_loop_count: int
    density: float
    avg_in_degree: float
    avg_out_degree: float
    max_in_degree: int
    max_out_degree: int
    weakly_connected_component_count: int
    largest_wcc_size: int
    strongly_connected_component_count: int
    non_trivial_scc_count: int
    has_cycles: bool
    most_depended_upon: list[NodeDegreeSummary]
    most_dependent: list[NodeDegreeSummary]
    neo4j_query_ms: float
    compute_ms: float
    execution_time_ms: float


class CentralityEntry(BaseModel):
    id: str
    name: str
    type: str
    in_degree: int
    out_degree: int
    in_degree_centrality: float
    out_degree_centrality: float
    pagerank: float
    betweenness: float


class CentralityResponse(BaseModel):
    valid_at: datetime
    known_at: datetime
    node_count: int
    edge_count: int
    pagerank_damping: float
    betweenness_algorithm: str
    betweenness_sample_size: int
    top_by_pagerank: list[CentralityEntry]
    top_by_betweenness: list[CentralityEntry]
    top_by_in_degree: list[CentralityEntry]
    selected_nodes: list[CentralityEntry] = []
    neo4j_query_ms: float
    compute_ms: float
    execution_time_ms: float


class SccEntry(BaseModel):
    nodes: list[str]
    size: int


class AnalyticsCyclesResponse(BaseModel):
    valid_at: datetime
    known_at: datetime
    has_cycles: bool
    non_trivial_scc_count: int
    self_loop_count: int
    strongly_connected_components: list[SccEntry]
    self_loops: list[str]
    sample_cycles: list[list[str]]
    neo4j_query_ms: float
    compute_ms: float
    execution_time_ms: float


class ImpactRequest(BaseModel):
    node_id: EntityId
    max_depth: int = Field(default=10, ge=1, le=1000)
    relationship_types: list[RelationshipType] | None = None
    valid_at: UtcDatetime | None = None
    known_at: UtcDatetime | None = None
    include_paths: bool = True
    dataset: str = "default"


class AffectedNode(BaseModel):
    node: SearchNodeSummary
    hops: int
    dependency_path: list[str] | None = None
    path_edges: list[SearchEdge] | None = None


class ImpactResponse(BaseModel):
    target_node_id: str
    direction_convention: str
    max_depth: int
    valid_at: datetime
    known_at: datetime
    directly_affected: list[AffectedNode]
    indirectly_affected: list[AffectedNode]
    directly_affected_count: int
    indirectly_affected_count: int
    total_affected: int
    max_impact_depth: int
    neo4j_query_ms: float
    execution_time_ms: float
