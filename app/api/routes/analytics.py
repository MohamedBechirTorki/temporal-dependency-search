"""Graph analytics and impact analysis endpoints."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.api.deps import get_container
from app.api.schemas.analytics import (
    AnalyticsCyclesResponse,
    AnalyticsOverviewResponse,
    CentralityResponse,
    ImpactRequest,
    ImpactResponse,
)
from app.api.schemas.common import ErrorResponse, RelationshipType, UtcDatetime
from app.container import Container

router = APIRouter(
    prefix="/api/v1/analytics",
    tags=["analytics"],
    responses={404: {"model": ErrorResponse}, 422: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
C = Annotated[Container, Depends(get_container)]


@router.get("/overview", response_model=AnalyticsOverviewResponse)
def analytics_overview(
    c: C,
    valid_at: UtcDatetime | None = None,
    at: UtcDatetime | None = None,
    known_at: UtcDatetime | None = None,
    relationship_types: list[RelationshipType] | None = Query(default=None),
    top_k: Annotated[int, Query(ge=1, le=200)] = 10,
    dataset: str = Query(default="default"),
):
    """Graph-wide statistics: node/edge counts, density, degree distributions, WCC/SCC counts, top hubs."""
    return c.analytics.overview(
        valid_at=at or valid_at,
        known_at=known_at,
        relationship_types=relationship_types,
        top_k=top_k,
        dataset=dataset,
    )


@router.get("/centrality", response_model=CentralityResponse)
def analytics_centrality(
    c: C,
    valid_at: UtcDatetime | None = None,
    at: UtcDatetime | None = None,
    known_at: UtcDatetime | None = None,
    relationship_types: list[RelationshipType] | None = Query(default=None),
    top_k: Annotated[int, Query(ge=1, le=200)] = 20,
    betweenness_samples: Annotated[int | None, Query(ge=1, le=10000)] = None,
    node_ids: list[str] | None = Query(default=None),
    damping: Annotated[float, Query(gt=0.0, lt=1.0)] = 0.85,
    dataset: str = Query(default="default"),
):
    """Degree centrality, PageRank, and Brandes betweenness centrality (exact or sampled)."""
    return c.analytics.centrality(
        valid_at=at or valid_at,
        known_at=known_at,
        relationship_types=relationship_types,
        top_k=top_k,
        betweenness_samples=betweenness_samples,
        node_ids_filter=node_ids,
        damping=damping,
        dataset=dataset,
    )


@router.get("/cycles", response_model=AnalyticsCyclesResponse)
def analytics_cycles(
    c: C,
    valid_at: UtcDatetime | None = None,
    at: UtcDatetime | None = None,
    known_at: UtcDatetime | None = None,
    relationship_types: list[RelationshipType] | None = Query(default=None),
    max_cycles: Annotated[int, Query(ge=1, le=1000)] = 100,
    max_cycle_length: Annotated[int, Query(ge=2, le=50)] = 15,
    dataset: str = Query(default="default"),
):
    """Graph-wide cycle detection using strongly connected components and self-loops."""
    return c.analytics.cycles(
        valid_at=at or valid_at,
        known_at=known_at,
        relationship_types=relationship_types,
        max_cycles=max_cycles,
        max_cycle_length=max_cycle_length,
        dataset=dataset,
    )


@router.post("/impact", response_model=ImpactResponse)
def analytics_impact(body: ImpactRequest, c: C):
    """Impact analysis: identify all components directly or indirectly depending on node_id."""
    return c.analytics.impact(body)
