"""Graph search endpoints (direct, multi-hop, shortest-path, reachability, cycle check)."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from app.api.deps import get_container
from app.api.schemas.common import ErrorResponse
from app.api.schemas.search import (
    CycleCheckRequest,
    CycleCheckResponse,
    DirectSearchRequest,
    DirectSearchResponse,
    MultiHopSearchRequest,
    MultiHopSearchResponse,
    ReachabilityRequest,
    ReachabilityResponse,
    ShortestPathRequest,
    ShortestPathResponse,
)
from app.container import Container

router = APIRouter(
    prefix="/api/v1/search",
    tags=["search"],
    responses={404: {"model": ErrorResponse}, 422: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
C = Annotated[Container, Depends(get_container)]


@router.post("/direct", response_model=DirectSearchResponse)
def direct_search(body: DirectSearchRequest, c: C):
    """Find direct dependencies of a node (outgoing, incoming, or both)."""
    return c.search.direct(body)


@router.post("/multi-hop", response_model=MultiHopSearchResponse)
def multi_hop_search(body: MultiHopSearchRequest, c: C):
    """Find transitive dependencies up to max_depth hops safely (handles cycles)."""
    return c.search.multi_hop(body)


@router.post("/shortest-path", response_model=ShortestPathResponse)
def shortest_path_search(body: ShortestPathRequest, c: C):
    """Find shortest dependency path between source_id and target_id (unweighted or weighted)."""
    return c.search.shortest_path(body)


@router.post("/reachability", response_model=ReachabilityResponse)
def reachability_search(body: ReachabilityRequest, c: C):
    """Determine whether source_id can reach target_id through dependency relationships."""
    return c.search.reachability(body)


@router.post("/check-cycle", response_model=CycleCheckResponse)
def check_cycle(body: CycleCheckRequest, c: C):
    """Check whether adding source_id -> target_id creates a directed cycle."""
    return c.search.check_cycle(body)
