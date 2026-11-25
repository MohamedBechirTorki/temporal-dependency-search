"""Temporal endpoints: as-of reconstruction, historical diff/compare, and node timeline history."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from app.api.deps import get_container
from app.api.schemas.common import ErrorResponse
from app.api.schemas.temporal import AsOfRequest, AsOfResponse, CompareRequest, CompareResponse, NodeTimelineResponse
from app.container import Container
from app.graph.cypher import Direction

router = APIRouter(
    prefix="/api/v1/temporal",
    tags=["temporal"],
    responses={404: {"model": ErrorResponse}, 422: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
C = Annotated[Container, Depends(get_container)]


@router.post("/as-of", response_model=AsOfResponse)
def temporal_as_of(body: AsOfRequest, c: C):
    """Reconstruct the dependency graph (or node-rooted subgraph) at (valid_at, known_at)."""
    return c.temporal.as_of(body)


@router.post("/compare", response_model=CompareResponse)
def temporal_compare(body: CompareRequest, c: C):
    """Compare the dependency graph between two points in valid time and/or transaction time."""
    return c.temporal.compare(body)


@router.get("/history/{node_id}", response_model=NodeTimelineResponse)
def temporal_history(
    node_id: str,
    c: C,
    direction: Direction = "both",
    include_retracted: bool = True,
    include_superseded: bool = True,
):
    """Return full chronological node snapshots, dependency versions, and domain events."""
    return c.temporal.history(
        node_id,
        direction=direction,
        include_retracted=include_retracted,
        include_superseded=include_superseded,
    )
