"""Node endpoints."""
from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query, status

from app.api.deps import get_container
from app.api.schemas.common import ErrorResponse, Page, RelationshipType, UtcDatetime
from app.api.schemas.dependencies import DependencyVersion
from app.api.schemas.nodes import NodeCreate, NodeHistoryEntry, NodeUpdate, NodeView, NodeWriteResult
from app.container import Container

router = APIRouter(prefix="/api/v1/nodes", tags=["nodes"],
                   responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse},
                              422: {"model": ErrorResponse}, 503: {"model": ErrorResponse}})

C = Annotated[Container, Depends(get_container)]


@router.post("", status_code=status.HTTP_201_CREATED, response_model=NodeWriteResult)
def create_node(body: NodeCreate, c: C):
    return c.nodes.create(body)


@router.get("", response_model=Page[NodeView])
def list_nodes(c: C, type: str | None = None, include_deleted: bool = False,
               limit: Annotated[int, Query(ge=1, le=1000)] = 100, offset: Annotated[int, Query(ge=0)] = 0):
    return c.nodes.list(type, include_deleted, offset, limit)


@router.get("/{node_id}", response_model=NodeView)
def get_node(node_id: str, c: C):
    return c.nodes.get(node_id)


@router.patch("/{node_id}", response_model=NodeWriteResult)
def update_node(node_id: str, body: NodeUpdate, c: C):
    return c.nodes.update(node_id, body)


@router.delete("/{node_id}", response_model=NodeWriteResult)
def delete_node(node_id: str, c: C, effective_from: UtcDatetime | None = None,
                client_event_id: str | None = None, recorded_at: UtcDatetime | None = None):
    """Soft delete: ends the node's validity at ``effective_from`` (default: now).

    All dependencies touching the node are ended at the same valid time;
    their earlier history is preserved.
    """
    return c.nodes.delete(node_id, effective_from, client_event_id, recorded_at)


@router.get("/{node_id}/history", response_model=list[NodeHistoryEntry])
def node_history(node_id: str, c: C):
    return c.nodes.history(node_id)


@router.get("/{node_id}/dependencies", response_model=Page[DependencyVersion])
def node_dependencies(
    node_id: str, c: C,
    direction: Literal["outgoing", "incoming", "both"] = "outgoing",
    valid_at: UtcDatetime | None = None, known_at: UtcDatetime | None = None,
    relationship: RelationshipType | None = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100, offset: Annotated[int, Query(ge=0)] = 0,
):
    """Dependency records of a node from the authoritative store (MongoDB).

    outgoing = what the node depends on; incoming = what depends on the node.
    Bitemporal: valid_at (default now) and known_at (default now).
    """
    return c.dependencies.records_for_node(node_id, direction, valid_at, known_at, relationship, offset, limit)
