"""Dependency endpoints."""
from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query, status

from app.api.deps import get_container
from app.api.schemas.common import EntityId, ErrorResponse, Page, RelationshipType, UtcDatetime
from app.api.schemas.dependencies import (
    DependencyCorrection, DependencyCreate, DependencyEventView, DependencyModify, DependencyRestore,
    DependencySummary, DependencyView, DependencyWriteResult,
)
from app.container import Container

router = APIRouter(prefix="/api/v1/dependencies", tags=["dependencies"],
                   responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse},
                              422: {"model": ErrorResponse}, 503: {"model": ErrorResponse}})

C = Annotated[Container, Depends(get_container)]


@router.post("", status_code=status.HTTP_201_CREATED, response_model=DependencyWriteResult)
def create_dependency(body: DependencyCreate, c: C):
    """Create ``source -> target`` (source depends on target)."""
    return c.dependencies.create(body)


@router.get("", response_model=Page[DependencySummary])
def list_dependencies(c: C, source: EntityId | None = None, target: EntityId | None = None,
                      relationship: RelationshipType | None = None,
                      limit: Annotated[int, Query(ge=1, le=1000)] = 100,
                      offset: Annotated[int, Query(ge=0)] = 0):
    return c.dependencies.list(source, target, relationship, offset, limit)


@router.get("/{dependency_id}", response_model=DependencyView)
def get_dependency(dependency_id: str, c: C, valid_at: UtcDatetime | None = None,
                   known_at: UtcDatetime | None = None, include_history: bool = False):
    return c.dependencies.get(dependency_id, valid_at, known_at, include_history)


@router.patch("/{dependency_id}", response_model=DependencyWriteResult)
def modify_dependency(dependency_id: str, body: DependencyModify, c: C):
    return c.dependencies.modify(dependency_id, body)


@router.delete("/{dependency_id}", response_model=DependencyWriteResult)
def remove_dependency(dependency_id: str, c: C, effective_from: UtcDatetime | None = None,
                      kind: Literal["change", "correction"] = "change",
                      client_event_id: str | None = None, recorded_at: UtcDatetime | None = None):
    """End the dependency's validity at ``effective_from`` (default now). History is kept."""
    return c.dependencies.remove(dependency_id, effective_from, kind, client_event_id, recorded_at)


@router.post("/{dependency_id}/restore", response_model=DependencyWriteResult)
def restore_dependency(dependency_id: str, body: DependencyRestore, c: C):
    return c.dependencies.restore(dependency_id, body)


@router.post("/{dependency_id}/corrections", response_model=DependencyWriteResult)
def correct_dependency(dependency_id: str, body: DependencyCorrection, c: C):
    return c.dependencies.correct(dependency_id, body)


@router.get("/{dependency_id}/events", response_model=list[DependencyEventView])
def dependency_events(dependency_id: str, c: C):
    return c.dependencies.events(dependency_id)
