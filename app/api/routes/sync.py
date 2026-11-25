"""Synchronisation admin endpoints (MongoDB -> Neo4j projection)."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.api.deps import get_container
from app.api.schemas.sync import ProcessReport, RebuildReport, ReconcileReport, SyncStatus
from app.container import Container

router = APIRouter(prefix="/api/v1/sync", tags=["sync"])
C = Annotated[Container, Depends(get_container)]


@router.get("/status", response_model=SyncStatus)
def sync_status(c: C):
    return c.sync.status()


@router.post("/process", response_model=ProcessReport)
def process(c: C, max_batches: Annotated[int, Query(ge=1, le=10000)] = 100):
    """Drain claimable outbox entries now."""
    return c.sync.drain(max_batches)


@router.post("/retry-failed")
def retry_failed(c: C) -> dict:
    """Move dead-lettered entries back to pending."""
    return {"reset": c.sync.retry_failed()}


@router.post("/reconcile", response_model=ReconcileReport)
def reconcile(c: C, repair: bool = False):
    """Compare MongoDB with the Neo4j projection; optionally repair drift."""
    return c.sync.reconcile(repair)


@router.post("/rebuild", response_model=RebuildReport)
def rebuild(c: C):
    """Wipe the Neo4j projection and rebuild it from MongoDB."""
    return c.sync.rebuild()
