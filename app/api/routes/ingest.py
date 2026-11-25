"""Bulk ingestion endpoints."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from app.api.deps import get_container
from app.api.schemas.common import ErrorResponse
from app.api.schemas.ingest import BatchIngestRequest, BatchIngestResponse
from app.container import Container

router = APIRouter(
    prefix="/api/v1/ingest",
    tags=["ingestion"],
    responses={409: {"model": ErrorResponse}, 422: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
C = Annotated[Container, Depends(get_container)]


@router.post("/batch", status_code=status.HTTP_201_CREATED, response_model=BatchIngestResponse)
def ingest_batch(body: BatchIngestRequest, c: C):
    """Ingest nodes, initial dependencies, and optional temporal events in bulk."""
    metrics = c.importer.ingest_batch(
        [n.model_dump() for n in body.nodes],
        [d.model_dump() for d in body.dependencies],
        [e.model_dump(exclude_none=True) for e in body.events],
        validate=body.validate_batch,
        sync=body.sync,
    )
    return BatchIngestResponse(**metrics.to_dict())
