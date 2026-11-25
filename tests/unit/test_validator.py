"""Unit tests for domain & batch validation and Pydantic schemas."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.api.schemas.dependencies import DependencyCorrection, DependencyCreate, DependencyModify
from app.api.schemas.nodes import NodeCreate
from app.ingestion.validator import MAX_METADATA_BYTES, validate_batch, validate_metadata

UTC = timezone.utc


def test_metadata_validation():
    assert validate_metadata({"env": "prod", "tags": ["a", {"b": 1}]}) == {"env": "prod", "tags": ["a", {"b": 1}]}
    with pytest.raises(ValueError):
        validate_metadata({"$set": 1})
    with pytest.raises(ValueError):
        validate_metadata({"a.b": 1})
    with pytest.raises(ValueError):
        validate_metadata({"nested": [{"$gt": 0}]})
    with pytest.raises(ValueError):
        validate_metadata({"blob": "x" * MAX_METADATA_BYTES})


def test_pydantic_schema_validation():
    with pytest.raises(ValidationError):
        NodeCreate(id="bad id with spaces")
    with pytest.raises(ValidationError):
        NodeCreate(id="n1", valid_from=datetime(2026, 1, 1))  # naive datetime
    with pytest.raises(ValidationError):
        DependencyCreate(
            source="a", target="b",
            valid_from=datetime(2026, 2, 1, tzinfo=UTC),
            valid_to=datetime(2026, 1, 1, tzinfo=UTC),
        )
    with pytest.raises(ValidationError):
        DependencyModify()  # empty modify
    with pytest.raises(ValidationError):
        DependencyCorrection()  # empty correction


def test_validate_batch_rules():
    nodes = [
        {"id": "A", "type": "service"},
        {"id": "B", "type": "database"},
    ]
    deps = [
        {"id": "d1", "source": "A", "target": "B", "valid_from": "2026-01-01T00:00:00Z", "valid_to": "2026-02-01T00:00:00Z"},
        # Non-overlapping same triple is allowed
        {"id": "d2", "source": "A", "target": "B", "valid_from": "2026-02-01T00:00:00Z"},
        # Self-loop produces warning, not error
        {"id": "d3", "source": "A", "target": "A"},
    ]
    rep = validate_batch(nodes, deps)
    assert rep.valid is True
    assert len(rep.warnings) == 1

    # Overlapping triple & unknown endpoint rejected
    bad_deps = [
        {"id": "d1", "source": "A", "target": "B", "valid_from": "2026-01-01T00:00:00Z"},
        {"id": "d2", "source": "A", "target": "B", "valid_from": "2026-01-15T00:00:00Z"},
        {"id": "d3", "source": "A", "target": "MISSING"},
    ]
    rep2 = validate_batch(nodes, bad_deps)
    assert rep2.valid is False
    codes = {e.code for e in rep2.errors}
    assert codes == {"overlapping_triple", "unknown_node"}
