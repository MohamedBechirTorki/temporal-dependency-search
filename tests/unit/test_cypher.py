"""Unit tests for Cypher pattern builders and parameter helpers."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.graph.cypher import edge_filter, normalize_rels, qpp_hop_pattern, quantifier, query_params

UTC = timezone.utc


def test_qpp_hop_pattern_directions_and_rel_filter():
    out = qpp_hop_pattern("outgoing", has_rel_filter=False)
    assert out.startswith("(()-[r:DEPENDS_ON WHERE ") and out.endswith("->())")
    assert "r.relationship IN" not in out

    inc = qpp_hop_pattern("incoming", has_rel_filter=True)
    assert inc.startswith("(()<-[r:DEPENDS_ON WHERE ") and inc.endswith("-())")
    assert "r.relationship IN $relationships" in inc

    both = qpp_hop_pattern("both", has_rel_filter=False)
    assert both.startswith("(()-[r:DEPENDS_ON WHERE ") and both.endswith("-())")

    with pytest.raises(ValueError):
        qpp_hop_pattern("sideways")  # type: ignore[arg-type]


def test_quantifier_validation():
    assert quantifier(1, None) == "+"
    assert quantifier(2, None) == "{2,}"
    assert quantifier(1, 5) == "{1, 5}"
    with pytest.raises(ValueError):
        quantifier(0, 3)
    with pytest.raises(ValueError):
        quantifier(4, 2)


def test_normalize_rels_and_params():
    assert normalize_rels(None) is None
    assert normalize_rels([]) is None
    assert normalize_rels(["calls", "uses", "calls"]) == ["calls", "uses"]
    t = datetime(2026, 1, 1, tzinfo=UTC)
    assert "relationships" not in query_params(t, t, None)
    assert query_params(t, t, ["calls"])["relationships"] == ["calls"]
    assert "r.relationship IN $relationships" in edge_filter(has_rel_filter=True)
