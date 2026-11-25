"""Phase 3 integration & correctness tests: direct, multi-hop, shortest path, reachability, cycles."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.config import CyclePolicy
from app.graph import shortest_path as sp_mod

pytestmark = pytest.mark.integration


def _seed_graph(client: TestClient, nodes: list[str], deps: list[dict]) -> None:
    r = client.post(
        "/api/v1/ingest/batch",
        json={
            "nodes": [
                {"id": nid, "type": "service", "valid_from": "2026-01-01T00:00:00Z", "recorded_at": "2026-01-01T00:00:00Z"}
                for nid in nodes
            ],
            "dependencies": [
                {
                    "valid_from": "2026-01-02T00:00:00Z",
                    "recorded_at": "2026-01-02T00:00:00Z",
                    **d,
                }
                for d in deps
            ],
            "sync": True,
        },
    )
    assert r.status_code == 201, r.text


def test_direct_and_isolated_and_self_loop(client: TestClient):
    _seed_graph(
        client,
        ["A", "B", "C", "ISOLATED"],
        [
            {"id": "d_ab", "source": "A", "target": "B", "relationship": "calls", "weight": 1.0},
            {"id": "d_ca", "source": "C", "target": "A", "relationship": "reads", "weight": 2.0},
            {"id": "d_aa", "source": "A", "target": "A", "relationship": "self_check", "weight": 0.5},
        ],
    )

    # Missing node -> 404
    assert client.post("/api/v1/search/direct", json={"node_id": "NOPE"}).status_code == 404

    # Outgoing from A: A->B and self-loop A->A
    r_out = client.post("/api/v1/search/direct", json={"node_id": "A", "direction": "outgoing"})
    assert r_out.status_code == 200
    assert {item["edge"]["dependency_id"] for item in r_out.json()["items"]} == {"d_ab", "d_aa"}

    # Incoming to A: C->A and self-loop A->A
    r_in = client.post("/api/v1/search/direct", json={"node_id": "A", "direction": "incoming"})
    assert {item["edge"]["dependency_id"] for item in r_in.json()["items"]} == {"d_ca", "d_aa"}

    # Both with self-loop appears once (direction == "self")
    r_both = client.post("/api/v1/search/direct", json={"node_id": "A", "direction": "both"})
    assert r_both.json()["total"] == 3
    dirs = {item["edge"]["dependency_id"]: item["direction"] for item in r_both.json()["items"]}
    assert dirs == {"d_ab": "outgoing", "d_ca": "incoming", "d_aa": "self"}

    # Relationship filter
    r_rel = client.post(
        "/api/v1/search/direct",
        json={"node_id": "A", "direction": "both", "relationship_types": ["calls"]},
    )
    assert r_rel.json()["total"] == 1
    assert r_rel.json()["items"][0]["neighbor"]["id"] == "B"

    # Isolated node
    r_iso = client.post("/api/v1/search/direct", json={"node_id": "ISOLATED", "direction": "both"})
    assert r_iso.json()["total"] == 0
    assert r_iso.json()["items"] == []


def test_multi_hop_diamond_cycles_deep_and_temporal(client: TestClient):
    # Graph: A -> B -> C -> D, A -> E -> D, plus cycle C -> A, plus disconnected X -> Y
    _seed_graph(
        client,
        ["A", "B", "C", "D", "E", "X", "Y"],
        [
            {"id": "d_ab", "source": "A", "target": "B"},
            {"id": "d_bc", "source": "B", "target": "C"},
            {"id": "d_cd", "source": "C", "target": "D"},
            {"id": "d_ae", "source": "A", "target": "E"},
            {"id": "d_ed", "source": "E", "target": "D"},
            {"id": "d_ca", "source": "C", "target": "A"},  # cycle
            {"id": "d_xy", "source": "X", "target": "Y"},
        ],
    )

    # Depth 1 from A: B, E
    r1 = client.post("/api/v1/search/multi-hop", json={"node_id": "A", "max_depth": 1}).json()
    assert [i["node"]["id"] for i in r1["items"]] == ["B", "E"]

    # Depth 3 from A: B(1), E(1), C(2), D(2 via E, shorter than A->B->C->D!)
    r3 = client.post("/api/v1/search/multi-hop", json={"node_id": "A", "max_depth": 3}).json()
    hops_by_id = {i["node"]["id"]: i["hops"] for i in r3["items"]}
    assert hops_by_id == {"B": 1, "E": 1, "C": 2, "D": 2}
    d_item = next(i for i in r3["items"] if i["node"]["id"] == "D")
    assert d_item["path_nodes"] == ["A", "E", "D"]

    # Remove A->E at 2026-02-01: after Feb, D is 3 hops away via A->B->C->D
    assert (
        client.delete(
            "/api/v1/dependencies/d_ae",
            params={"effective_from": "2026-02-01T00:00:00Z", "recorded_at": "2026-02-01T00:00:00Z"},
        ).status_code
        == 200
    )

    r_jan = client.post(
        "/api/v1/search/multi-hop",
        json={"node_id": "A", "max_depth": 3, "valid_at": "2026-01-15T00:00:00Z"},
    ).json()
    assert {i["node"]["id"]: i["hops"] for i in r_jan["items"]}["D"] == 2

    r_feb = client.post(
        "/api/v1/search/multi-hop",
        json={"node_id": "A", "max_depth": 3, "valid_at": "2026-02-15T00:00:00Z"},
    ).json()
    assert {i["node"]["id"]: i["hops"] for i in r_feb["items"]}["D"] == 3

    # Depth limit exceeding MAX_TRAVERSAL_DEPTH -> 422
    assert client.post("/api/v1/search/multi-hop", json={"node_id": "A", "max_depth": 999}).status_code == 422


def test_deep_path_20_hops(client: TestClient):
    nodes = [f"n{i}" for i in range(21)]
    deps = [{"id": f"d{i}", "source": f"n{i}", "target": f"n{i+1}"} for i in range(20)]
    _seed_graph(client, nodes, deps)

    r = client.post("/api/v1/search/multi-hop", json={"node_id": "n0", "max_depth": 20, "include_paths": False})
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 20
    assert body["items"][-1]["node"]["id"] == "n20"
    assert body["items"][-1]["hops"] == 20


def test_shortest_path_unweighted_weighted_and_constraints(client: TestClient, monkeypatch):
    # A -> D (weight 10.0, 1 hop)
    # A -> B -> C -> D (weights 1.0 + 1.0 + 1.0 = 3.0, 3 hops)
    # Parallel edge A -> B (relationship "backup", weight 50.0)
    # Plus extra nodes E -> F -> G for testing bulk fallback
    _seed_graph(
        client,
        ["A", "B", "C", "D", "E", "F", "G"],
        [
            {"id": "d_ad", "source": "A", "target": "D", "relationship": "calls", "weight": 10.0},
            {"id": "d_ab", "source": "A", "target": "B", "relationship": "calls", "weight": 1.0},
            {"id": "d_ab_bak", "source": "A", "target": "B", "relationship": "backup", "weight": 50.0},
            {"id": "d_bc", "source": "B", "target": "C", "relationship": "calls", "weight": 1.0},
            {"id": "d_cd", "source": "C", "target": "D", "relationship": "calls", "weight": 1.0},
            {"id": "d_de", "source": "D", "target": "E", "relationship": "calls", "weight": 0.0},
            {"id": "d_ef", "source": "E", "target": "F", "relationship": "calls", "weight": 2.0},
        ],
    )

    # Unweighted: 1 hop A -> D
    r_unw = client.post("/api/v1/search/shortest-path", json={"source_id": "A", "target_id": "D"}).json()
    assert r_unw["found"] is True
    assert r_unw["hops"] == 1
    assert r_unw["total_weight"] == 10.0
    assert [n["id"] for n in r_unw["nodes"]] == ["A", "D"]

    # Weighted: 3 hops A -> B -> C -> D (total weight 3.0 < 10.0)
    r_w = client.post(
        "/api/v1/search/shortest-path",
        json={"source_id": "A", "target_id": "D", "weighted": True},
    ).json()
    assert r_w["found"] is True
    assert r_w["hops"] == 3
    assert r_w["total_weight"] == 3.0
    assert [n["id"] for n in r_w["nodes"]] == ["A", "B", "C", "D"]
    assert [e["dependency_id"] for e in r_w["edges"]] == ["d_ab", "d_bc", "d_cd"]

    # Weighted with max_depth=2 must fall back to 1-hop A -> D (weight 10.0)
    r_w2 = client.post(
        "/api/v1/search/shortest-path",
        json={"source_id": "A", "target_id": "D", "weighted": True, "max_depth": 2},
    ).json()
    assert r_w2["found"] is True
    assert r_w2["hops"] == 1
    assert r_w2["total_weight"] == 10.0

    # Zero-weight edge D -> E -> F
    r_zero = client.post(
        "/api/v1/search/shortest-path",
        json={"source_id": "A", "target_id": "F", "weighted": True},
    ).json()
    assert r_zero["total_weight"] == 5.0
    assert [n["id"] for n in r_zero["nodes"]] == ["A", "B", "C", "D", "E", "F"]

    # Bulk fallback when LAZY_EXPAND_QUERY_LIMIT is small
    monkeypatch.setattr(sp_mod, "LAZY_EXPAND_QUERY_LIMIT", 1)
    r_bulk = client.post(
        "/api/v1/search/shortest-path",
        json={"source_id": "A", "target_id": "F", "weighted": True},
    ).json()
    assert r_bulk["found"] is True
    assert r_bulk["total_weight"] == 5.0

    # Unreachable node G
    r_miss = client.post(
        "/api/v1/search/shortest-path",
        json={"source_id": "A", "target_id": "G", "weighted": True},
    ).json()
    assert r_miss["found"] is False
    assert r_miss["nodes"] == []

    # Same node A -> A
    r_same = client.post("/api/v1/search/shortest-path", json={"source_id": "A", "target_id": "A"}).json()
    assert r_same["found"] is True
    assert r_same["hops"] == 0
    assert r_same["total_weight"] == 0.0


def test_reachability_and_temporal_state(client: TestClient):
    _seed_graph(
        client,
        ["A", "B", "C", "D"],
        [
            {"id": "d_ab", "source": "A", "target": "B"},
            {"id": "d_bc", "source": "B", "target": "C", "valid_from": "2026-01-02T00:00:00Z", "valid_to": "2026-02-01T00:00:00Z"},
            {"id": "d_cd", "source": "C", "target": "D"},
        ],
    )

    r1 = client.post(
        "/api/v1/search/reachability",
        json={"source_id": "A", "target_id": "D", "valid_at": "2026-01-15T00:00:00Z"},
    ).json()
    assert r1["reachable"] is True
    assert r1["hops"] == 3
    assert r1["path_nodes"] == ["A", "B", "C", "D"]

    # Depth constraint
    r_short = client.post(
        "/api/v1/search/reachability",
        json={"source_id": "A", "target_id": "D", "max_depth": 2, "valid_at": "2026-01-15T00:00:00Z"},
    ).json()
    assert r_short["reachable"] is False

    # After B->C expired on Feb 1
    r_after = client.post(
        "/api/v1/search/reachability",
        json={"source_id": "A", "target_id": "D", "valid_at": "2026-02-15T00:00:00Z"},
    ).json()
    assert r_after["reachable"] is False

    # Incoming reachability D <- A
    r_inc = client.post(
        "/api/v1/search/reachability",
        json={"source_id": "D", "target_id": "A", "direction": "incoming", "valid_at": "2026-01-15T00:00:00Z"},
    ).json()
    assert r_inc["reachable"] is True
    assert r_inc["hops"] == 3


def test_cycle_detection_and_configurable_policies(app_factory):
    # 1. WARN policy
    c_warn = app_factory(cycle_policy=CyclePolicy.WARN)
    _seed_graph(
        c_warn,
        ["A", "B", "C"],
        [
            {"id": "d_ab", "source": "A", "target": "B"},
            {"id": "d_bc", "source": "B", "target": "C"},
        ],
    )

    # Check-cycle endpoint identifies C -> A would create [C, A, B, C]
    chk = c_warn.post("/api/v1/search/check-cycle", json={"source_id": "C", "target_id": "A"}).json()
    assert chk["creates_cycle"] is True
    assert chk["cycle_length"] == 3
    assert chk["cycle_nodes"] == ["C", "A", "B", "C"]

    # Non-cycle A -> C
    chk_ok = c_warn.post("/api/v1/search/check-cycle", json={"source_id": "A", "target_id": "C"}).json()
    assert chk_ok["creates_cycle"] is False

    # Self-loop check
    chk_self = c_warn.post("/api/v1/search/check-cycle", json={"source_id": "A", "target_id": "A"}).json()
    assert chk_self["creates_cycle"] is True
    assert chk_self["cycle_length"] == 1

    # WARN accepts C -> A and reports cycle_detected in meta
    r_warn = c_warn.post(
        "/api/v1/dependencies",
        json={"id": "d_ca", "source": "C", "target": "A", "recorded_at": "2026-01-03T00:00:00Z"},
    )
    assert r_warn.status_code == 201
    assert r_warn.json()["meta"]["cycle_detected"] is True
    assert r_warn.json()["meta"]["cycle_path"] == ["C", "A", "B", "C"]

    # 2. REJECT policy
    c_rej = app_factory(cycle_policy=CyclePolicy.REJECT)
    _seed_graph(
        c_rej,
        ["A", "B", "C"],
        [
            {"id": "d_ab", "source": "A", "target": "B"},
            {"id": "d_bc", "source": "B", "target": "C"},
        ],
    )
    r_rej = c_rej.post(
        "/api/v1/dependencies",
        json={"id": "d_ca", "source": "C", "target": "A", "recorded_at": "2026-01-03T00:00:00Z"},
    )
    assert r_rej.status_code == 409
    err = r_rej.json()["error"]
    assert err["code"] == "cycle_detected"
    assert err["details"]["cycle_nodes"] == ["C", "A", "B", "C"]
    # Confirm nothing was written to MongoDB
    assert c_rej.get("/api/v1/dependencies/d_ca").status_code == 404

    # Historical non-overlapping validity: after B->C is removed on Feb 1, adding C->A on Feb 15 is acyclic!
    assert (
        c_rej.delete(
            "/api/v1/dependencies/d_bc",
            params={"effective_from": "2026-02-01T00:00:00Z", "recorded_at": "2026-02-01T00:00:00Z"},
        ).status_code
        == 200
    )
    r_ok = c_rej.post(
        "/api/v1/dependencies",
        json={
            "id": "d_ca",
            "source": "C",
            "target": "A",
            "valid_from": "2026-02-15T00:00:00Z",
            "recorded_at": "2026-02-15T00:00:00Z",
        },
    )
    assert r_ok.status_code == 201

    # Attempting to restore B->C on Feb 20 (which now closes A->B->C->A) is rejected!
    r_rest_rej = c_rej.post(
        "/api/v1/dependencies/d_bc/restore",
        json={"effective_from": "2026-02-20T00:00:00Z", "recorded_at": "2026-02-20T00:00:00Z"},
    )
    assert r_rest_rej.status_code == 409
    assert r_rest_rej.json()["error"]["code"] == "cycle_detected"
