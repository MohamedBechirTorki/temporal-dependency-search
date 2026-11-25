"""Phase 5 integration tests: overview, centrality, cycles, and impact analysis."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


def _seed_architecture(client: TestClient) -> None:
    nodes = ["Web", "API", "Worker", "Auth", "Billing", "DB", "CycleA", "CycleB", "Isolated"]
    deps = [
        {"id": "d_web_api", "source": "Web", "target": "API", "relationship": "calls"},
        {"id": "d_api_auth", "source": "API", "target": "Auth", "relationship": "calls"},
        {"id": "d_worker_auth", "source": "Worker", "target": "Auth", "relationship": "calls"},
        {"id": "d_auth_db", "source": "Auth", "target": "DB", "relationship": "reads"},
        {"id": "d_bill_db", "source": "Billing", "target": "DB", "relationship": "writes"},
        {"id": "d_ca_cb", "source": "CycleA", "target": "CycleB", "relationship": "syncs"},
        {"id": "d_cb_ca", "source": "CycleB", "target": "CycleA", "relationship": "syncs"},
        {"id": "d_iso_self", "source": "Isolated", "target": "Isolated", "relationship": "heartbeat"},
    ]
    r = client.post(
        "/api/v1/ingest/batch",
        json={
            "nodes": [
                {"id": n, "type": "service", "valid_from": "2026-01-01T00:00:00Z", "recorded_at": "2026-01-01T00:00:00Z"}
                for n in nodes
            ],
            "dependencies": [
                {"valid_from": "2026-01-02T00:00:00Z", "recorded_at": "2026-01-02T00:00:00Z", **d}
                for d in deps
            ],
            "sync": True,
        },
    )
    assert r.status_code == 201, r.text


def test_overview_on_empty_and_populated_graph(client: TestClient):
    empty = client.get("/api/v1/analytics/overview").json()
    assert empty["node_count"] == 0
    assert empty["active_edge_count"] == 0
    assert empty["density"] == 0.0
    assert empty["has_cycles"] is False

    _seed_architecture(client)
    ov = client.get("/api/v1/analytics/overview", params={"top_k": 3}).json()
    assert ov["node_count"] == 9
    assert ov["active_edge_count"] == 8
    assert ov["unique_directed_pairs"] == 7
    assert ov["self_loop_count"] == 1
    assert ov["density"] == pytest.approx(7 / (9 * 8))
    # 3 WCCs: {Web,API,Worker,Auth,Billing,DB}, {CycleA,CycleB}, {Isolated}
    assert ov["weakly_connected_component_count"] == 3
    assert ov["largest_wcc_size"] == 6
    assert ov["non_trivial_scc_count"] == 1
    assert ov["has_cycles"] is True
    # Most depended-upon: Auth (in=2) and DB (in=2)
    top_in_ids = {n["id"]: n["in_degree"] for n in ov["most_depended_upon"][:2]}
    assert top_in_ids == {"Auth": 2, "DB": 2}


def test_centrality_and_temporal_cycles(client: TestClient):
    _seed_architecture(client)

    cent = client.get(
        "/api/v1/analytics/centrality",
        params=[("top_k", "3"), ("node_ids", "DB"), ("node_ids", "Auth"), ("node_ids", "Web")],
    ).json()
    assert cent["betweenness_algorithm"] == "brandes_exact"
    # Auth is on paths Web->API->Auth->DB, API->Auth->DB, Worker->Auth->DB => highest betweenness
    assert cent["top_by_betweenness"][0]["id"] == "Auth"
    # Within the acyclic service stack, DB (sink) > Auth > Web in PageRank
    selected = {n["id"]: n for n in cent["selected_nodes"]}
    assert list(selected) == ["DB", "Auth", "Web"]
    assert selected["DB"]["pagerank"] > selected["Auth"]["pagerank"] > selected["Web"]["pagerank"]

    # Sampled mode works via query param
    cent_s = client.get("/api/v1/analytics/centrality", params={"betweenness_samples": 4}).json()
    assert cent_s["betweenness_algorithm"] == "brandes_sampled"
    assert cent_s["betweenness_sample_size"] == 4

    # Cycles at Jan 15: CycleA<->CycleB + Isolated self-loop
    cyc1 = client.get("/api/v1/analytics/cycles", params={"valid_at": "2026-01-15T00:00:00Z"}).json()
    assert cyc1["has_cycles"] is True
    assert cyc1["non_trivial_scc_count"] == 1
    assert cyc1["strongly_connected_components"][0]["nodes"] == ["CycleA", "CycleB"]
    assert cyc1["self_loops"] == ["Isolated"]
    assert ["Isolated", "Isolated"] in cyc1["sample_cycles"]
    assert any(set(c) == {"CycleA", "CycleB"} and c[0] == c[-1] for c in cyc1["sample_cycles"])

    # Break both cycles effective Feb 1
    assert client.delete(
        "/api/v1/dependencies/d_cb_ca",
        params={"effective_from": "2026-02-01T00:00:00Z", "recorded_at": "2026-02-01T00:00:00Z"},
    ).status_code == 200
    assert client.delete(
        "/api/v1/dependencies/d_iso_self",
        params={"effective_from": "2026-02-01T00:00:00Z", "recorded_at": "2026-02-01T00:00:00Z"},
    ).status_code == 200

    cyc2 = client.get("/api/v1/analytics/cycles", params={"valid_at": "2026-02-15T00:00:00Z"}).json()
    assert cyc2["has_cycles"] is False
    assert cyc2["sample_cycles"] == []

    # Once the 2-cycle trap is broken on Feb 15, DB becomes #1 overall in PageRank
    cent_feb = client.get("/api/v1/analytics/centrality", params={"valid_at": "2026-02-15T00:00:00Z"}).json()
    assert cent_feb["top_by_pagerank"][0]["id"] == "DB"


def test_impact_analysis_directions_paths_and_temporal(client: TestClient):
    _seed_architecture(client)

    assert client.post("/api/v1/analytics/impact", json={"node_id": "MISSING"}).status_code == 404

    imp = client.post("/api/v1/analytics/impact", json={"node_id": "DB", "max_depth": 5}).json()
    assert imp["target_node_id"] == "DB"
    assert [n["node"]["id"] for n in imp["directly_affected"]] == ["Auth", "Billing"]
    assert [(n["node"]["id"], n["hops"]) for n in imp["indirectly_affected"]] == [
        ("API", 2),
        ("Worker", 2),
        ("Web", 3),
    ]
    assert imp["total_affected"] == 5
    assert imp["max_impact_depth"] == 3

    web_entry = next(n for n in imp["indirectly_affected"] if n["node"]["id"] == "Web")
    assert web_entry["dependency_path"] == ["Web", "API", "Auth", "DB"]
    # Edges must be ordered from Web -> API -> Auth -> DB with accurate source -> target
    assert [(e["source"], e["target"]) for e in web_entry["path_edges"]] == [
        ("Web", "API"),
        ("API", "Auth"),
        ("Auth", "DB"),
    ]

    # Remove Worker -> Auth on Feb 1 and re-evaluate impact on Feb 15
    assert client.delete(
        "/api/v1/dependencies/d_worker_auth",
        params={"effective_from": "2026-02-01T00:00:00Z", "recorded_at": "2026-02-01T00:00:00Z"},
    ).status_code == 200

    imp_feb = client.post(
        "/api/v1/analytics/impact",
        json={"node_id": "DB", "valid_at": "2026-02-15T00:00:00Z"},
    ).json()
    affected_ids = {n["node"]["id"] for n in imp_feb["directly_affected"] + imp_feb["indirectly_affected"]}
    assert affected_ids == {"Auth", "Billing", "API", "Web"}
