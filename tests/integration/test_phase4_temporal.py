"""Phase 4 integration tests: temporal as-of reconstruction, historical diff/compare, and timeline history."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


def _setup_temporal_scenario(client: TestClient) -> dict[str, str]:
    """Build a multi-month bitemporal history with creations, edits, removals, late events, and node updates."""
    for nid, name in [("A", "Gateway v1"), ("B", "Auth"), ("C", "Billing"), ("D", "Postgres"), ("ISO", "Isolated")]:
        r = client.post(
            "/api/v1/nodes",
            json={
                "id": nid,
                "name": name,
                "type": "service",
                "valid_from": "2026-01-01T00:00:00Z",
                "recorded_at": "2026-01-01T00:00:00Z",
            },
        )
        assert r.status_code == 201

    # Jan 2: A->B, A->C, B->D, C->D (diamond) + B->C cross-edge
    deps = [
        {"id": "d_ab", "source": "A", "target": "B", "weight": 1.0, "metadata": {"proto": "http", "tier": 1}},
        {"id": "d_ac", "source": "A", "target": "C", "weight": 2.0, "metadata": {"proto": "grpc"}},
        {"id": "d_bd", "source": "B", "target": "D", "weight": 1.0, "metadata": {}},
        {"id": "d_cd", "source": "C", "target": "D", "weight": 1.0, "metadata": {}},
        {"id": "d_bc", "source": "B", "target": "C", "weight": 0.5, "metadata": {}},
    ]
    for d in deps:
        r = client.post(
            "/api/v1/dependencies",
            json={**d, "valid_from": "2026-01-02T00:00:00Z", "recorded_at": "2026-01-02T00:00:00Z"},
        )
        assert r.status_code == 201

    # Feb 1: node A renamed to "Gateway v2"; d_ab weight & metadata modified; d_bc removed
    assert (
        client.patch(
            "/api/v1/nodes/A",
            json={"name": "Gateway v2", "recorded_at": "2026-02-01T00:00:00Z"},
        ).status_code
        == 200
    )
    r_mod = client.patch(
        "/api/v1/dependencies/d_ab",
        json={
            "weight": 3.5,
            "metadata": {"proto": "grpc", "tls": True},
            "effective_from": "2026-02-01T00:00:00Z",
            "recorded_at": "2026-02-01T00:00:00Z",
        },
    )
    assert r_mod.status_code == 200
    ev_mod_ab = r_mod.json()["meta"]["event_ids"][0]

    assert (
        client.delete(
            "/api/v1/dependencies/d_bc",
            params={"effective_from": "2026-02-01T00:00:00Z", "recorded_at": "2026-02-01T00:00:00Z"},
        ).status_code
        == 200
    )

    # Mar 1: new node E added + C->E added; node C stays active; late event arrives for d_ac effective Jan 15!
    assert (
        client.post(
            "/api/v1/nodes",
            json={"id": "E", "name": "Cache", "type": "cache", "valid_from": "2026-03-01T00:00:00Z",
                  "recorded_at": "2026-03-01T00:00:00Z"},
        ).status_code
        == 201
    )
    assert (
        client.post(
            "/api/v1/dependencies",
            json={"id": "d_ce", "source": "C", "target": "E", "weight": 1.5,
                  "valid_from": "2026-03-01T00:00:00Z", "recorded_at": "2026-03-01T00:00:00Z"},
        ).status_code
        == 201
    )
    # Late-arriving event recorded Mar 15, effective Jan 15
    assert (
        client.patch(
            "/api/v1/dependencies/d_ac",
            json={"weight": 4.0, "effective_from": "2026-01-15T00:00:00Z", "recorded_at": "2026-03-15T00:00:00Z"},
        ).status_code
        == 200
    )

    # Apr 1: correction retracts Feb 1 d_ab mod and replaces with weight=2.5 effective Feb 10; node E deleted Apr 15
    assert (
        client.post(
            "/api/v1/dependencies/d_ab/corrections",
            json={
                "retract_event_ids": [ev_mod_ab],
                "add_events": [
                    {"type": "modified", "valid_at": "2026-02-10T00:00:00Z", "weight": 2.5,
                     "metadata": {"proto": "http", "tier": 1, "tls": True}}
                ],
                "reason": "incorrect Feb 1 weight",
                "recorded_at": "2026-04-01T00:00:00Z",
            },
        ).status_code
        == 200
    )
    assert (
        client.delete(
            "/api/v1/nodes/E",
            params={"effective_from": "2026-04-15T00:00:00Z", "recorded_at": "2026-04-15T00:00:00Z"},
        ).status_code
        == 200
    )
    return {"ev_mod_ab": ev_mod_ab}


def test_temporal_as_of_subgraph_bitemporal_and_whole_graph(client: TestClient):
    _setup_temporal_scenario(client)

    # Missing node -> 404
    assert (
        client.post("/api/v1/temporal/as-of", json={"node_id": "NOPE", "valid_at": "2026-01-10T00:00:00Z"}).status_code
        == 404
    )

    # Query outside valid range (2025-12-15, before nodes started on 2026-01-01)
    r_early = client.post(
        "/api/v1/temporal/as-of",
        json={"node_id": "A", "valid_at": "2025-12-15T00:00:00Z"},
    ).json()
    assert r_early["node_active_at_time"] is False
    assert r_early["nodes"] == []
    assert r_early["edges"] == []

    # At valid_at=Jan 20, known_at=Feb 15 (before late event on Mar 15, after Feb 1 rename)
    r_jan_feb = client.post(
        "/api/v1/temporal/as-of",
        json={"node_id": "A", "valid_at": "2026-01-20T00:00:00Z", "known_at": "2026-02-15T00:00:00Z", "max_depth": 2},
    ).json()
    assert r_jan_feb["node_active_at_time"] is True
    nodes_by_id = {n["id"]: n for n in r_jan_feb["nodes"]}
    assert set(nodes_by_id) == {"A", "B", "C", "D"}
    assert nodes_by_id["A"]["name"] == "Gateway v2"
    edges_by_id = {e["dependency_id"]: e for e in r_jan_feb["edges"]}
    # Diamond + cross-edge d_bc all reconstructed at Jan 20
    assert set(edges_by_id) == {"d_ab", "d_ac", "d_bd", "d_cd", "d_bc"}
    # Before Mar 15 late event, d_ac had weight 2.0 on Jan 20
    assert edges_by_id["d_ac"]["weight"] == 2.0

    # At valid_at=Jan 20, known_at=Jan 10 (before Feb 1 rename -> node A name was "Gateway v1"!)
    r_jan_jan = client.post(
        "/api/v1/temporal/as-of",
        json={"node_id": "A", "valid_at": "2026-01-20T00:00:00Z", "known_at": "2026-01-10T00:00:00Z", "max_depth": 1},
    ).json()
    assert {n["id"]: n["name"] for n in r_jan_jan["nodes"]}["A"] == "Gateway v1"
    # max_depth=1 outgoing from A must include only d_ab and d_ac, not d_bc/d_bd/d_cd
    assert {e["dependency_id"] for e in r_jan_jan["edges"]} == {"d_ab", "d_ac"}

    # At valid_at=Jan 20, known_at=Apr 10 (after Mar 15 late event -> d_ac weight is 4.0)
    r_jan_apr = client.post(
        "/api/v1/temporal/as-of",
        json={"node_id": "A", "valid_at": "2026-01-20T00:00:00Z", "known_at": "2026-04-10T00:00:00Z", "max_depth": 1},
    ).json()
    assert {e["dependency_id"]: e["weight"] for e in r_jan_apr["edges"]}["d_ac"] == 4.0

    # Node E soft-deleted Apr 15:
    # - known_at=Apr 10, valid_at=Apr 20 -> at Apr 10 deletion wasn't recorded yet, so E & d_ce still believed active!
    r_e_before = client.post(
        "/api/v1/temporal/as-of",
        json={"node_id": "E", "valid_at": "2026-04-20T00:00:00Z", "known_at": "2026-04-10T00:00:00Z",
              "direction": "incoming", "max_depth": 1},
    ).json()
    assert r_e_before["node_active_at_time"] is True
    assert [e["dependency_id"] for e in r_e_before["edges"]] == ["d_ce"]

    # - known_at=Apr 20, valid_at=Apr 20 -> E is deleted
    r_e_after = client.post(
        "/api/v1/temporal/as-of",
        json={"node_id": "E", "valid_at": "2026-04-20T00:00:00Z", "known_at": "2026-04-20T00:00:00Z",
              "direction": "incoming", "max_depth": 1},
    ).json()
    assert r_e_after["node_active_at_time"] is False
    assert r_e_after["edges"] == []

    # Whole-graph reconstruction at Mar 15 (E active, d_ce active, d_bc removed, ISO isolated node present)
    r_whole = client.post(
        "/api/v1/temporal/as-of",
        json={"valid_at": "2026-03-15T00:00:00Z", "known_at": "2026-04-20T00:00:00Z"},
    ).json()
    assert r_whole["node_id"] is None
    assert {n["id"] for n in r_whole["nodes"]} == {"A", "B", "C", "D", "E", "ISO"}
    assert {e["dependency_id"] for e in r_whole["edges"]} == {"d_ab", "d_ac", "d_bd", "d_cd", "d_ce"}


def test_temporal_compare_valid_time_and_belief_diffs(client: TestClient):
    _setup_temporal_scenario(client)

    # Compare whole graph between Jan 10 and Mar 15 (as currently known on Apr 20)
    cmp_whole = client.post(
        "/api/v1/temporal/compare",
        json={
            "from_valid_at": "2026-01-10T00:00:00Z",
            "to_valid_at": "2026-03-15T00:00:00Z",
            "known_at": "2026-04-20T00:00:00Z",
        },
    ).json()
    s = cmp_whole["summary"]
    assert s["from_edge_count"] == 5
    assert s["to_edge_count"] == 5
    assert [e["dependency_id"] for e in cmp_whole["added"]] == ["d_ce"]
    assert [e["dependency_id"] for e in cmp_whole["removed"]] == ["d_bc"]
    assert [m["dependency_id"] for m in cmp_whole["modified"]] == ["d_ab", "d_ac"]
    assert s["unchanged_count"] == 2  # d_bd, d_cd

    # Inspect detailed attribute diffs on d_ab:
    # Jan 10: weight 1.0, metadata {"proto": "http", "tier": 1}
    # Mar 15 (after Apr 1 correction): weight 2.5, metadata {"proto": "http", "tier": 1, "tls": True}
    diff_ab = cmp_whole["modified"][0]
    assert set(diff_ab["changed_attributes"]) == {"weight", "metadata"}
    assert diff_ab["attribute_changes"]["weight"] == {"before": 1.0, "after": 2.5}
    assert diff_ab["attribute_changes"]["metadata"] == {
        "added": {"tls": True},
        "removed": {},
        "changed": {},
    }

    # Belief comparison at SAME valid_at=Feb 5, comparing known_at=Feb 15 vs known_at=Apr 10 (after correction!)
    cmp_belief = client.post(
        "/api/v1/temporal/compare",
        json={
            "from_valid_at": "2026-02-05T00:00:00Z",
            "to_valid_at": "2026-02-05T00:00:00Z",
            "from_known_at": "2026-02-15T00:00:00Z",
            "to_known_at": "2026-04-10T00:00:00Z",
            "node_id": "A",
            "direction": "outgoing",
            "max_depth": 1,
        },
    ).json()
    # On Feb 5:
    # - Known Feb 15: d_ab had weight 3.5 (erroneous Feb 1 mod), d_ac had weight 2.0
    # - Known Apr 10: d_ab reverted to 1.0 (correction takes effect Feb 10), d_ac is 4.0 (late event Jan 15)
    mods = {m["dependency_id"]: m for m in cmp_belief["modified"]}
    assert set(mods) == {"d_ab", "d_ac"}
    assert mods["d_ab"]["attribute_changes"]["weight"] == {"before": 3.5, "after": 1.0}
    assert mods["d_ac"]["attribute_changes"]["weight"] == {"before": 2.0, "after": 4.0}


def test_temporal_history_endpoint_and_filters(client: TestClient):
    ids = _setup_temporal_scenario(client)

    assert client.get("/api/v1/temporal/history/NOPE").status_code == 404

    full = client.get("/api/v1/temporal/history/A").json()
    assert full["node"]["id"] == "A"
    assert [h["operation"] for h in full["node_history"]] == ["created", "updated"]
    assert {d["id"] for d in full["dependencies"]} == {"d_ab", "d_ac"}
    # Superseded versions and retracted events are included by default
    assert any(v["superseded_at"] is not None for v in full["versions"])
    assert any(e["event_id"] == ids["ev_mod_ab"] and e["retracted_at"] is not None for e in full["events"])

    filtered = client.get(
        "/api/v1/temporal/history/A",
        params={"include_retracted": "false", "include_superseded": "false"},
    ).json()
    assert all(v["superseded_at"] is None for v in filtered["versions"])
    assert all(e["retracted_at"] is None for e in filtered["events"])
