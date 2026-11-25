"""Phase 2 integration tests: CRUD, bitemporal history, synchronization, retries, reconcile, batch import."""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


def _create_node(client: TestClient, node_id: str, **extra) -> dict:
    payload = {"id": node_id, "name": f"Node {node_id}", "type": "service", **extra}
    r = client.post("/api/v1/nodes", json=payload)
    assert r.status_code == 201, r.text
    return r.json()


def test_node_crud_history_and_soft_delete_clips_dependencies(client: TestClient):
    _create_node(client, "A", valid_from="2026-01-01T00:00:00Z", recorded_at="2026-01-01T00:00:00Z")
    _create_node(client, "B", valid_from="2026-01-01T00:00:00Z", recorded_at="2026-01-01T00:00:00Z")

    # Update A
    r = client.patch(
        "/api/v1/nodes/A",
        json={"description": "API gateway", "metadata": {"tier": 1}, "recorded_at": "2026-01-05T00:00:00Z"},
    )
    assert r.status_code == 200
    assert r.json()["node"]["rev"] == 2
    assert r.json()["meta"]["sync"]["status"] == "synced"

    # Duplicate node rejected with 409
    r_dup = client.post("/api/v1/nodes", json={"id": "A", "type": "service"})
    assert r_dup.status_code == 409

    # Create dependency A -> B valid from 2026-01-10
    r_dep = client.post(
        "/api/v1/dependencies",
        json={
            "id": "dep_ab",
            "source": "A",
            "target": "B",
            "relationship": "calls",
            "weight": 2.0,
            "valid_from": "2026-01-10T00:00:00Z",
            "recorded_at": "2026-01-10T00:00:00Z",
        },
    )
    assert r_dep.status_code == 201
    assert r_dep.json()["meta"]["sync"]["status"] == "synced"

    # Soft delete node A effective 2026-02-01 -> clips dep_ab at 2026-02-01
    r_del = client.delete(
        "/api/v1/nodes/A",
        params={"effective_from": "2026-02-01T00:00:00Z", "recorded_at": "2026-02-01T00:00:00Z"},
    )
    assert r_del.status_code == 200
    assert r_del.json()["node"]["deleted"] is True
    assert r_del.json()["affected_dependencies"] == ["dep_ab"]
    assert r_del.json()["meta"]["sync"]["status"] == "synced"

    # Node history preserves all 3 snapshots
    r_hist = client.get("/api/v1/nodes/A/history")
    assert r_hist.status_code == 200
    assert [h["operation"] for h in r_hist.json()] == ["created", "updated", "deleted"]

    # Historical query before deletion still sees dep_ab active; after deletion sees inactive
    r_before = client.get("/api/v1/dependencies/dep_ab", params={"valid_at": "2026-01-20T00:00:00Z"})
    assert r_before.status_code == 200
    assert r_before.json()["status"] == "active"
    assert r_before.json()["current"]["valid_to"] == "2026-02-01T00:00:00Z"

    r_after = client.get("/api/v1/dependencies/dep_ab", params={"valid_at": "2026-02-05T00:00:00Z"})
    assert r_after.status_code == 200
    assert r_after.json()["status"] == "inactive"

    # Both DBs reconcile cleanly
    rec = client.post("/api/v1/sync/reconcile").json()
    assert rec["consistent"] is True


def test_bitemporal_late_events_corrections_idempotency_and_self_loop(client: TestClient):
    _create_node(client, "svc_a", valid_from="2026-01-01T00:00:00Z", recorded_at="2026-01-01T00:00:00Z")
    _create_node(client, "svc_b", valid_from="2026-01-01T00:00:00Z", recorded_at="2026-01-01T00:00:00Z")

    # Self-loop works and projects cleanly
    r_self = client.post(
        "/api/v1/dependencies",
        json={"id": "dep_self", "source": "svc_a", "target": "svc_a", "recorded_at": "2026-01-01T00:00:00Z"},
    )
    assert r_self.status_code == 201
    assert r_self.json()["meta"]["sync"]["status"] == "synced"

    # Create dep_1 with client_event_id
    r1 = client.post(
        "/api/v1/dependencies",
        json={
            "id": "dep_1",
            "source": "svc_a",
            "target": "svc_b",
            "weight": 1.0,
            "valid_from": "2026-01-10T00:00:00Z",
            "recorded_at": "2026-01-10T00:00:00Z",
            "client_event_id": "req-create-1",
        },
    )
    assert r1.status_code == 201
    ev_create_id = r1.json()["meta"]["event_ids"][0]

    # Idempotent replay returns duplicate=True
    r1_dup = client.post(
        "/api/v1/dependencies",
        json={
            "id": "dep_1",
            "source": "svc_a",
            "target": "svc_b",
            "weight": 1.0,
            "valid_from": "2026-01-10T00:00:00Z",
            "recorded_at": "2026-01-10T00:00:00Z",
            "client_event_id": "req-create-1",
        },
    )
    assert r1_dup.status_code == 201
    assert r1_dup.json()["meta"]["duplicate"] is True

    # Reusing client_event_id for a different dependency -> 409
    r_bad_key = client.post(
        "/api/v1/dependencies",
        json={"id": "dep_other", "source": "svc_b", "target": "svc_a", "client_event_id": "req-create-1"},
    )
    assert r_bad_key.status_code == 409

    # Modify at valid_time 2026-03-01, recorded_at 2026-03-01
    r_mod1 = client.patch(
        "/api/v1/dependencies/dep_1",
        json={"weight": 3.0, "effective_from": "2026-03-01T00:00:00Z", "recorded_at": "2026-03-01T00:00:00Z"},
    )
    assert r_mod1.status_code == 200
    ev_mod1_id = r_mod1.json()["meta"]["event_ids"][0]

    # Late-arriving modification for valid_time 2026-02-01, recorded_at 2026-04-01
    r_late = client.patch(
        "/api/v1/dependencies/dep_1",
        json={"weight": 2.0, "effective_from": "2026-02-01T00:00:00Z", "recorded_at": "2026-04-01T00:00:00Z"},
    )
    assert r_late.status_code == 200

    # At valid_at=2026-02-15:
    # - known_at=2026-03-15 (before late event arrived): weight was 1.0
    # - known_at=2026-04-15 (after late event arrived): weight is 2.0
    r_k_before = client.get(
        "/api/v1/dependencies/dep_1",
        params={"valid_at": "2026-02-15T00:00:00Z", "known_at": "2026-03-15T00:00:00Z"},
    )
    assert r_k_before.json()["current"]["weight"] == 1.0

    r_k_after = client.get(
        "/api/v1/dependencies/dep_1",
        params={"valid_at": "2026-02-15T00:00:00Z", "known_at": "2026-04-15T00:00:00Z"},
    )
    assert r_k_after.json()["current"]["weight"] == 2.0

    # Correction at 2026-05-01: retract ev_mod1_id and set weight=5.0 at 2026-03-15
    r_corr = client.post(
        "/api/v1/dependencies/dep_1/corrections",
        json={
            "retract_event_ids": [ev_mod1_id],
            "add_events": [{"type": "modified", "valid_at": "2026-03-15T00:00:00Z", "weight": 5.0}],
            "reason": "fix erroneous March change",
            "recorded_at": "2026-05-01T00:00:00Z",
        },
    )
    assert r_corr.status_code == 200
    # At valid_at=2026-03-05: previously believed weight=3.0, now corrected back to 2.0
    assert (
        client.get(
            "/api/v1/dependencies/dep_1",
            params={"valid_at": "2026-03-05T00:00:00Z", "known_at": "2026-04-15T00:00:00Z"},
        ).json()["current"]["weight"]
        == 3.0
    )
    assert (
        client.get(
            "/api/v1/dependencies/dep_1",
            params={"valid_at": "2026-03-05T00:00:00Z", "known_at": "2026-05-02T00:00:00Z"},
        ).json()["current"]["weight"]
        == 2.0
    )
    assert (
        client.get(
            "/api/v1/dependencies/dep_1",
            params={"valid_at": "2026-03-20T00:00:00Z", "known_at": "2026-05-02T00:00:00Z"},
        ).json()["current"]["weight"]
        == 5.0
    )

    # Remove and restore
    assert client.delete(
        "/api/v1/dependencies/dep_1",
        params={"effective_from": "2026-06-01T00:00:00Z", "recorded_at": "2026-06-01T00:00:00Z"},
    ).status_code == 200
    assert client.post(
        "/api/v1/dependencies/dep_1/restore",
        json={"effective_from": "2026-07-01T00:00:00Z", "weight": 6.0, "recorded_at": "2026-07-01T00:00:00Z"},
    ).status_code == 200

    # Events audit trail retains created + retracted mod + correction audit
    evs = client.get("/api/v1/dependencies/dep_1/events").json()
    assert evs[0]["event_id"] == ev_create_id
    retracted = [e for e in evs if e["event_id"] == ev_mod1_id][0]
    assert retracted["retracted_at"] is not None

    # Overlapping equivalent triple -> 409, while inside the removed gap [06-01, 07-01) -> 201
    r_overlap = client.post(
        "/api/v1/dependencies",
        json={"source": "svc_a", "target": "svc_b", "valid_from": "2026-06-15T00:00:00Z",
              "recorded_at": "2026-07-02T00:00:00Z"},
    )
    assert r_overlap.status_code == 409

    r_gap = client.post(
        "/api/v1/dependencies",
        json={"id": "dep_gap", "source": "svc_a", "target": "svc_b",
              "valid_from": "2026-06-01T00:00:00Z", "valid_to": "2026-07-01T00:00:00Z",
              "recorded_at": "2026-07-02T00:00:00Z"},
    )
    assert r_gap.status_code == 201

    # Reconcile confirms MongoDB and Neo4j agree across all versions
    rec = client.post("/api/v1/sync/reconcile").json()
    assert rec["consistent"] is True


def test_sync_retries_dead_letter_recovery_and_reconcile(app_factory):
    c = app_factory(sync_inline=True, sync_max_attempts=2, sync_backoff_base_seconds=0.01)
    container = c.app.state.container

    _create_node(c, "X")
    _create_node(c, "Y")

    # Simulate Neo4j projection failure while MongoDB stays up
    real_project = container.neo.project

    def broken_project(*args, **kwargs):
        raise RuntimeError("simulated neo4j write failure")

    container.neo.project = broken_project
    try:
        r_dep = c.post("/api/v1/dependencies", json={"id": "dep_xy", "source": "X", "target": "Y"})
        # Write succeeds in MongoDB; inline sync catches failure and leaves outbox pending
        assert r_dep.status_code == 201
        assert r_dep.json()["meta"]["sync"]["status"] == "pending"

        # Wait past 10ms backoff and trigger second attempt -> dead-letters after sync_max_attempts=2
        time.sleep(0.03)
        proc = c.post("/api/v1/sync/process").json()
        assert proc["dead_lettered"] == 1

        st = c.get("/api/v1/sync/status").json()
        assert st["counts"]["failed"] == 1
        assert len(st["failures"]) == 1
        assert st["failures"][0]["last_error_type"] == "RuntimeError"

        # Reconcile reports missing dependency in Neo4j
        rec_bad = c.post("/api/v1/sync/reconcile").json()
        assert rec_bad["consistent"] is False
        assert rec_bad["stale_dependencies"] == ["dep_xy"]
    finally:
        container.neo.project = real_project

    # Recover via retry-failed + process
    assert c.post("/api/v1/sync/retry-failed").json() == {"reset": 1}
    proc2 = c.post("/api/v1/sync/process").json()
    assert proc2["done"] == 1

    rec_ok = c.post("/api/v1/sync/reconcile").json()
    assert rec_ok["consistent"] is True

    # Introduce manual drift in Neo4j and repair via reconcile?repair=true and rebuild
    with container.neo.driver.session(database=container.neo.database) as s:
        s.run("MATCH (n:Component {id: 'Y'}) DETACH DELETE n").consume()
        s.run("CREATE (:Component {id: 'GHOST', rev: 1})").consume()

    rec_drift = c.post("/api/v1/sync/reconcile").json()
    assert rec_drift["consistent"] is False
    assert rec_drift["missing_nodes"] == ["Y"]
    assert rec_drift["orphan_nodes"] == ["GHOST"]

    rec_repaired = c.post("/api/v1/sync/reconcile", params={"repair": "true"}).json()
    assert rec_repaired["repaired"] is True
    assert c.post("/api/v1/sync/reconcile").json()["consistent"] is True

    # Rebuild wipes and reconstructs identically
    reb = c.post("/api/v1/sync/rebuild").json()
    assert reb["nodes"] == 2
    assert reb["dependencies"] == 1
    assert c.post("/api/v1/sync/reconcile").json()["consistent"] is True


def test_background_sync_worker_drains_deferred_writes(app_factory):
    c = app_factory(sync_inline=False, sync_worker_enabled=True, sync_poll_interval_seconds=0.05)
    r1 = c.post("/api/v1/nodes", json={"id": "W1", "type": "service"})
    assert r1.status_code == 201
    assert r1.json()["meta"]["sync"]["status"] == "deferred"
    r2 = c.post("/api/v1/nodes", json={"id": "W2", "type": "service"})
    assert r2.status_code == 201
    assert c.post("/api/v1/dependencies", json={"id": "dep_w", "source": "W1", "target": "W2"}).status_code == 201

    deadline = time.monotonic() + 5.0
    consistent = False
    while time.monotonic() < deadline:
        st = c.get("/api/v1/sync/status").json()
        if st["counts"]["pending"] == 0 and st["counts"]["processing"] == 0:
            if c.post("/api/v1/sync/reconcile").json()["consistent"]:
                consistent = True
                break
        time.sleep(0.05)
    assert consistent is True


def test_batch_importer_api_and_files(client: TestClient, container, tmp_path: Path):
    # Via API
    payload = {
        "nodes": [
            {"id": "n1", "type": "api", "valid_from": "2026-01-01T00:00:00Z", "recorded_at": "2026-01-01T00:00:00Z"},
            {"id": "n2", "type": "db", "valid_from": "2026-01-01T00:00:00Z", "recorded_at": "2026-01-01T00:00:00Z"},
            {"id": "n3", "type": "cache", "valid_from": "2026-01-01T00:00:00Z", "recorded_at": "2026-01-01T00:00:00Z"},
        ],
        "dependencies": [
            {"id": "d12", "source": "n1", "target": "n2", "weight": 1.0,
             "valid_from": "2026-01-02T00:00:00Z", "recorded_at": "2026-01-02T00:00:00Z"},
            {"id": "d13", "source": "n1", "target": "n3", "weight": 2.0,
             "valid_from": "2026-01-02T00:00:00Z", "recorded_at": "2026-01-02T00:00:00Z"},
        ],
        "events": [
            {"dependency_id": "d12", "type": "modified", "weight": 4.0,
             "effective_from": "2026-02-01T00:00:00Z", "recorded_at": "2026-02-01T00:00:00Z"},
            {"dependency_id": "d12", "type": "removed",
             "effective_from": "2026-03-01T00:00:00Z", "recorded_at": "2026-03-01T00:00:00Z"},
        ],
        "sync": True,
    }
    r = client.post("/api/v1/ingest/batch", json=payload)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["nodes_ingested"] == 3
    assert body["dependencies_ingested"] == 2
    assert body["events_ingested"] == 7  # 3 node + 2 dep created + 2 temporal events
    assert client.post("/api/v1/sync/reconcile").json()["consistent"] is True

    # Verify d12 has the exact expected bitemporal timeline
    d12 = client.get("/api/v1/dependencies/d12", params={"valid_at": "2026-02-15T00:00:00Z"}).json()
    assert d12["status"] == "active"
    assert d12["current"]["weight"] == 4.0
    assert d12["current"]["valid_to"] == "2026-03-01T00:00:00Z"

    # Also test file ingestion (JSONL + CSV)
    nodes_file = tmp_path / "nodes.jsonl"
    deps_file = tmp_path / "deps.csv"
    nodes_file.write_text(
        json.dumps({"id": "n4", "type": "queue", "recorded_at": "2026-01-01T00:00:00Z"}) + "\n",
        encoding="utf-8",
    )
    deps_file.write_text(
        "id,source,target,relationship,weight,recorded_at\n"
        "d24,n2,n4,publishes_to,1.5,2026-01-05T00:00:00Z\n",
        encoding="utf-8",
    )
    metrics = container.importer.ingest_files(nodes_file, deps_file, sync=True)
    assert metrics.nodes_ingested == 1
    assert metrics.dependencies_ingested == 1
    assert client.post("/api/v1/sync/reconcile").json()["consistent"] is True
    assert client.get("/api/v1/dependencies/d24").json()["current"]["weight"] == 1.5
