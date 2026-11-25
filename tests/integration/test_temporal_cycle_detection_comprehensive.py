"""Comprehensive integration tests for temporal cycle detection covering all 11 required scenarios."""
from datetime import datetime, timezone
import pytest
from fastapi.testclient import TestClient

from app.api.schemas.analytics import AnalyticsCyclesResponse
from app.container import Container

UTC = timezone.utc


def _seed_test_graph(container: Container, nodes: list[str | dict], deps: list[dict], dataset: str, sync: bool = True) -> None:
    node_dicts = [{"id": n, "name": n} if isinstance(n, str) else n for n in nodes]
    container.importer.ingest_nodes(node_dicts, dataset=dataset, sync=False)
    container.importer.ingest_dependencies(deps, dataset=dataset, sync=sync)


def test_1_graph_with_no_cycles(client: TestClient, container: Container):
    """1. A graph with no cycles (DAG)."""
    dataset = "test-dag"
    _seed_test_graph(
        container,
        ["A", "B", "C", "D"],
        [
            {"id": "d1", "source": "A", "target": "B"},
            {"id": "d2", "source": "B", "target": "C"},
            {"id": "d3", "source": "C", "target": "D"},
        ],
        dataset=dataset,
    )

    res = container.analytics.cycles(dataset=dataset)
    assert res.has_cycles is False
    assert res.non_trivial_scc_count == 0
    assert res.self_loop_count == 0
    assert res.strongly_connected_components == []
    assert res.self_loops == []


def test_2_graph_with_one_directed_cycle(client: TestClient, container: Container):
    """2. A graph with one directed cycle."""
    dataset = "test-one-cycle"
    _seed_test_graph(
        container,
        ["A", "B", "C"],
        [
            {"id": "d1", "source": "A", "target": "B"},
            {"id": "d2", "source": "B", "target": "C"},
            {"id": "d3", "source": "C", "target": "A"},
        ],
        dataset=dataset,
    )

    res = container.analytics.cycles(dataset=dataset)
    assert res.has_cycles is True
    assert res.non_trivial_scc_count == 1
    assert res.self_loop_count == 0
    assert set(res.strongly_connected_components[0].nodes) == {"A", "B", "C"}


def test_3_multiple_independent_cycles(client: TestClient, container: Container):
    """3. Multiple independent cycles in disjoint components."""
    dataset = "test-multi-cycles"
    _seed_test_graph(
        container,
        ["A", "B", "X", "Y", "Z"],
        [
            # Cycle 1: A <-> B
            {"id": "d_ab", "source": "A", "target": "B"},
            {"id": "d_ba", "source": "B", "target": "A"},
            # Cycle 2: X -> Y -> Z -> X
            {"id": "d_xy", "source": "X", "target": "Y"},
            {"id": "d_yz", "source": "Y", "target": "Z"},
            {"id": "d_zx", "source": "Z", "target": "X"},
        ],
        dataset=dataset,
    )

    res = container.analytics.cycles(dataset=dataset)
    assert res.has_cycles is True
    assert res.non_trivial_scc_count == 2
    assert res.self_loop_count == 0
    scc_sets = [set(scc.nodes) for scc in res.strongly_connected_components]
    assert {"A", "B"} in scc_sets
    assert {"X", "Y", "Z"} in scc_sets


def test_4_self_loop_only(client: TestClient, container: Container):
    """4. A graph with a self-loop and no non-trivial cycles."""
    dataset = "test-self-loop"
    _seed_test_graph(
        container,
        ["E"],
        [
            {"id": "d_ee", "source": "E", "target": "E"},
        ],
        dataset=dataset,
    )

    res = container.analytics.cycles(dataset=dataset)
    assert res.has_cycles is True
    assert res.non_trivial_scc_count == 0
    assert res.self_loop_count == 1
    assert res.self_loops == ["E"]
    assert ["E", "E"] in res.sample_cycles


def test_5_non_trivial_scc_and_self_loop_distinction(client: TestClient, container: Container):
    """5. Distinguishing non-trivial SCC from self-loops (the cycle-test topology)."""
    dataset = "test-distinction"
    _seed_test_graph(
        container,
        ["A", "B", "C", "D", "E"],
        [
            {"id": "d_ab", "source": "A", "target": "B"},
            {"id": "d_bc", "source": "B", "target": "C"},
            {"id": "d_ca", "source": "C", "target": "A"},
            {"id": "d_cd", "source": "C", "target": "D"},
            {"id": "d_ee", "source": "E", "target": "E"},
        ],
        dataset=dataset,
    )

    res = container.analytics.cycles(dataset=dataset)
    assert res.has_cycles is True
    assert res.non_trivial_scc_count == 1
    assert res.self_loop_count == 1
    assert set(res.strongly_connected_components[0].nodes) == {"A", "B", "C"}
    assert res.self_loops == ["E"]


def test_6_temporal_cycle_creation(client: TestClient, container: Container):
    """6. Temporal cycle creation (valid only starting at valid_from)."""
    dataset = "test-temporal-create"
    _seed_test_graph(
        container,
        ["A", "B"],
        [
            # A -> B valid from 2024-01-01
            {"id": "d_ab", "source": "A", "target": "B", "valid_from": "2024-01-01T00:00:00Z"},
            # B -> A valid only starting 2024-06-01T00:00:00Z
            {"id": "d_ba", "source": "B", "target": "A", "valid_from": "2024-06-01T00:00:00Z"},
        ],
        dataset=dataset,
    )

    # Before creation: no cycle
    res_before = container.analytics.cycles(valid_at=datetime(2024, 5, 1, tzinfo=UTC), dataset=dataset)
    assert res_before.has_cycles is False
    assert res_before.non_trivial_scc_count == 0

    # After creation: cycle exists
    res_after = container.analytics.cycles(valid_at=datetime(2024, 6, 15, tzinfo=UTC), dataset=dataset)
    assert res_after.has_cycles is True
    assert res_after.non_trivial_scc_count == 1


def test_7_temporal_cycle_expiration(client: TestClient, container: Container):
    """7. Temporal cycle expiration (valid only until valid_to)."""
    dataset = "test-temporal-expire"
    _seed_test_graph(
        container,
        ["A", "B"],
        [
            {"id": "d_ab", "source": "A", "target": "B", "valid_from": "2024-01-01T00:00:00Z"},
            # B -> A expires at 2024-07-01T00:00:00Z
            {"id": "d_ba", "source": "B", "target": "A", "valid_from": "2024-01-01T00:00:00Z", "valid_to": "2024-07-01T00:00:00Z"},
        ],
        dataset=dataset,
    )

    # During validity
    res_active = container.analytics.cycles(valid_at=datetime(2024, 6, 1, tzinfo=UTC), dataset=dataset)
    assert res_active.has_cycles is True
    assert res_active.non_trivial_scc_count == 1

    # After expiration
    res_expired = container.analytics.cycles(valid_at=datetime(2024, 8, 1, tzinfo=UTC), dataset=dataset)
    assert res_expired.has_cycles is False
    assert res_expired.non_trivial_scc_count == 0


def test_8_exact_boundary_valid_to_half_open(client: TestClient, container: Container):
    """8. Exact boundary behavior: [valid_from, valid_to) is half-open."""
    dataset = "test-boundary"
    _seed_test_graph(
        container,
        ["A", "B"],
        [
            {"id": "d_ab", "source": "A", "target": "B", "valid_from": "2024-01-01T00:00:00Z"},
            {"id": "d_ba", "source": "B", "target": "A", "valid_from": "2024-01-01T00:00:00Z", "valid_to": "2024-07-01T00:00:00Z"},
        ],
        dataset=dataset,
    )

    # Exactly 1ms before expiration: cycle active
    t_inside = datetime(2024, 6, 30, 23, 59, 59, 999000, tzinfo=UTC)
    res_inside = container.analytics.cycles(valid_at=t_inside, dataset=dataset)
    assert res_inside.has_cycles is True
    assert res_inside.non_trivial_scc_count == 1

    # At the exact valid_to boundary instant: cycle inactive
    t_boundary = datetime(2024, 7, 1, 0, 0, 0, 0, tzinfo=UTC)
    res_boundary = container.analytics.cycles(valid_at=t_boundary, dataset=dataset)
    assert res_boundary.has_cycles is False
    assert res_boundary.non_trivial_scc_count == 0


def test_9_dataset_isolation(client: TestClient, container: Container):
    """9. Dataset isolation: cycles in dataset X do not leak into dataset Y."""
    ds_cyclic = "dataset-cyclic"
    ds_acyclic = "dataset-acyclic"

    # Cyclic in ds_cyclic
    _seed_test_graph(
        container,
        ["N1", "N2"],
        [
            {"id": "d1", "source": "N1", "target": "N2"},
            {"id": "d2", "source": "N2", "target": "N1"},
        ],
        dataset=ds_cyclic,
    )

    # Acyclic in ds_acyclic
    _seed_test_graph(
        container,
        ["N1", "N2"],
        [
            {"id": "d1_ac", "source": "N1", "target": "N2"},
        ],
        dataset=ds_acyclic,
    )

    res_cyclic = container.analytics.cycles(dataset=ds_cyclic)
    res_acyclic = container.analytics.cycles(dataset=ds_acyclic)

    assert res_cyclic.has_cycles is True
    assert res_cyclic.non_trivial_scc_count == 1

    assert res_acyclic.has_cycles is False
    assert res_acyclic.non_trivial_scc_count == 0


def test_10_mongodb_to_neo4j_synchronization(client: TestClient, container: Container):
    """10. MongoDB-to-Neo4j transactional outbox synchronization."""
    dataset = "test-sync-outbox"
    _seed_test_graph(
        container,
        ["S1", "S2"],
        [
            {"id": "d_sync", "source": "S1", "target": "S2"},
        ],
        dataset=dataset,
        sync=False,
    )

    # Check MongoDB outbox table
    drain_rep = container.sync.drain()
    assert drain_rep.claimed > 0
    assert drain_rep.done > 0
    assert drain_rep.retried == 0
    assert drain_rep.dead_lettered == 0

    # Ensure Neo4j projected edge exists
    with container.dbs.neo4j_driver.session() as sess:
        edge = sess.run(
            "MATCH (s:Component {id: 'S1', dataset: $ds})-[r:DEPENDS_ON]->(t:Component {id: 'S2', dataset: $ds}) "
            "RETURN r.dependency_id AS did",
            ds=dataset
        ).single()
        assert edge is not None
        assert edge["did"] == "d_sync"


def test_11_historical_known_at_behavior(client: TestClient, container: Container):
    """11. Historical known_at (transaction time) query behavior."""
    dataset = "test-known-at"
    t_past = datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)
    t_now = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)

    # Recorded now (recorded_at=t_now)
    _seed_test_graph(
        container,
        [
            {"id": "K1", "name": "K1", "recorded_at": t_now.isoformat()},
            {"id": "K2", "name": "K2", "recorded_at": t_now.isoformat()},
        ],
        [
            {"id": "d_k12", "source": "K1", "target": "K2", "valid_from": "2020-01-01T00:00:00Z", "recorded_at": t_now.isoformat()},
            {"id": "d_k21", "source": "K2", "target": "K1", "valid_from": "2020-01-01T00:00:00Z", "recorded_at": t_now.isoformat()},
        ],
        dataset=dataset,
    )

    # At known_at = t_past, the system did not know about these relationships yet
    res_past_belief = container.analytics.cycles(
        valid_at=datetime(2022, 1, 1, tzinfo=UTC),
        known_at=t_past,
        dataset=dataset
    )
    assert res_past_belief.has_cycles is False
    assert res_past_belief.non_trivial_scc_count == 0

    # At known_at = t_now, the system knows about the cycle
    res_current_belief = container.analytics.cycles(
        valid_at=datetime(2022, 1, 1, tzinfo=UTC),
        known_at=t_now,
        dataset=dataset
    )
    assert res_current_belief.has_cycles is True
    assert res_current_belief.non_trivial_scc_count == 1
