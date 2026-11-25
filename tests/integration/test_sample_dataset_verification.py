"""End-to-end integration tests validating Section 6 requirements using sample_temporal_graph.zip."""
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from app.api.schemas.analytics import ImpactRequest
from app.api.schemas.search import DirectSearchRequest, MultiHopSearchRequest, ShortestPathRequest
from app.api.schemas.temporal import AsOfRequest
from app.container import Container

UTC = timezone.utc


def test_section_6_sample_dataset_validation(client: TestClient, container: Container):
    zip_path = Path("examples/sample_temporal_graph.zip")
    assert zip_path.exists(), "Sample zip must exist"

    # 1. Ingest with dataset="sample"
    metrics = container.importer.ingest_universal(
        zip_path,
        sync=True,
        dataset="sample",
        replace=True,
    )
    assert metrics.nodes_ingested == 7, f"Expected 7 nodes, got {metrics.nodes_ingested}"
    assert metrics.dependencies_ingested == 8, f"Expected 8 dependencies, got {metrics.dependencies_ingested}"

    # 2. Shortest Path: frontend-app -> orders-db
    sp_res = container.search.shortest_path(ShortestPathRequest(
        source_id="frontend-app",
        target_id="orders-db",
        dataset="sample",
    ))
    assert sp_res.found is True
    assert sp_res.hops == 3
    path_node_ids = [n.id for n in sp_res.nodes]
    assert path_node_ids == ["frontend-app", "api-gateway", "order-service", "orders-db"]

    # 3. Outgoing dependencies for order-service at depth 3
    direct_res = container.search.direct(DirectSearchRequest(
        node_id="order-service",
        direction="outgoing",
        dataset="sample",
    ))
    direct_neighbors = {item.neighbor.id for item in direct_res.items}
    assert direct_neighbors == {"payment-service", "user-db", "orders-db"}

    multi_res = container.search.multi_hop(MultiHopSearchRequest(
        node_id="order-service",
        direction="outgoing",
        max_depth=3,
        dataset="sample",
    ))
    reached_ids = {item.node.id for item in multi_res.items}
    assert {"payment-service", "user-db", "orders-db"}.issubset(reached_ids)

    # 4. Impact Analysis: impact of user-db
    impact_res = container.analytics.impact(ImpactRequest(
        node_id="user-db",
        max_depth=10,
        dataset="sample",
    ))
    direct_affected = [a.node.id for a in impact_res.directly_affected]
    indirect_affected = [a.node.id for a in impact_res.indirectly_affected]
    assert direct_affected == ["order-service"]
    assert "api-gateway" in indirect_affected
    assert "frontend-app" in indirect_affected

    # 5. Cycle Detection: June 2024 vs September 2024
    june_time = datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)
    sept_time = datetime(2024, 9, 1, 0, 0, 0, tzinfo=UTC)

    cycles_june = container.analytics.cycles(valid_at=june_time, dataset="sample")
    assert cycles_june.has_cycles is True
    assert cycles_june.non_trivial_scc_count == 1
    scc_nodes = set(cycles_june.strongly_connected_components[0].nodes)
    assert scc_nodes == {"legacy-service", "order-service", "user-db"}

    cycles_sept = container.analytics.cycles(valid_at=sept_time, dataset="sample")
    assert cycles_sept.has_cycles is False
    assert cycles_sept.non_trivial_scc_count == 0

    # 6. Historical query (As-of): June 1, 2024 vs September 1, 2024
    as_of_june = container.temporal.as_of(AsOfRequest(
        valid_at=june_time,
        dataset="sample",
    ))
    assert as_of_june.total_nodes == 7
    assert as_of_june.total_edges == 8
    june_edge_pairs = {(e.source, e.target) for e in as_of_june.edges}
    assert ("legacy-service", "order-service") in june_edge_pairs

    as_of_sept = container.temporal.as_of(AsOfRequest(
        valid_at=sept_time,
        dataset="sample",
    ))
    assert as_of_sept.total_nodes == 7
    assert as_of_sept.total_edges == 7
    sept_edge_pairs = {(e.source, e.target) for e in as_of_sept.edges}
    assert ("legacy-service", "order-service") not in sept_edge_pairs
