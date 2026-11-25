"""Graph analytics & impact analysis service."""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Sequence

from app.api.schemas.analytics import (
    AffectedNode,
    AnalyticsCyclesResponse,
    AnalyticsOverviewResponse,
    CentralityEntry,
    CentralityResponse,
    ImpactRequest,
    ImpactResponse,
    NodeDegreeSummary,
    SccEntry,
)
from app.config import Settings
from app.domain.errors import InvalidRequestError, NotFoundError
from app.graph.graph_algorithms import (
    betweenness_centrality,
    compute_degrees,
    find_sample_cycles,
    pagerank,
    strongly_connected_components,
    weakly_connected_components,
)
from app.graph.temporal_queries import TemporalGraphQueries
from app.graph.traversal import GraphTraversal
from app.repositories.mongodb_repository import MongoRepository
from app.services.common import TxClock, resolve_node_snapshots

DEFAULT_BETWEENNESS_EXACT_LIMIT = 1500
DEFAULT_BETWEENNESS_SAMPLES = 256


class AnalyticsService:
    def __init__(
        self,
        mongo: MongoRepository,
        traversal: GraphTraversal,
        queries: TemporalGraphQueries,
        settings: Settings,
        clock: TxClock,
    ) -> None:
        self.mongo = mongo
        self.traversal = traversal
        self.queries = queries
        self.settings = settings
        self.clock = clock

    def _resolve_times(self, valid_at: datetime | None, known_at: datetime | None) -> tuple[datetime, datetime]:
        now = self.clock.now()
        return valid_at or now, known_at or now

    def _load_active_snapshot(
        self,
        *,
        relationship_types: Sequence[str] | None,
        valid_at: datetime,
        known_at: datetime,
        dataset: str = "default",
    ) -> tuple[dict[str, dict[str, Any]], list[tuple[str, str]], int, float]:
        raw = self.queries.as_of_graph(
            relationship_types=relationship_types,
            valid_at=valid_at,
            known_at=known_at,
            include_nodes=True,
            limit=None,
            dataset=dataset,
        )
        nodes_map = resolve_node_snapshots(self.mongo, raw["nodes"], valid_at, known_at)
        edges = [
            (e["source"], e["target"])
            for e in raw["edges"]
            if e["source"] in nodes_map and e["target"] in nodes_map
        ]
        return nodes_map, edges, len(edges), raw["neo4j_query_ms"]

    def overview(
        self,
        *,
        valid_at: datetime | None = None,
        known_at: datetime | None = None,
        relationship_types: Sequence[str] | None = None,
        top_k: int = 10,
        dataset: str = "default",
    ) -> AnalyticsOverviewResponse:
        t_start = time.perf_counter()
        v, k = self._resolve_times(valid_at, known_at)
        nodes_map, edges, edge_count, neo4j_ms = self._load_active_snapshot(
            relationship_types=relationship_types, valid_at=v, known_at=k, dataset=dataset
        )

        t_comp0 = time.perf_counter()
        node_ids = list(nodes_map.keys())
        n = len(node_ids)
        deg = compute_degrees(node_ids, edges)
        wccs = weakly_connected_components(node_ids, edges)
        sccs = strongly_connected_components(node_ids, edges)
        non_trivial_sccs = [c for c in sccs if len(c) > 1]
        has_cycles = bool(non_trivial_sccs or deg.self_loops)

        density = len(deg.unique_pairs) / float(n * (n - 1)) if n > 1 else 0.0
        avg_in = sum(deg.in_degree.values()) / float(n) if n else 0.0
        avg_out = sum(deg.out_degree.values()) / float(n) if n else 0.0
        max_in = max(deg.in_degree.values(), default=0)
        max_out = max(deg.out_degree.values(), default=0)

        def _summary(nid: str) -> NodeDegreeSummary:
            info = nodes_map[nid]
            return NodeDegreeSummary(
                id=nid,
                name=info["name"],
                type=info["type"],
                in_degree=deg.in_degree[nid],
                out_degree=deg.out_degree[nid],
                total_degree=deg.total_degree[nid],
            )

        most_depended = [
            _summary(nid)
            for nid in sorted(node_ids, key=lambda x: (-deg.in_degree[x], -deg.total_degree[x], x))[:top_k]
        ]
        most_dependent = [
            _summary(nid)
            for nid in sorted(node_ids, key=lambda x: (-deg.out_degree[x], -deg.total_degree[x], x))[:top_k]
        ]
        compute_ms = round((time.perf_counter() - t_comp0) * 1000, 3)

        return AnalyticsOverviewResponse(
            valid_at=v,
            known_at=k,
            node_count=n,
            active_edge_count=edge_count,
            unique_directed_pairs=len(deg.unique_pairs),
            self_loop_count=len(deg.self_loops),
            density=round(density, 8),
            avg_in_degree=round(avg_in, 4),
            avg_out_degree=round(avg_out, 4),
            max_in_degree=max_in,
            max_out_degree=max_out,
            weakly_connected_component_count=len(wccs),
            largest_wcc_size=len(wccs[0]) if wccs else 0,
            strongly_connected_component_count=len(sccs),
            non_trivial_scc_count=len(non_trivial_sccs),
            has_cycles=has_cycles,
            most_depended_upon=most_depended,
            most_dependent=most_dependent,
            neo4j_query_ms=neo4j_ms,
            compute_ms=compute_ms,
            execution_time_ms=round((time.perf_counter() - t_start) * 1000, 3),
        )

    def centrality(
        self,
        *,
        valid_at: datetime | None = None,
        known_at: datetime | None = None,
        relationship_types: Sequence[str] | None = None,
        top_k: int = 20,
        betweenness_samples: int | None = None,
        node_ids_filter: Sequence[str] | None = None,
        damping: float = 0.85,
        dataset: str = "default",
    ) -> CentralityResponse:
        t_start = time.perf_counter()
        v, k = self._resolve_times(valid_at, known_at)
        nodes_map, edges, edge_count, neo4j_ms = self._load_active_snapshot(
            relationship_types=relationship_types, valid_at=v, known_at=k, dataset=dataset
        )

        t_comp0 = time.perf_counter()
        node_ids = list(nodes_map.keys())
        n = len(node_ids)
        deg = compute_degrees(node_ids, edges)
        pr = pagerank(node_ids, deg.unique_pairs, damping=damping)

        sample_k = betweenness_samples
        if sample_k is None and n > DEFAULT_BETWEENNESS_EXACT_LIMIT:
            sample_k = DEFAULT_BETWEENNESS_SAMPLES
        bw, bw_algo, bw_samples = betweenness_centrality(node_ids, deg.unique_pairs, sample_k=sample_k)

        denom = float(n - 1) if n > 1 else 1.0

        def _entry(nid: str) -> CentralityEntry:
            info = nodes_map[nid]
            ind = deg.in_degree[nid]
            outd = deg.out_degree[nid]
            return CentralityEntry(
                id=nid,
                name=info["name"],
                type=info["type"],
                in_degree=ind,
                out_degree=outd,
                in_degree_centrality=round(ind / denom if n > 1 else 0.0, 6),
                out_degree_centrality=round(outd / denom if n > 1 else 0.0, 6),
                pagerank=round(pr[nid], 8),
                betweenness=round(bw[nid], 8),
            )

        top_pr = [_entry(nid) for nid in sorted(node_ids, key=lambda x: (-pr[x], x))[:top_k]]
        top_bw = [_entry(nid) for nid in sorted(node_ids, key=lambda x: (-bw[x], x))[:top_k]]
        top_in = [_entry(nid) for nid in sorted(node_ids, key=lambda x: (-deg.in_degree[x], x))[:top_k]]
        selected = [_entry(nid) for nid in (node_ids_filter or []) if nid in nodes_map]
        compute_ms = round((time.perf_counter() - t_comp0) * 1000, 3)

        return CentralityResponse(
            valid_at=v,
            known_at=k,
            node_count=n,
            edge_count=edge_count,
            pagerank_damping=damping,
            betweenness_algorithm=bw_algo,
            betweenness_sample_size=bw_samples,
            top_by_pagerank=top_pr,
            top_by_betweenness=top_bw,
            top_by_in_degree=top_in,
            selected_nodes=selected,
            neo4j_query_ms=neo4j_ms,
            compute_ms=compute_ms,
            execution_time_ms=round((time.perf_counter() - t_start) * 1000, 3),
        )

    def cycles(
        self,
        *,
        valid_at: datetime | None = None,
        known_at: datetime | None = None,
        relationship_types: Sequence[str] | None = None,
        max_cycles: int = 100,
        max_cycle_length: int = 15,
        dataset: str = "default",
    ) -> AnalyticsCyclesResponse:
        t_start = time.perf_counter()
        v, k = self._resolve_times(valid_at, known_at)
        nodes_map, edges, _, neo4j_ms = self._load_active_snapshot(
            relationship_types=relationship_types, valid_at=v, known_at=k, dataset=dataset
        )

        t_comp0 = time.perf_counter()
        node_ids = list(nodes_map.keys())
        deg = compute_degrees(node_ids, edges)
        sccs = strongly_connected_components(node_ids, edges)
        non_trivial = [c for c in sccs if len(c) > 1]
        sample_cycles = find_sample_cycles(
            edges, non_trivial, deg.self_loops, max_cycles=max_cycles, max_cycle_length=max_cycle_length
        )
        compute_ms = round((time.perf_counter() - t_comp0) * 1000, 3)

        return AnalyticsCyclesResponse(
            valid_at=v,
            known_at=k,
            has_cycles=bool(non_trivial or deg.self_loops),
            non_trivial_scc_count=len(non_trivial),
            self_loop_count=len(deg.self_loops),
            strongly_connected_components=[SccEntry(nodes=c, size=len(c)) for c in non_trivial],
            self_loops=deg.self_loops,
            sample_cycles=sample_cycles,
            neo4j_query_ms=neo4j_ms,
            compute_ms=compute_ms,
            execution_time_ms=round((time.perf_counter() - t_start) * 1000, 3),
        )

    def impact(self, req: ImpactRequest) -> ImpactResponse:
        t_start = time.perf_counter()
        if req.max_depth > self.settings.max_traversal_depth:
            raise InvalidRequestError(
                f"max_depth ({req.max_depth}) exceeds MAX_TRAVERSAL_DEPTH ({self.settings.max_traversal_depth})"
            )
        dataset = getattr(req, "dataset", "default") or "default"
        if not self.traversal.node_exists(req.node_id, dataset=dataset) and self.mongo.get_node(req.node_id) is None:
            raise NotFoundError(f"node {req.node_id!r} not found")

        v, k = self._resolve_times(req.valid_at, req.known_at)
        raw = self.traversal.multi_hop(
            req.node_id,
            direction="incoming",
            max_depth=req.max_depth,
            relationship_types=req.relationship_types,
            valid_at=v,
            known_at=k,
            include_paths=req.include_paths,
            offset=0,
            limit=100000,
            dataset=dataset,
        )

        direct: list[AffectedNode] = []
        indirect: list[AffectedNode] = []
        max_hop = 0

        for item in raw["items"]:
            hops = item["hops"]
            max_hop = max(max_hop, hops)
            dep_path = list(reversed(item["path_nodes"])) if item.get("path_nodes") else None
            path_edges = list(reversed(item["path_edges"])) if item.get("path_edges") else None
            entry = AffectedNode(
                node=item["node"],
                hops=hops,
                dependency_path=dep_path,
                path_edges=path_edges,
            )
            if hops == 1:
                direct.append(entry)
            else:
                indirect.append(entry)

        return ImpactResponse(
            target_node_id=req.node_id,
            direction_convention="incoming (X -> ... -> target because A -> B means A depends on B)",
            max_depth=req.max_depth,
            valid_at=v,
            known_at=k,
            directly_affected=direct,
            indirectly_affected=indirect,
            directly_affected_count=len(direct),
            indirectly_affected_count=len(indirect),
            total_affected=len(direct) + len(indirect),
            max_impact_depth=max_hop,
            neo4j_query_ms=raw["neo4j_query_ms"],
            execution_time_ms=round((time.perf_counter() - t_start) * 1000, 3),
        )
