"""Cycle detection against the Neo4j graph projection."""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Sequence

from neo4j import Driver

from app.graph.cypher import format_edge, normalize_rels, qpp_hop_pattern, quantifier, query_params


class CycleDetector:
    def __init__(self, driver: Driver, database: str) -> None:
        self.driver = driver
        self.database = database

    def would_create_cycle(
        self,
        source_id: str,
        target_id: str,
        *,
        valid_at: datetime,
        known_at: datetime,
        relationship_types: Sequence[str] | None = None,
        max_depth: int | None = None,
        dataset: str = "default",
    ) -> dict[str, Any]:
        """Check whether adding directed edge source -> target creates a cycle.

        Adding source -> target creates a directed cycle iff target can already
        reach source along directed outgoing edges visible at (valid_at, known_at).
        """
        t_start = time.perf_counter()
        if source_id == target_id:
            return {
                "creates_cycle": True,
                "cycle_length": 1,
                "cycle_nodes": [source_id, source_id],
                "existing_edges": [],
                "neo4j_query_ms": 0.0,
                "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
            }

        rels = normalize_rels(relationship_types)
        hop = qpp_hop_pattern("outgoing", has_rel_filter=bool(rels), dataset_param="dataset")
        qnt = quantifier(1, max_depth)
        q = (
            f"MATCH (t:Component {{id: $tgt}}), (s:Component {{id: $src}}) "
            f"WHERE ($dataset IS NULL OR t.dataset = $dataset OR (t.dataset IS NULL AND $dataset = 'default')) "
            f"  AND ($dataset IS NULL OR s.dataset = $dataset OR (s.dataset IS NULL AND $dataset = 'default')) "
            f"MATCH p = SHORTEST 1 (t) {hop}{qnt} (s) "
            "RETURN [n IN nodes(p) | n.id] AS path_nodes, "
            "[idx IN range(0, size(relationships(p)) - 1) | "
            "{src: nodes(p)[idx].id, tgt: nodes(p)[idx + 1].id, props: properties(relationships(p)[idx])}] AS path_edges"
        )
        params = query_params(valid_at, known_at, rels, dataset=dataset, src=source_id, tgt=target_id)

        t_db0 = time.perf_counter()
        with self.driver.session(database=self.database) as sess:
            rec = sess.run(q, params).single()
        neo4j_ms = (time.perf_counter() - t_db0) * 1000

        if rec is None:
            return {
                "creates_cycle": False,
                "cycle_length": None,
                "cycle_nodes": [],
                "existing_edges": [],
                "neo4j_query_ms": round(neo4j_ms, 3),
                "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
            }

        # Path found is target -> ... -> source; prepending source closes the cycle
        cycle_nodes = [source_id, *rec["path_nodes"]]
        existing_edges = [format_edge(e["props"], e["src"], e["tgt"]) for e in rec["path_edges"]]
        return {
            "creates_cycle": True,
            "cycle_length": len(existing_edges) + 1,
            "cycle_nodes": cycle_nodes,
            "existing_edges": existing_edges,
            "neo4j_query_ms": round(neo4j_ms, 3),
            "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
        }
