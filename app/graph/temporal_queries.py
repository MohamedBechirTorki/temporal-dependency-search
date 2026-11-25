"""Neo4j queries for temporal as-of reconstruction and graph comparison."""
from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any, Sequence

from neo4j import Driver

from app.domain.time import as_utc, end_to_api, start_to_api
from app.graph.cypher import (
    Direction,
    edge_filter,
    normalize_rels,
    qpp_hop_pattern,
    quantifier,
    query_params,
)


def _format_edge_with_meta(props: dict[str, Any], source: str, target: str) -> dict[str, Any]:
    raw_meta = props.get("metadata_json")
    metadata = json.loads(raw_meta) if raw_meta else {}
    return {
        "dependency_id": props["dependency_id"],
        "version_id": props["version_id"],
        "source": source,
        "target": target,
        "relationship": props["relationship"],
        "weight": float(props["weight"]),
        "metadata": metadata,
        "valid_from": as_utc(props["valid_from"]),
        "valid_to": end_to_api(props["valid_to"]),
        "recorded_at": as_utc(props["tx_from"]),
    }


def _format_component(props: dict[str, Any], hops: int | None = None) -> dict[str, Any]:
    raw_meta = props.get("metadata_json")
    return {
        "id": props["id"],
        "name": props.get("name") or props["id"],
        "type": props.get("type") or "component",
        "description": props.get("description"),
        "metadata": json.loads(raw_meta) if raw_meta else {},
        "valid_from": start_to_api(props.get("valid_from")),
        "valid_to": end_to_api(props.get("valid_to")),
        "deleted": bool(props.get("deleted", False)),
        "created_at": as_utc(props["created_at"]) if props.get("created_at") else None,
        "updated_at": as_utc(props["updated_at"]) if props.get("updated_at") else None,
        "hops": hops,
    }


class TemporalGraphQueries:
    def __init__(self, driver: Driver, database: str) -> None:
        self.driver = driver
        self.database = database

    def as_of_subgraph(
        self,
        node_id: str,
        *,
        direction: Direction,
        max_depth: int,
        relationship_types: Sequence[str] | None,
        valid_at: datetime,
        known_at: datetime,
        dataset: str = "default",
    ) -> dict[str, Any]:
        """Reconstruct the bitemporal subgraph reachable from ``node_id`` within ``max_depth``."""
        t0 = time.perf_counter()
        rels = normalize_rels(relationship_types)
        hop = qpp_hop_pattern(direction, has_rel_filter=bool(rels), dataset_param="dataset")
        qnt = quantifier(1, max_depth)
        flt = edge_filter(has_rel_filter=bool(rels), dataset_param="dataset")

        reach_q = (
            f"MATCH (start:Component {{id: $id}}) "
            f"WHERE ($dataset IS NULL OR start.dataset = $dataset OR (start.dataset IS NULL AND $dataset = 'default')) "
            f"OPTIONAL MATCH p = SHORTEST 1 (start) {hop}{qnt} (n:Component) "
            f"WHERE n IS NULL OR (n.id <> $id AND ($dataset IS NULL OR n.dataset = $dataset OR (n.dataset IS NULL AND $dataset = 'default'))) "
            f"RETURN properties(start) AS start_props, "
            f"collect(DISTINCT {{id: n.id, hops: length(p), props: properties(n)}}) AS reached"
        )
        params = query_params(valid_at, known_at, rels, dataset=dataset, id=node_id)

        with self.driver.session(database=self.database) as sess:
            rec = sess.run(reach_q, params).single()
            if rec is None:
                return {"found": False, "nodes": {}, "edges": [], "neo4j_query_ms": 0.0}

            hop_map: dict[str, int] = {node_id: 0}
            node_props: dict[str, dict[str, Any]] = {node_id: _format_component(rec["start_props"], hops=0)}
            for entry in rec["reached"]:
                if entry["id"] is not None:
                    nid = entry["id"]
                    hop_map[nid] = entry["hops"]
                    node_props[nid] = _format_component(entry["props"], hops=entry["hops"])

            frontier = [nid for nid, h in hop_map.items() if h < max_depth]
            all_ids = list(hop_map.keys())
            if direction == "outgoing":
                edge_where = f"s.id IN $frontier AND t.id IN $all_ids AND {flt}"
            elif direction == "incoming":
                edge_where = f"t.id IN $frontier AND s.id IN $all_ids AND {flt}"
            else:
                edge_where = f"(s.id IN $frontier OR t.id IN $frontier) AND s.id IN $all_ids AND t.id IN $all_ids AND {flt}"

            edge_q = (
                f"MATCH (s:Component)-[r:DEPENDS_ON]->(t:Component) "
                f"WHERE ($dataset IS NULL OR s.dataset = $dataset OR (s.dataset IS NULL AND $dataset = 'default')) "
                f"  AND ($dataset IS NULL OR t.dataset = $dataset OR (t.dataset IS NULL AND $dataset = 'default')) "
                f"  AND {edge_where} "
                f"RETURN s.id AS src, t.id AS tgt, properties(r) AS props "
                f"ORDER BY r.dependency_id ASC"
            )
            edge_params = query_params(valid_at, known_at, rels, dataset=dataset, frontier=frontier, all_ids=all_ids)
            edges = [_format_edge_with_meta(r["props"], r["src"], r["tgt"]) for r in sess.run(edge_q, edge_params)]

        neo4j_ms = round((time.perf_counter() - t0) * 1000, 3)
        return {"found": True, "nodes": node_props, "edges": edges, "neo4j_query_ms": neo4j_ms}

    def as_of_graph(
        self,
        *,
        relationship_types: Sequence[str] | None,
        valid_at: datetime,
        known_at: datetime,
        include_nodes: bool = True,
        limit: int | None = None,
        offset: int = 0,
        dataset: str = "default",
    ) -> dict[str, Any]:
        """Reconstruct all visible edges (and optionally nodes) in the whole graph at (valid_at, known_at)."""
        t0 = time.perf_counter()
        rels = normalize_rels(relationship_types)
        flt = edge_filter(has_rel_filter=bool(rels), dataset_param="dataset")
        params = query_params(valid_at, known_at, rels, dataset=dataset, offset=offset, limit=limit or 0)

        with self.driver.session(database=self.database) as sess:
            total_edges = sess.run(
                f"MATCH ()-[r:DEPENDS_ON]->() WHERE {flt} RETURN count(r) AS n", params
            ).single()["n"]

            slice_clause = " SKIP $offset LIMIT $limit" if limit is not None else ""
            edge_q = (
                f"MATCH (s:Component)-[r:DEPENDS_ON]->(t:Component) "
                f"WHERE ($dataset IS NULL OR s.dataset = $dataset OR (s.dataset IS NULL AND $dataset = 'default')) "
                f"  AND ($dataset IS NULL OR t.dataset = $dataset OR (t.dataset IS NULL AND $dataset = 'default')) "
                f"  AND {flt} "
                f"RETURN s.id AS src, t.id AS tgt, properties(r) AS props "
                f"ORDER BY r.dependency_id ASC{slice_clause}"
            )
            edges = [_format_edge_with_meta(r["props"], r["src"], r["tgt"]) for r in sess.run(edge_q, params)]

            node_props: dict[str, dict[str, Any]] = {}
            if include_nodes:
                # Filter out nodes not yet created at known_at; nodes updated after known_at
                # will be reconciled against MongoDB node_history by TemporalService.
                node_q = (
                    "MATCH (c:Component) "
                    "WHERE ($dataset IS NULL OR c.dataset = $dataset OR (c.dataset IS NULL AND $dataset = 'default')) "
                    "  AND c.created_at IS NOT NULL AND c.created_at <= $known_at "
                    "RETURN c.id AS id, properties(c) AS props ORDER BY c.id ASC"
                )
                for row in sess.run(node_q, dataset=dataset, known_at=known_at):
                    node_props[row["id"]] = _format_component(row["props"], hops=None)

        neo4j_ms = round((time.perf_counter() - t0) * 1000, 3)
        return {
            "nodes": node_props,
            "edges": edges,
            "total_edges": total_edges,
            "neo4j_query_ms": neo4j_ms,
        }
