"""Neo4j graph traversal operations: direct search, multi-hop search, reachability."""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Sequence

from neo4j import Driver

from app.graph.cypher import (
    Direction,
    edge_filter,
    format_edge,
    format_node,
    normalize_rels,
    qpp_hop_pattern,
    quantifier,
    query_params,
)


class GraphTraversal:
    def __init__(self, driver: Driver, database: str) -> None:
        self.driver = driver
        self.database = database

    def node_exists(self, node_id: str, dataset: str = "default") -> bool:
        with self.driver.session(database=self.database) as s:
            rec = s.run(
                "MATCH (c:Component {id: $id}) "
                "WHERE ($dataset IS NULL OR c.dataset = $dataset OR (c.dataset IS NULL AND $dataset = 'default')) "
                "RETURN c.id AS id LIMIT 1",
                id=node_id,
                dataset=dataset,
            ).single()
            return rec is not None

    def nodes_exist(self, node_ids: Sequence[str], dataset: str = "default") -> set[str]:
        ids = list(set(node_ids))
        with self.driver.session(database=self.database) as s:
            return {
                r["id"]
                for r in s.run(
                    "MATCH (c:Component) "
                    "WHERE c.id IN $ids AND ($dataset IS NULL OR c.dataset = $dataset OR (c.dataset IS NULL AND $dataset = 'default')) "
                    "RETURN c.id AS id",
                    ids=ids,
                    dataset=dataset,
                )
            }

    def direct(
        self,
        node_id: str,
        *,
        direction: Direction,
        relationship_types: Sequence[str] | None,
        valid_at: datetime,
        known_at: datetime,
        offset: int,
        limit: int,
        dataset: str = "default",
    ) -> dict[str, Any]:
        t_start = time.perf_counter()
        rels = normalize_rels(relationship_types)
        flt = edge_filter(has_rel_filter=bool(rels))

        if direction == "outgoing":
            match = (
                f"MATCH (s:Component {{id: $id}})-[r:DEPENDS_ON]->(t:Component) "
                f"WHERE ($dataset IS NULL OR s.dataset = $dataset OR (s.dataset IS NULL AND $dataset = 'default')) AND {flt}"
            )
        elif direction == "incoming":
            match = (
                f"MATCH (s:Component)-[r:DEPENDS_ON]->(t:Component {{id: $id}}) "
                f"WHERE ($dataset IS NULL OR t.dataset = $dataset OR (t.dataset IS NULL AND $dataset = 'default')) AND {flt}"
            )
        else:
            match = (
                f"MATCH (s:Component)-[r:DEPENDS_ON]->(t:Component) "
                f"WHERE (s.id = $id OR t.id = $id) "
                f"  AND ($dataset IS NULL OR s.dataset = $dataset OR (s.dataset IS NULL AND $dataset = 'default')) AND {flt}"
            )

        count_q = f"{match} RETURN count(r) AS total"
        data_q = (
            f"{match} "
            "RETURN s.id AS src_id, properties(s) AS src_props, "
            "t.id AS tgt_id, properties(t) AS tgt_props, properties(r) AS rel_props "
            "ORDER BY r.dependency_id ASC SKIP $offset LIMIT $limit"
        )
        params = query_params(valid_at, known_at, rels, dataset=dataset, id=node_id, offset=offset, limit=limit)

        t_db0 = time.perf_counter()
        with self.driver.session(database=self.database) as sess:
            total = sess.run(count_q, params).single()["total"]
            rows = [r.data() for r in sess.run(data_q, params)] if total else []
        neo4j_ms = (time.perf_counter() - t_db0) * 1000

        items = []
        for row in rows:
            src_id, tgt_id = row["src_id"], row["tgt_id"]
            if src_id == tgt_id:
                edge_dir = "self"
                neighbor = format_node(row["tgt_props"])
            elif src_id == node_id:
                edge_dir = "outgoing"
                neighbor = format_node(row["tgt_props"])
            else:
                edge_dir = "incoming"
                neighbor = format_node(row["src_props"])
            items.append({
                "direction": edge_dir,
                "neighbor": neighbor,
                "edge": format_edge(row["rel_props"], src_id, tgt_id),
            })

        return {
            "items": items,
            "total": total,
            "neo4j_query_ms": round(neo4j_ms, 3),
            "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
        }

    def multi_hop(
        self,
        node_id: str,
        *,
        direction: Direction,
        max_depth: int,
        relationship_types: Sequence[str] | None,
        valid_at: datetime,
        known_at: datetime,
        include_paths: bool,
        offset: int,
        limit: int,
        dataset: str = "default",
    ) -> dict[str, Any]:
        t_start = time.perf_counter()
        rels = normalize_rels(relationship_types)
        hop = qpp_hop_pattern(direction, has_rel_filter=bool(rels))
        qnt = quantifier(1, max_depth)

        if include_paths:
            proj = (
                "node: properties(n), hops: length(p), "
                "path_nodes: [x IN nodes(p) | x.id], "
                "path_edges: [rel IN relationships(p) | "
                "{src: startNode(rel).id, tgt: endNode(rel).id, props: properties(rel)}]"
            )
        else:
            proj = "node: properties(n), hops: length(p)"

        q = (
            f"MATCH (start:Component {{id: $id}}) "
            f"WHERE ($dataset IS NULL OR start.dataset = $dataset OR (start.dataset IS NULL AND $dataset = 'default')) "
            f"MATCH p = SHORTEST 1 (start) {hop}{qnt} (n:Component) "
            f"WHERE n.id <> $id "
            f"WITH n, p ORDER BY length(p) ASC, n.id ASC "
            f"WITH collect({{{proj}}}) AS all_rows "
            f"RETURN size(all_rows) AS total, all_rows[$offset..$end] AS page"
        )
        params = query_params(valid_at, known_at, rels, dataset=dataset, id=node_id, offset=offset, end=offset + limit)

        t_db0 = time.perf_counter()
        with self.driver.session(database=self.database) as sess:
            rec = sess.run(q, params).single()
        neo4j_ms = (time.perf_counter() - t_db0) * 1000

        total = rec["total"] if rec else 0
        page = rec["page"] if rec else []

        items = []
        for row in page:
            entry: dict[str, Any] = {
                "node": format_node(row["node"]),
                "hops": row["hops"],
                "path_nodes": row.get("path_nodes"),
                "path_edges": None,
            }
            if include_paths and row.get("path_edges") is not None:
                entry["path_edges"] = [
                    format_edge(e["props"], e["src"], e["tgt"]) for e in row["path_edges"]
                ]
            items.append(entry)

        return {
            "items": items,
            "total": total,
            "neo4j_query_ms": round(neo4j_ms, 3),
            "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
        }

    def reachability(
        self,
        source_id: str,
        target_id: str,
        *,
        direction: Direction,
        max_depth: int | None,
        relationship_types: Sequence[str] | None,
        valid_at: datetime,
        known_at: datetime,
        include_path: bool = True,
        dataset: str = "default",
    ) -> dict[str, Any]:
        t_start = time.perf_counter()
        if source_id == target_id:
            return {
                "reachable": True,
                "hops": 0,
                "path_nodes": [source_id] if include_path else None,
                "path_edges": [] if include_path else None,
                "neo4j_query_ms": 0.0,
                "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
            }

        rels = normalize_rels(relationship_types)
        hop = qpp_hop_pattern(direction, has_rel_filter=bool(rels))
        qnt = quantifier(1, max_depth)

        if include_path:
            ret = (
                "RETURN length(p) AS hops, "
                "[x IN nodes(p) | x.id] AS path_nodes, "
                "[rel IN relationships(p) | "
                "{src: startNode(rel).id, tgt: endNode(rel).id, props: properties(rel)}] AS path_edges"
            )
        else:
            ret = "RETURN length(p) AS hops"

        q = (
            f"MATCH (a:Component {{id: $src}}), (b:Component {{id: $tgt}}) "
            f"WHERE ($dataset IS NULL OR a.dataset = $dataset OR (a.dataset IS NULL AND $dataset = 'default')) "
            f"  AND ($dataset IS NULL OR b.dataset = $dataset OR (b.dataset IS NULL AND $dataset = 'default')) "
            f"MATCH p = SHORTEST 1 (a) {hop}{qnt} (b) "
            f"{ret}"
        )
        params = query_params(valid_at, known_at, rels, dataset=dataset, src=source_id, tgt=target_id)

        t_db0 = time.perf_counter()
        with self.driver.session(database=self.database) as sess:
            rec = sess.run(q, params).single()
        neo4j_ms = (time.perf_counter() - t_db0) * 1000

        if rec is None:
            return {
                "reachable": False,
                "hops": None,
                "path_nodes": None,
                "path_edges": None,
                "neo4j_query_ms": round(neo4j_ms, 3),
                "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
            }

        path_edges = None
        if include_path:
            path_edges = [format_edge(e["props"], e["src"], e["tgt"]) for e in rec["path_edges"]]

        return {
            "reachable": True,
            "hops": rec["hops"],
            "path_nodes": rec.get("path_nodes") if include_path else None,
            "path_edges": path_edges,
            "neo4j_query_ms": round(neo4j_ms, 3),
            "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
        }
