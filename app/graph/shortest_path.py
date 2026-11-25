"""Shortest-path queries (unweighted in Cypher; weighted Dijkstra over Neo4j)."""
from __future__ import annotations

import heapq
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

# Cap per-node Bolt round trips during weighted Dijkstra; if exploration exceeds
# this, load the remaining visible subgraph in one query and finish in memory.
LAZY_EXPAND_QUERY_LIMIT = 64


class ShortestPathFinder:
    def __init__(self, driver: Driver, database: str) -> None:
        self.driver = driver
        self.database = database

    def find(
        self,
        source_id: str,
        target_id: str,
        *,
        direction: Direction,
        weighted: bool,
        max_depth: int | None,
        relationship_types: Sequence[str] | None,
        valid_at: datetime,
        known_at: datetime,
        dataset: str = "default",
    ) -> dict[str, Any]:
        t_start = time.perf_counter()
        if source_id == target_id:
            node = self._load_nodes([source_id], dataset=dataset)[0]
            return {
                "found": True,
                "weighted": weighted,
                "hops": 0,
                "total_weight": 0.0,
                "nodes": [node],
                "edges": [],
                "algorithm": "trivial_self",
                "neo4j_query_ms": 0.0,
                "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
            }

        rels = normalize_rels(relationship_types)
        if not weighted:
            return self._unweighted_cypher(
                source_id, target_id, direction=direction, max_depth=max_depth,
                rels=rels, valid_at=valid_at, known_at=known_at, dataset=dataset, t_start=t_start,
            )
        return self._weighted_dijkstra(
            source_id, target_id, direction=direction, max_depth=max_depth,
            rels=rels, valid_at=valid_at, known_at=known_at, dataset=dataset, t_start=t_start,
        )

    def _load_nodes(self, ids: list[str], dataset: str = "default") -> list[dict[str, Any]]:
        with self.driver.session(database=self.database) as sess:
            q = (
                "MATCH (c:Component) "
                "WHERE c.id IN $ids AND ($dataset IS NULL OR c.dataset = $dataset OR (c.dataset IS NULL AND $dataset = 'default')) "
                "RETURN c.id AS id, properties(c) AS props"
            )
            rows = {
                r["id"]: format_node(r["props"])
                for r in sess.run(q, ids=ids, dataset=dataset)
            }
        return [rows[nid] for nid in ids]

    def _unweighted_cypher(
        self,
        source_id: str,
        target_id: str,
        *,
        direction: Direction,
        max_depth: int | None,
        rels: list[str] | None,
        valid_at: datetime,
        known_at: datetime,
        dataset: str = "default",
        t_start: float,
    ) -> dict[str, Any]:
        hop = qpp_hop_pattern(direction, has_rel_filter=bool(rels))
        qnt = quantifier(1, max_depth)
        q = (
            f"MATCH (a:Component {{id: $src}}), (b:Component {{id: $tgt}}) "
            f"WHERE ($dataset IS NULL OR a.dataset = $dataset OR (a.dataset IS NULL AND $dataset = 'default')) "
            f"  AND ($dataset IS NULL OR b.dataset = $dataset OR (b.dataset IS NULL AND $dataset = 'default')) "
            f"MATCH p = SHORTEST 1 (a) {hop}{qnt} (b) "
            "RETURN length(p) AS hops, "
            "[n IN nodes(p) | properties(n)] AS node_props, "
            "[rel IN relationships(p) | "
            "{src: startNode(rel).id, tgt: endNode(rel).id, props: properties(rel)}] AS rels"
        )
        params = query_params(valid_at, known_at, rels, dataset=dataset, src=source_id, tgt=target_id)

        t_db0 = time.perf_counter()
        with self.driver.session(database=self.database) as sess:
            rec = sess.run(q, params).single()
        neo4j_ms = (time.perf_counter() - t_db0) * 1000

        if rec is None:
            return {
                "found": False,
                "weighted": False,
                "hops": None,
                "total_weight": None,
                "nodes": [],
                "edges": [],
                "algorithm": "cypher_shortest_bfs",
                "neo4j_query_ms": round(neo4j_ms, 3),
                "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
            }

        nodes = [format_node(np) for np in rec["node_props"]]
        edges = [format_edge(e["props"], e["src"], e["tgt"]) for e in rec["rels"]]
        total_weight = round(sum(e["weight"] for e in edges), 9)
        return {
            "found": True,
            "weighted": False,
            "hops": rec["hops"],
            "total_weight": total_weight,
            "nodes": nodes,
            "edges": edges,
            "algorithm": "cypher_shortest_bfs",
            "neo4j_query_ms": round(neo4j_ms, 3),
            "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
        }

    def _weighted_dijkstra(
        self,
        source_id: str,
        target_id: str,
        *,
        direction: Direction,
        max_depth: int | None,
        rels: list[str] | None,
        valid_at: datetime,
        known_at: datetime,
        dataset: str = "default",
        t_start: float,
    ) -> dict[str, Any]:
        # 1. Fast Neo4j reachability prune so unreachable targets return immediately
        hop = qpp_hop_pattern(direction, has_rel_filter=bool(rels))
        qnt = quantifier(1, max_depth)
        reach_q = (
            f"MATCH (a:Component {{id: $src}}), (b:Component {{id: $tgt}}) "
            f"WHERE ($dataset IS NULL OR a.dataset = $dataset OR (a.dataset IS NULL AND $dataset = 'default')) "
            f"  AND ($dataset IS NULL OR b.dataset = $dataset OR (b.dataset IS NULL AND $dataset = 'default')) "
            f"MATCH p = SHORTEST 1 (a) {hop}{qnt} (b) RETURN 1 AS ok"
        )
        params = query_params(valid_at, known_at, rels, dataset=dataset, src=source_id, tgt=target_id)
        neo4j_s = 0.0

        with self.driver.session(database=self.database) as sess:
            t0 = time.perf_counter()
            reachable = sess.run(reach_q, params).single() is not None
            neo4j_s += time.perf_counter() - t0
            if not reachable:
                return {
                    "found": False,
                    "weighted": True,
                    "hops": None,
                    "total_weight": None,
                    "nodes": [],
                    "edges": [],
                    "algorithm": "dijkstra_neo4j",
                    "neo4j_query_ms": round(neo4j_s * 1000, 3),
                    "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
                }

            flt = edge_filter(has_rel_filter=bool(rels))
            if direction == "outgoing":
                nbr_q = (
                    f"MATCH (u:Component {{id: $u}})-[r:DEPENDS_ON]->(v:Component) WHERE {flt} "
                    "RETURN v.id AS nxt, u.id AS src, v.id AS tgt, properties(r) AS props"
                )
                bulk_q = (
                    f"MATCH (u:Component)-[r:DEPENDS_ON]->(v:Component) WHERE {flt} "
                    "RETURN u.id AS key, v.id AS nxt, u.id AS src, v.id AS tgt, properties(r) AS props"
                )
            elif direction == "incoming":
                nbr_q = (
                    f"MATCH (v:Component)-[r:DEPENDS_ON]->(u:Component {{id: $u}}) WHERE {flt} "
                    "RETURN v.id AS nxt, v.id AS src, u.id AS tgt, properties(r) AS props"
                )
                bulk_q = (
                    f"MATCH (v:Component)-[r:DEPENDS_ON]->(u:Component) WHERE {flt} "
                    "RETURN u.id AS key, v.id AS nxt, v.id AS src, u.id AS tgt, properties(r) AS props"
                )
            else:
                nbr_q = (
                    f"MATCH (u:Component {{id: $u}})-[r:DEPENDS_ON]-(v:Component) WHERE {flt} "
                    "RETURN v.id AS nxt, startNode(r).id AS src, endNode(r).id AS tgt, properties(r) AS props"
                )
                bulk_q = (
                    f"MATCH (s:Component)-[r:DEPENDS_ON]->(t:Component) WHERE {flt} "
                    "RETURN s.id AS src, t.id AS tgt, properties(r) AS props"
                )

            adj: dict[str, list[tuple[str, dict[str, Any]]]] = {}
            bulk_loaded = False
            queries = 0

            def neighbors(u: str) -> list[tuple[str, dict[str, Any]]]:
                nonlocal neo4j_s, bulk_loaded, queries
                if u in adj:
                    return adj[u]
                if bulk_loaded:
                    return []
                if queries >= LAZY_EXPAND_QUERY_LIMIT:
                    t1 = time.perf_counter()
                    rows = list(sess.run(bulk_q, query_params(valid_at, known_at, rels)))
                    neo4j_s += time.perf_counter() - t1
                    adj.clear()
                    if direction == "both":
                        for row in rows:
                            edge = format_edge(row["props"], row["src"], row["tgt"])
                            adj.setdefault(row["src"], []).append((row["tgt"], edge))
                            if row["src"] != row["tgt"]:
                                adj.setdefault(row["tgt"], []).append((row["src"], edge))
                    else:
                        for row in rows:
                            edge = format_edge(row["props"], row["src"], row["tgt"])
                            adj.setdefault(row["key"], []).append((row["nxt"], edge))
                    bulk_loaded = True
                    return adj.get(u, [])

                t1 = time.perf_counter()
                rows = list(sess.run(nbr_q, query_params(valid_at, known_at, rels, u=u)))
                neo4j_s += time.perf_counter() - t1
                queries += 1
                out = [(row["nxt"], format_edge(row["props"], row["src"], row["tgt"])) for row in rows]
                adj[u] = out
                return out

            # Heap entries: (cost, hops, node_id)
            pq: list[tuple[float, int, str]] = [(0.0, 0, source_id)]
            # State: (node_id, hops) when max_depth is bounded, else node_id
            best: dict[Any, float] = {(source_id, 0) if max_depth is not None else source_id: 0.0}
            parent: dict[Any, tuple[Any, dict[str, Any]]] = {}
            goal_state: Any = None

            while pq:
                cost, hops, u = heapq.heappop(pq)
                state = (u, hops) if max_depth is not None else u
                if cost > best.get(state, float("inf")):
                    continue
                if u == target_id:
                    goal_state = state
                    break
                if max_depth is not None and hops >= max_depth:
                    continue
                for v, edge in neighbors(u):
                    n_cost = cost + edge["weight"]
                    n_hops = hops + 1
                    n_state = (v, n_hops) if max_depth is not None else v
                    if n_cost < best.get(n_state, float("inf")):
                        best[n_state] = n_cost
                        parent[n_state] = (state, edge)
                        heapq.heappush(pq, (n_cost, n_hops, v))

            if goal_state is None:
                return {
                    "found": False,
                    "weighted": True,
                    "hops": None,
                    "total_weight": None,
                    "nodes": [],
                    "edges": [],
                    "algorithm": "dijkstra_neo4j",
                    "neo4j_query_ms": round(neo4j_s * 1000, 3),
                    "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
                }

            rev_edges: list[dict[str, Any]] = []
            rev_node_ids: list[str] = []
            cur = goal_state
            while cur in parent:
                rev_node_ids.append(cur[0] if max_depth is not None else cur)
                prev, edge = parent[cur]
                rev_edges.append(edge)
                cur = prev
            rev_node_ids.append(source_id)
            node_ids = list(reversed(rev_node_ids))
            edges = list(reversed(rev_edges))

            t2 = time.perf_counter()
            props_map = {
                r["id"]: format_node(r["props"])
                for r in sess.run(
                    "MATCH (c:Component) WHERE c.id IN $ids "
                    "  AND ($dataset IS NULL OR c.dataset = $dataset OR (c.dataset IS NULL AND $dataset = 'default')) "
                    "RETURN c.id AS id, properties(c) AS props",
                    ids=list(set(node_ids)),
                    dataset=dataset,
                )
            }
            neo4j_s += time.perf_counter() - t2

        return {
            "found": True,
            "weighted": True,
            "hops": len(edges),
            "total_weight": round(best[goal_state], 9),
            "nodes": [props_map[nid] for nid in node_ids],
            "edges": edges,
            "algorithm": "dijkstra_neo4j",
            "neo4j_query_ms": round(neo4j_s * 1000, 3),
            "execution_time_ms": round((time.perf_counter() - t_start) * 1000, 3),
        }
