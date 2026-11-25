"""Temporal service: historical as-of reconstruction, graph comparison, and full entity history."""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from app.api.schemas.dependencies import DependencyEventView, DependencySummary
from app.api.schemas.temporal import (
    AsOfEdge,
    AsOfNode,
    AsOfRequest,
    AsOfResponse,
    CompareRequest,
    CompareResponse,
    CompareSummary,
    EdgeDiff,
    NodeTimelineResponse,
)
from app.config import Settings
from app.domain.errors import InvalidRequestError, NotFoundError
from app.domain.temporal import canonical_json
from app.domain.time import MAX_TIME
from app.graph.cypher import Direction
from app.graph.temporal_queries import TemporalGraphQueries
from app.repositories.mongodb_repository import MongoRepository
from app.services.common import TxClock, resolve_node_snapshots
from app.services.dependency_service import version_view
from app.services.node_service import NodeService


def _diff_metadata(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    added = {k: after[k] for k in after if k not in before}
    removed = {k: before[k] for k in before if k not in after}
    changed = {
        k: {"before": before[k], "after": after[k]}
        for k in before
        if k in after and canonical_json(before[k]) != canonical_json(after[k])
    }
    return {"added": added, "removed": removed, "changed": changed}


class TemporalService:
    def __init__(
        self,
        mongo: MongoRepository,
        queries: TemporalGraphQueries,
        node_service: NodeService,
        settings: Settings,
        clock: TxClock,
    ) -> None:
        self.mongo = mongo
        self.queries = queries
        self.node_service = node_service
        self.settings = settings
        self.clock = clock

    def _require_node(self, node_id: str) -> None:
        if self.mongo.get_node(node_id) is None:
            raise NotFoundError(f"node {node_id!r} not found")

    def as_of(self, req: AsOfRequest) -> AsOfResponse:
        t_start = time.perf_counter()
        if req.max_depth > self.settings.max_traversal_depth:
            raise InvalidRequestError(
                f"max_depth ({req.max_depth}) exceeds MAX_TRAVERSAL_DEPTH ({self.settings.max_traversal_depth})"
            )
        k = req.known_at or self.clock.now()
        v = req.valid_at
        dataset = getattr(req, "dataset", "default") or "default"

        if req.node_id is not None:
            self._require_node(req.node_id)
            raw = self.queries.as_of_subgraph(
                req.node_id,
                direction=req.direction,
                max_depth=req.max_depth,
                relationship_types=req.relationship_types,
                valid_at=v,
                known_at=k,
                dataset=dataset,
            )
            nodes_map = resolve_node_snapshots(self.mongo, raw["nodes"], v, k)
            root_active = req.node_id in nodes_map
            nodes_list = (
                [
                    AsOfNode(**nodes_map[nid])
                    for nid in sorted(nodes_map, key=lambda x: (nodes_map[x]["hops"] or 0, x))
                ]
                if (root_active and req.include_nodes)
                else []
            )
            if not root_active:
                all_edges: list[dict[str, Any]] = []
            else:
                all_edges = [
                    e for e in raw["edges"] if e["source"] in nodes_map and e["target"] in nodes_map
                ]
            total_edges = len(all_edges)
            paged_edges = all_edges[req.offset : req.offset + req.limit]
            return AsOfResponse(
                node_id=req.node_id,
                valid_at=v,
                known_at=k,
                node_active_at_time=root_active,
                nodes=nodes_list,
                edges=[AsOfEdge(**e) for e in paged_edges],
                total_nodes=len(nodes_map) if root_active else 0,
                total_edges=total_edges,
                limit=req.limit,
                offset=req.offset,
                neo4j_query_ms=raw["neo4j_query_ms"],
                execution_time_ms=round((time.perf_counter() - t_start) * 1000, 3),
            )

        raw = self.queries.as_of_graph(
            relationship_types=req.relationship_types,
            valid_at=v,
            known_at=k,
            include_nodes=req.include_nodes,
            limit=req.limit,
            offset=req.offset,
            dataset=dataset,
        )
        nodes_map = resolve_node_snapshots(self.mongo, raw["nodes"], v, k) if req.include_nodes else {}
        nodes_list = [AsOfNode(**nodes_map[nid]) for nid in sorted(nodes_map)]
        return AsOfResponse(
            node_id=None,
            valid_at=v,
            known_at=k,
            node_active_at_time=None,
            nodes=nodes_list,
            edges=[AsOfEdge(**e) for e in raw["edges"]],
            total_nodes=len(nodes_list),
            total_edges=raw["total_edges"],
            limit=req.limit,
            offset=req.offset,
            neo4j_query_ms=raw["neo4j_query_ms"],
            execution_time_ms=round((time.perf_counter() - t_start) * 1000, 3),
        )

    def _snapshot_edges(
        self,
        *,
        node_id: str | None,
        direction: Direction,
        max_depth: int,
        relationship_types: list[str] | None,
        valid_at: datetime,
        known_at: datetime,
        dataset: str = "default",
    ) -> tuple[dict[str, dict[str, Any]], float]:
        if node_id is not None:
            raw = self.queries.as_of_subgraph(
                node_id,
                direction=direction,
                max_depth=max_depth,
                relationship_types=relationship_types,
                valid_at=valid_at,
                known_at=known_at,
                dataset=dataset,
            )
        else:
            raw = self.queries.as_of_graph(
                relationship_types=relationship_types,
                valid_at=valid_at,
                known_at=known_at,
                include_nodes=False,
                limit=None,
                dataset=dataset,
            )
        return {e["dependency_id"]: e for e in raw["edges"]}, raw["neo4j_query_ms"]

    def compare(self, req: CompareRequest) -> CompareResponse:
        t_start = time.perf_counter()
        if req.max_depth > self.settings.max_traversal_depth:
            raise InvalidRequestError(
                f"max_depth ({req.max_depth}) exceeds MAX_TRAVERSAL_DEPTH ({self.settings.max_traversal_depth})"
            )
        if req.node_id is not None:
            self._require_node(req.node_id)

        dataset = getattr(req, "dataset", "default") or "default"
        default_k = req.known_at or self.clock.now()
        k_from = req.from_known_at or default_k
        k_to = req.to_known_at or default_k

        m1, ms1 = self._snapshot_edges(
            node_id=req.node_id,
            direction=req.direction,
            max_depth=req.max_depth,
            relationship_types=req.relationship_types,
            valid_at=req.from_valid_at,
            known_at=k_from,
            dataset=dataset,
        )
        m2, ms2 = self._snapshot_edges(
            node_id=req.node_id,
            direction=req.direction,
            max_depth=req.max_depth,
            relationship_types=req.relationship_types,
            valid_at=req.to_valid_at,
            known_at=k_to,
            dataset=dataset,
        )

        added = [AsOfEdge(**m2[did]) for did in sorted(m2) if did not in m1]
        removed = [AsOfEdge(**m1[did]) for did in sorted(m1) if did not in m2]
        modified: list[EdgeDiff] = []
        unchanged = 0

        for did in sorted(set(m1) & set(m2)):
            e1, e2 = m1[did], m2[did]
            changed_attrs: list[str] = []
            attr_changes: dict[str, Any] = {}
            if e1["weight"] != e2["weight"]:
                changed_attrs.append("weight")
                attr_changes["weight"] = {"before": e1["weight"], "after": e2["weight"]}
            if canonical_json(e1["metadata"]) != canonical_json(e2["metadata"]):
                changed_attrs.append("metadata")
                attr_changes["metadata"] = _diff_metadata(e1["metadata"], e2["metadata"])
            if changed_attrs:
                modified.append(
                    EdgeDiff(
                        dependency_id=did,
                        source=e2["source"],
                        target=e2["target"],
                        relationship=e2["relationship"],
                        before=AsOfEdge(**e1),
                        after=AsOfEdge(**e2),
                        changed_attributes=changed_attrs,
                        attribute_changes=attr_changes,
                    )
                )
            else:
                unchanged += 1

        return CompareResponse(
            from_valid_at=req.from_valid_at,
            to_valid_at=req.to_valid_at,
            from_known_at=k_from,
            to_known_at=k_to,
            node_id=req.node_id,
            summary=CompareSummary(
                from_edge_count=len(m1),
                to_edge_count=len(m2),
                added_count=len(added),
                removed_count=len(removed),
                modified_count=len(modified),
                unchanged_count=unchanged,
            ),
            added=added,
            removed=removed,
            modified=modified,
            neo4j_query_ms=round(ms1 + ms2, 3),
            execution_time_ms=round((time.perf_counter() - t_start) * 1000, 3),
        )

    def history(
        self,
        node_id: str,
        *,
        direction: Direction = "both",
        include_retracted: bool = True,
        include_superseded: bool = True,
    ) -> NodeTimelineResponse:
        node = self.node_service.get(node_id)
        node_hist = self.node_service.history(node_id)

        if direction == "outgoing":
            head_query: dict[str, Any] = {"source": node_id}
        elif direction == "incoming":
            head_query = {"target": node_id}
        else:
            head_query = {"$or": [{"source": node_id}, {"target": node_id}]}

        heads = list(self.mongo.heads.find(head_query).sort("_id", 1))
        dep_ids = [h["_id"] for h in heads]

        ver_query: dict[str, Any] = {"dependency_id": {"$in": dep_ids}}
        if not include_superseded:
            ver_query["tx_to"] = MAX_TIME
        versions = (
            [version_view(r) for r in self.mongo.versions.find(ver_query).sort([("tx_from", 1), ("valid_from", 1)])]
            if dep_ids
            else []
        )

        ev_query: dict[str, Any] = {"entity_type": "dependency", "entity_id": {"$in": dep_ids}}
        if not include_retracted:
            ev_query["retracted_at"] = None
        events = (
            [
                DependencyEventView(
                    event_id=e["_id"],
                    dependency_id=e["entity_id"],
                    type=e["type"],
                    kind=e["kind"],
                    valid_at=e.get("valid_at"),
                    tx_time=e["tx_time"],
                    seq=e["seq"],
                    fields=e.get("fields") or {},
                    retracted_at=e.get("retracted_at"),
                    retracted_by=e.get("retracted_by"),
                    cause=e.get("cause"),
                    client_event_id=e.get("client_event_id"),
                )
                for e in self.mongo.events.find(ev_query).sort([("tx_time", 1), ("seq", 1)])
            ]
            if dep_ids
            else []
        )

        return NodeTimelineResponse(
            node=node,
            node_history=node_hist,
            dependencies=[
                DependencySummary(
                    id=h["_id"],
                    source=h["source"],
                    target=h["target"],
                    relationship=h["relationship"],
                    created_at=h["created_at"],
                    updated_at=h["updated_at"],
                    rev=h["rev"],
                )
                for h in heads
            ],
            versions=versions,
            events=events,
        )
