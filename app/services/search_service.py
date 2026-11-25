"""Graph search service orchestrating Neo4j traversal, shortest path, reachability, and cycle checks."""
from __future__ import annotations

from datetime import datetime

from app.api.schemas.search import (
    CycleCheckRequest,
    CycleCheckResponse,
    DirectSearchRequest,
    DirectSearchResponse,
    MultiHopSearchRequest,
    MultiHopSearchResponse,
    ReachabilityRequest,
    ReachabilityResponse,
    ShortestPathRequest,
    ShortestPathResponse,
)
from app.config import Settings
from app.domain.errors import InvalidRequestError, NotFoundError
from app.graph.cycle_detection import CycleDetector
from app.graph.shortest_path import ShortestPathFinder
from app.graph.traversal import GraphTraversal
from app.repositories.mongodb_repository import MongoRepository
from app.services.common import TxClock


class SearchService:
    def __init__(
        self,
        mongo: MongoRepository,
        traversal: GraphTraversal,
        shortest: ShortestPathFinder,
        cycles: CycleDetector,
        settings: Settings,
        clock: TxClock,
    ) -> None:
        self.mongo = mongo
        self.traversal = traversal
        self.shortest = shortest
        self.cycles = cycles
        self.settings = settings
        self.clock = clock

    def _resolve_times(self, valid_at: datetime | None, known_at: datetime | None) -> tuple[datetime, datetime]:
        now = self.clock.now()
        return valid_at or now, known_at or now

    def _check_depth(self, max_depth: int | None) -> None:
        if max_depth is not None and max_depth > self.settings.max_traversal_depth:
            raise InvalidRequestError(
                f"max_depth ({max_depth}) exceeds configured MAX_TRAVERSAL_DEPTH ({self.settings.max_traversal_depth})",
                details={"max_depth": max_depth, "limit": self.settings.max_traversal_depth},
            )

    def _require_nodes(self, *node_ids: str, dataset: str = "default") -> None:
        unique = list(dict.fromkeys(node_ids))
        found = self.traversal.nodes_exist(unique, dataset=dataset)
        missing = [nid for nid in unique if nid not in found]
        if not missing:
            return
        # Distinguish "not yet synced" vs "does not exist in MongoDB"
        in_mongo = self.mongo.get_nodes(missing)
        truly_missing = [nid for nid in missing if nid not in in_mongo]
        if truly_missing:
            raise NotFoundError(
                f"node(s) not found: {', '.join(truly_missing)}",
                details={"missing_nodes": truly_missing},
            )

    def direct(self, req: DirectSearchRequest) -> DirectSearchResponse:
        dataset = getattr(req, "dataset", "default") or "default"
        self._require_nodes(req.node_id, dataset=dataset)
        v, k = self._resolve_times(req.valid_at, req.known_at)
        res = self.traversal.direct(
            req.node_id,
            direction=req.direction,
            relationship_types=req.relationship_types,
            valid_at=v,
            known_at=k,
            offset=req.offset,
            limit=req.limit,
            dataset=dataset,
        )
        return DirectSearchResponse(
            node_id=req.node_id,
            direction=req.direction,
            valid_at=v,
            known_at=k,
            limit=req.limit,
            offset=req.offset,
            **res,
        )

    def multi_hop(self, req: MultiHopSearchRequest) -> MultiHopSearchResponse:
        self._check_depth(req.max_depth)
        dataset = getattr(req, "dataset", "default") or "default"
        self._require_nodes(req.node_id, dataset=dataset)
        v, k = self._resolve_times(req.valid_at, req.known_at)
        res = self.traversal.multi_hop(
            req.node_id,
            direction=req.direction,
            max_depth=req.max_depth,
            relationship_types=req.relationship_types,
            valid_at=v,
            known_at=k,
            include_paths=req.include_paths,
            offset=req.offset,
            limit=req.limit,
            dataset=dataset,
        )
        return MultiHopSearchResponse(
            node_id=req.node_id,
            direction=req.direction,
            max_depth=req.max_depth,
            valid_at=v,
            known_at=k,
            limit=req.limit,
            offset=req.offset,
            **res,
        )

    def shortest_path(self, req: ShortestPathRequest) -> ShortestPathResponse:
        self._check_depth(req.max_depth)
        dataset = getattr(req, "dataset", "default") or "default"
        self._require_nodes(req.source_id, req.target_id, dataset=dataset)
        v, k = self._resolve_times(req.valid_at, req.known_at)
        res = self.shortest.find(
            req.source_id,
            req.target_id,
            direction=req.direction,
            weighted=req.weighted,
            max_depth=req.max_depth,
            relationship_types=req.relationship_types,
            valid_at=v,
            known_at=k,
            dataset=dataset,
        )
        return ShortestPathResponse(
            source_id=req.source_id,
            target_id=req.target_id,
            direction=req.direction,
            valid_at=v,
            known_at=k,
            **res,
        )

    def reachability(self, req: ReachabilityRequest) -> ReachabilityResponse:
        self._check_depth(req.max_depth)
        dataset = getattr(req, "dataset", "default") or "default"
        self._require_nodes(req.source_id, req.target_id, dataset=dataset)
        v, k = self._resolve_times(req.valid_at, req.known_at)
        res = self.traversal.reachability(
            req.source_id,
            req.target_id,
            direction=req.direction,
            max_depth=req.max_depth,
            relationship_types=req.relationship_types,
            valid_at=v,
            known_at=k,
            include_path=req.include_path,
            dataset=dataset,
        )
        return ReachabilityResponse(
            source_id=req.source_id,
            target_id=req.target_id,
            direction=req.direction,
            valid_at=v,
            known_at=k,
            **res,
        )

    def check_cycle(self, req: CycleCheckRequest) -> CycleCheckResponse:
        self._check_depth(req.max_depth)
        dataset = getattr(req, "dataset", "default") or "default"
        self._require_nodes(req.source_id, req.target_id, dataset=dataset)
        v, k = self._resolve_times(req.valid_at, req.known_at)
        res = self.cycles.would_create_cycle(
            req.source_id,
            req.target_id,
            valid_at=v,
            known_at=k,
            relationship_types=req.relationship_types,
            max_depth=req.max_depth,
            dataset=dataset,
        )
        return CycleCheckResponse(
            source_id=req.source_id,
            target_id=req.target_id,
            valid_at=v,
            known_at=k,
            **res,
        )
