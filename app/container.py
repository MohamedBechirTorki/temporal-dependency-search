"""Service container wired at application startup."""
from __future__ import annotations

import logging
from dataclasses import dataclass

from app.config import Settings
from app.db import Databases
from app.graph.cycle_detection import CycleDetector
from app.graph.shortest_path import ShortestPathFinder
from app.graph.temporal_queries import TemporalGraphQueries
from app.graph.traversal import GraphTraversal
from app.ingestion.importer import BatchImporter
from app.ingestion.synchronization import SyncService
from app.repositories.mongodb_repository import MongoRepository
from app.repositories.neo4j_repository import Neo4jRepository
from app.services.analytics_service import AnalyticsService
from app.services.common import TxClock
from app.services.dependency_service import DependencyService
from app.services.node_service import NodeService
from app.services.search_service import SearchService
from app.services.temporal_service import TemporalService

log = logging.getLogger(__name__)


@dataclass
class Container:
    settings: Settings
    dbs: Databases
    mongo: MongoRepository
    neo: Neo4jRepository
    sync: SyncService
    clock: TxClock
    traversal: GraphTraversal
    shortest: ShortestPathFinder
    cycles: CycleDetector
    temporal_queries: TemporalGraphQueries
    dependencies: DependencyService
    nodes: NodeService
    importer: BatchImporter
    search: SearchService
    temporal: TemporalService
    analytics: AnalyticsService

    @classmethod
    def build(cls, settings: Settings) -> "Container":
        dbs = Databases(settings)
        mongo = MongoRepository(dbs.mongo_client, dbs.mongo)
        neo = Neo4jRepository(dbs.neo4j_driver, dbs.neo4j_database)
        sync = SyncService(mongo, neo, settings)
        clock = TxClock(settings)
        traversal = GraphTraversal(dbs.neo4j_driver, dbs.neo4j_database)
        shortest = ShortestPathFinder(dbs.neo4j_driver, dbs.neo4j_database)
        cycles = CycleDetector(dbs.neo4j_driver, dbs.neo4j_database)
        temporal_queries = TemporalGraphQueries(dbs.neo4j_driver, dbs.neo4j_database)
        deps = DependencyService(mongo, sync, settings, clock=clock, cycles=cycles)
        nodes = NodeService(mongo, sync, deps, settings, clock=clock)
        importer = BatchImporter(mongo, sync, settings, clock=clock)
        search = SearchService(mongo, traversal, shortest, cycles, settings, clock)
        temporal = TemporalService(mongo, temporal_queries, nodes, settings, clock)
        analytics = AnalyticsService(mongo, traversal, temporal_queries, settings, clock)
        return cls(
            settings, dbs, mongo, neo, sync, clock, traversal, shortest, cycles,
            temporal_queries, deps, nodes, importer, search, temporal, analytics,
        )

    def init_storage(self) -> None:
        """Best-effort schema setup; the app starts even if a database is down."""
        try:
            timings = self.mongo.ensure_indexes()
            log.info("mongodb indexes ready", extra={"timings_ms": timings})
        except Exception as exc:
            log.error("mongodb index setup failed", extra={"error": type(exc).__name__})
        if self.sync.ensure_schema():
            log.info("neo4j schema ready")

    def close(self) -> None:
        self.sync.stop_worker()
        self.dbs.close()
