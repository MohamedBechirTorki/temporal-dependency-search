"""Neo4j repository: graph projection used for traversal and analysis.

Graph model
-----------
(:Component {id, name, type, description, metadata_json, valid_from, valid_to,
             deleted, created_at, updated_at, rev})

(:Component)-[:DEPENDS_ON {version_id, dependency_id, relationship, weight,
                           metadata_json, valid_from, valid_to, tx_from, tx_to,
                           rev}]->(:Component)

One relationship per *bitemporal version row* (including superseded rows), so
current and historical traversals run inside Neo4j by filtering on the
temporal properties. The relationship type is fixed; the dependency kind is
the ``relationship`` property (keeps every query parameterised).

Idempotency: every projected element carries the source revision ``rev``;
updates are applied only if the incoming ``rev`` is newer.
"""
from __future__ import annotations

import logging
from typing import Any, Iterable

from neo4j import Driver

log = logging.getLogger(__name__)

SCHEMA_STATEMENTS = [
    "CREATE CONSTRAINT component_node_key IF NOT EXISTS FOR (c:Component) REQUIRE c.node_key IS UNIQUE",
    "CREATE INDEX component_id IF NOT EXISTS FOR (c:Component) ON (c.id)",
    "CREATE INDEX component_dataset IF NOT EXISTS FOR (c:Component) ON (c.dataset)",
    "CREATE INDEX component_type IF NOT EXISTS FOR (c:Component) ON (c.type)",
    "CREATE INDEX depends_on_dependency IF NOT EXISTS FOR ()-[r:DEPENDS_ON]-() ON (r.dependency_id)",
    "CREATE INDEX depends_on_relationship IF NOT EXISTS FOR ()-[r:DEPENDS_ON]-() ON (r.relationship)",
    "CREATE INDEX depends_on_valid IF NOT EXISTS FOR ()-[r:DEPENDS_ON]-() ON (r.valid_from, r.valid_to)",
    "CREATE INDEX depends_on_tx IF NOT EXISTS FOR ()-[r:DEPENDS_ON]-() ON (r.tx_from, r.tx_to)",
    "CREATE INDEX depends_on_dataset IF NOT EXISTS FOR ()-[r:DEPENDS_ON]-() ON (r.dataset)",
]
VERSION_UNIQUE = (
    "CREATE CONSTRAINT depends_on_version_key IF NOT EXISTS FOR ()-[r:DEPENDS_ON]-() REQUIRE r.version_key IS UNIQUE"
)
VERSION_INDEX_FALLBACK = "CREATE INDEX depends_on_version_key IF NOT EXISTS FOR ()-[r:DEPENDS_ON]-() ON (r.version_key)"

NODE_UPSERT = """
UNWIND $rows AS row
MERGE (c:Component {node_key: coalesce(row.node_key, (coalesce(row.dataset, 'default') + ':' + row.id))})
ON CREATE SET c.id = row.id, c.dataset = coalesce(row.dataset, 'default')
WITH c, row WHERE c.rev IS NULL OR c.rev < row.rev
SET c.id = row.id, c.dataset = coalesce(row.dataset, 'default'),
    c.name = row.name, c.type = row.type, c.description = row.description,
    c.metadata_json = row.metadata_json, c.valid_from = row.valid_from, c.valid_to = row.valid_to,
    c.deleted = row.deleted, c.created_at = row.created_at, c.updated_at = row.updated_at,
    c.rev = row.rev
"""

VERSION_UPSERT = """
UNWIND $rows AS row
MERGE (s:Component {node_key: coalesce(row.source_key, (coalesce(row.dataset, 'default') + ':' + row.source))})
ON CREATE SET s.id = row.source, s.dataset = coalesce(row.dataset, 'default')
MERGE (t:Component {node_key: coalesce(row.target_key, (coalesce(row.dataset, 'default') + ':' + row.target))})
ON CREATE SET t.id = row.target, t.dataset = coalesce(row.dataset, 'default')
MERGE (s)-[r:DEPENDS_ON {version_key: coalesce(row.version_key, (coalesce(row.dataset, 'default') + ':' + row.version_id))}]->(t)
ON CREATE SET r.version_id = row.version_id, r.dependency_id = row.props.dependency_id, r.dataset = coalesce(row.dataset, 'default')
WITH r, row WHERE r.rev IS NULL OR r.rev < row.rev
SET r += row.props, r.rev = row.rev, r.dataset = coalesce(row.dataset, 'default'), r.version_id = row.version_id
"""

PRUNE_VERSIONS = """
UNWIND $deps AS dep
MATCH ()-[r:DEPENDS_ON {dependency_id: dep.id}]->()
WHERE NOT r.version_id IN dep.version_ids
DELETE r
"""


def _chunks(rows: list[Any], size: int) -> Iterable[list[Any]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


class Neo4jRepository:
    def __init__(self, driver: Driver, database: str) -> None:
        self.driver = driver
        self.database = database

    def ensure_schema(self) -> None:
        with self.driver.session(database=self.database) as s:
            try:
                s.run("DROP CONSTRAINT component_id IF EXISTS").consume()
            except Exception:
                pass
            try:
                s.run("DROP CONSTRAINT depends_on_version IF EXISTS").consume()
            except Exception:
                pass
            # Backfill dataset/keys on legacy nodes if present
            try:
                s.run(
                    "MATCH (c:Component) WHERE c.dataset IS NULL "
                    "SET c.dataset = 'default', c.node_key = 'default:' + c.id"
                ).consume()
                s.run(
                    "MATCH ()-[r:DEPENDS_ON]->() WHERE r.dataset IS NULL "
                    "SET r.dataset = 'default', r.version_key = 'default:' + r.version_id"
                ).consume()
            except Exception:
                pass

            for stmt in SCHEMA_STATEMENTS:
                s.run(stmt).consume()
            try:
                s.run(VERSION_UNIQUE).consume()
            except Exception as exc:  # relationship uniqueness may be unavailable
                log.warning("relationship uniqueness constraint unavailable, using index",
                            extra={"error": type(exc).__name__})
                s.run(VERSION_INDEX_FALLBACK).consume()

    def delete_dataset(self, dataset: str) -> None:
        with self.driver.session(database=self.database) as s:
            if dataset == "default":
                s.run("MATCH (n:Component) WHERE n.dataset = 'default' OR n.dataset IS NULL DETACH DELETE n").consume()
                s.run("MATCH ()-[r:DEPENDS_ON]->() WHERE r.dataset = 'default' OR r.dataset IS NULL DELETE r").consume()
            else:
                s.run("MATCH (n:Component {dataset: $ds}) DETACH DELETE n", ds=dataset).consume()
                s.run("MATCH ()-[r:DEPENDS_ON {dataset: $ds}]->() DELETE r", ds=dataset).consume()

    def project(
        self, nodes: list[dict], versions: list[dict], prune: list[dict] | None = None, chunk: int = 2000
    ) -> None:
        """Upsert node/version state in a single write transaction.

        ``prune``: [{id, version_ids}] - delete projected versions of these
        dependencies that no longer exist in MongoDB (repair/rebuild only).
        """

        def work(tx):
            for part in _chunks(nodes, chunk):
                tx.run(NODE_UPSERT, rows=part).consume()
            for part in _chunks(versions, chunk):
                tx.run(VERSION_UPSERT, rows=part).consume()
            if prune:
                for part in _chunks(prune, chunk):
                    tx.run(PRUNE_VERSIONS, deps=part).consume()

        with self.driver.session(database=self.database) as s:
            s.execute_write(work)

    def wipe(self) -> None:
        with self.driver.session(database=self.database) as s:
            s.run("MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 5000 ROWS").consume()

    def delete_orphans(self, node_ids: list[str], dependency_ids: list[str]) -> None:
        with self.driver.session(database=self.database) as s:
            if dependency_ids:
                s.run(
                    "MATCH ()-[r:DEPENDS_ON]->() WHERE r.dependency_id IN $ids DELETE r", ids=dependency_ids
                ).consume()
            if node_ids:
                s.run("MATCH (c:Component) WHERE c.id IN $ids DETACH DELETE c", ids=node_ids).consume()

    # ---------------------------------------------------------- reconcile reads
    def node_revs(self) -> dict[str, int | None]:
        with self.driver.session(database=self.database) as s:
            return {r["id"]: r["rev"] for r in s.run("MATCH (c:Component) RETURN c.id AS id, c.rev AS rev")}

    def dependency_revs(self) -> dict[str, tuple[int | None, int | None, int]]:
        q = (
            "MATCH ()-[r:DEPENDS_ON]->() "
            "RETURN r.dependency_id AS id, min(r.rev) AS min_rev, max(r.rev) AS max_rev, count(r) AS n"
        )
        with self.driver.session(database=self.database) as s:
            return {r["id"]: (r["min_rev"], r["max_rev"], r["n"]) for r in s.run(q)}

    def versions_of(self, dependency_id: str) -> list[dict]:
        q = (
            "MATCH (s:Component)-[r:DEPENDS_ON {dependency_id: $id}]->(t:Component) "
            "RETURN s.id AS source, t.id AS target, properties(r) AS props ORDER BY r.version_id"
        )
        with self.driver.session(database=self.database) as s:
            return [r.data() for r in s.run(q, id=dependency_id)]

    def get_component(self, node_id: str) -> dict | None:
        with self.driver.session(database=self.database) as s:
            rec = s.run("MATCH (c:Component {id: $id}) RETURN properties(c) AS p", id=node_id).single()
            return rec["p"] if rec else None
