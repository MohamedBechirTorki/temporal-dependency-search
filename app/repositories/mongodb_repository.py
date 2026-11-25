"""MongoDB repository: authoritative storage (nodes, events, versions, outbox).

Collections
-----------
nodes                current node state (+ soft-delete / valid-time window)
node_history         append-only snapshot per node change (transaction-time history)
dependencies         one head document per logical dependency (identity, revision counters)
dependency_versions  append-only bitemporal version rows (never deleted)
events               append-only domain event log (nodes + dependencies)
outbox               synchronisation events for the Neo4j projection
dependency_keys      lock documents serialising writes per (source, target, relationship)
"""
from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable, TypeVar

from bson import ObjectId
from pymongo import ASCENDING, IndexModel, MongoClient, ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.database import Database
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

from app.domain.temporal import mongo_visibility_filter
from app.domain.time import MAX_TIME, utc_now

log = logging.getLogger(__name__)
T = TypeVar("T")

CLAIMABLE_STATUSES = ("pending", "processing")


class MongoRepository:
    def __init__(self, client: MongoClient, db: Database) -> None:
        self.client = client
        self.db = db
        self.nodes = db["nodes"]
        self.node_history = db["node_history"]
        self.heads = db["dependencies"]
        self.versions = db["dependency_versions"]
        self.events = db["events"]
        self.outbox = db["outbox"]
        self.keys = db["dependency_keys"]

    # ------------------------------------------------------------------ schema
    INDEXES: dict[str, list[IndexModel]] = {
        "nodes": [
            IndexModel([("type", ASCENDING)], name="type"),
            IndexModel([("deleted", ASCENDING)], name="deleted"),
        ],
        "node_history": [IndexModel([("node_id", ASCENDING), ("tx_time", ASCENDING)], name="node_tx")],
        "dependencies": [
            IndexModel([("source", ASCENDING)], name="source"),
            IndexModel([("target", ASCENDING)], name="target"),
            IndexModel([("relationship", ASCENDING)], name="relationship"),
            IndexModel([("source", ASCENDING), ("target", ASCENDING), ("relationship", ASCENDING)], name="triple"),
        ],
        "dependency_versions": [
            IndexModel([("dependency_id", ASCENDING), ("tx_to", ASCENDING)], name="dep_tx"),
            IndexModel(
                [("source", ASCENDING), ("tx_to", ASCENDING), ("valid_from", ASCENDING), ("valid_to", ASCENDING)],
                name="source_bitemporal",
            ),
            IndexModel(
                [("target", ASCENDING), ("tx_to", ASCENDING), ("valid_from", ASCENDING), ("valid_to", ASCENDING)],
                name="target_bitemporal",
            ),
            IndexModel(
                [("source", ASCENDING), ("target", ASCENDING), ("relationship", ASCENDING), ("tx_to", ASCENDING)],
                name="triple_tx",
            ),
            IndexModel([("relationship", ASCENDING)], name="relationship"),
            IndexModel([("valid_from", ASCENDING), ("valid_to", ASCENDING)], name="valid_time"),
            IndexModel([("tx_from", ASCENDING), ("tx_to", ASCENDING)], name="tx_time"),
        ],
        "events": [
            IndexModel([("entity_type", ASCENDING), ("entity_id", ASCENDING), ("seq", ASCENDING)], name="entity_seq"),
            IndexModel([("tx_time", ASCENDING)], name="tx_time"),
            IndexModel([("valid_at", ASCENDING)], name="valid_at"),
            IndexModel(
                [("client_event_id", ASCENDING)],
                name="client_event_id_unique",
                unique=True,
                partialFilterExpression={"client_event_id": {"$type": "string"}},
            ),
        ],
        "outbox": [
            IndexModel([("status", ASCENDING), ("next_attempt_at", ASCENDING)], name="status_next"),
            IndexModel([("lease_token", ASCENDING)], name="lease_token"),
            IndexModel([("aggregate_type", ASCENDING), ("aggregate_id", ASCENDING)], name="aggregate"),
        ],
        "dependency_keys": [],
    }

    def ensure_indexes(self) -> dict[str, float]:
        """Create collections and indexes; returns creation time (ms) per collection."""
        timings: dict[str, float] = {}
        existing = set(self.db.list_collection_names())
        for name, models in self.INDEXES.items():
            t0 = time.perf_counter()
            if name not in existing:
                self.db.create_collection(name)
            if models:
                self.db[name].create_indexes(models)
            timings[name] = round((time.perf_counter() - t0) * 1000, 3)
        return timings

    def drop_all(self) -> None:
        """Testing/benchmark helper: drop all engine collections."""
        for name in self.INDEXES:
            self.db.drop_collection(name)

    def delete_dataset(self, dataset: str) -> None:
        """Cleanly remove all nodes, dependencies, versions, events and outbox for a specific dataset."""
        if dataset == "default":
            query = {"$or": [{"dataset": "default"}, {"dataset": None}, {"dataset": {"$exists": False}}]}
        else:
            query = {"dataset": dataset}
        dep_ids = [d.get("dependency_id") or d["_id"] for d in self.heads.find(query, {"_id": 1, "dependency_id": 1})]
        self.heads.delete_many(query)
        self.versions.delete_many(query)
        if dep_ids:
            self.versions.delete_many({"dependency_id": {"$in": dep_ids}})
        self.nodes.delete_many(query)
        self.events.delete_many(query)
        self.outbox.delete_many(query)
        self.node_history.delete_many(query)

    # ------------------------------------------------------------- transactions
    def run_transaction(self, fn: Callable[[ClientSession], T]) -> T:
        """Run ``fn`` in a multi-document ACID transaction (retried on transient errors)."""
        with self.client.start_session() as session:
            return session.with_transaction(
                fn,
                read_concern=ReadConcern("snapshot"),
                write_concern=WriteConcern("majority"),
                read_preference=ReadPreference.PRIMARY,
            )

    def snapshot_session(self) -> ClientSession:
        """Session whose reads all observe one point-in-time snapshot."""
        return self.client.start_session(snapshot=True)

    # -------------------------------------------------------------------- nodes
    def get_node(
        self,
        node_id: str,
        session: ClientSession | None = None,
        dataset: str | None = None,
    ) -> dict | None:
        if isinstance(session, str) and dataset is None:
            dataset, session = session, None
        clauses: list[dict[str, Any]] = [{"_id": node_id}, {"id": node_id}]
        if dataset and dataset != "default":
            clauses.append({"_id": f"{dataset}:{node_id}"})
        q: dict[str, Any] = {"$or": clauses}
        if dataset:
            if dataset == "default":
                q["$and"] = [{"$or": [{"dataset": "default"}, {"dataset": None}, {"dataset": {"$exists": False}}]}]
            else:
                q["dataset"] = dataset
        return self.nodes.find_one(q, session=session)

    def get_nodes(
        self,
        ids: Iterable[str],
        session: ClientSession | None = None,
        dataset: str | None = None,
    ) -> dict[str, dict]:
        if isinstance(session, str) and dataset is None:
            dataset, session = session, None
        id_list = list(ids)
        keys = list(id_list)
        if dataset and dataset != "default":
            keys.extend([f"{dataset}:{nid}" for nid in id_list])
        clauses: list[dict[str, Any]] = [{"_id": {"$in": keys}}, {"id": {"$in": id_list}}]
        q: dict[str, Any] = {"$or": clauses}
        if dataset:
            if dataset == "default":
                q["$and"] = [{"$or": [{"dataset": "default"}, {"dataset": None}, {"dataset": {"$exists": False}}]}]
            else:
                q["dataset"] = dataset
        res: dict[str, dict] = {}
        for d in self.nodes.find(q, session=session):
            res[d.get("id") or d["_id"]] = d
        return res

    def insert_node(self, doc: dict, session: ClientSession) -> None:
        self.nodes.insert_one(doc, session=session)

    def update_node(self, node_id: str, expected_rev: int, changes: dict, session: ClientSession) -> dict | None:
        return self.nodes.find_one_and_update(
            {"_id": node_id, "rev": expected_rev},
            {"$set": changes, "$inc": {"rev": 1}},
            session=session,
            return_document=ReturnDocument.AFTER,
        )

    def touch_node(self, node_id: str, session: ClientSession) -> None:
        """Force a write on the node so concurrent node deletion conflicts (no write skew)."""
        self.nodes.update_one({"_id": node_id}, {"$set": {"_lock": ObjectId()}}, session=session)

    def list_nodes(self, query: dict, offset: int, limit: int) -> tuple[list[dict], int]:
        total = self.nodes.count_documents(query)
        items = list(self.nodes.find(query).sort("_id", ASCENDING).skip(offset).limit(limit))
        return items, total

    def insert_node_history(self, doc: dict, session: ClientSession) -> None:
        self.node_history.insert_one(doc, session=session)

    def node_history_for(self, node_id: str) -> list[dict]:
        return list(self.node_history.find({"node_id": node_id}).sort("tx_time", ASCENDING))

    # ------------------------------------------------------------- dependencies
    def get_head(self, dep_id: str, session: ClientSession | None = None) -> dict | None:
        return self.heads.find_one({"_id": dep_id}, session=session)

    def insert_head(self, doc: dict, session: ClientSession) -> None:
        self.heads.insert_one(doc, session=session)

    def update_head(self, dep_id: str, expected_event_seq: int, changes: dict, inc: dict, session: ClientSession) -> bool:
        res = self.heads.update_one(
            {"_id": dep_id, "event_seq": expected_event_seq}, {"$set": changes, "$inc": inc}, session=session
        )
        return res.modified_count == 1

    def heads_for_node(self, node_id: str, session: ClientSession | None = None) -> list[dict]:
        return list(self.heads.find({"$or": [{"source": node_id}, {"target": node_id}]}, session=session))

    def list_heads(self, query: dict, offset: int, limit: int) -> tuple[list[dict], int]:
        total = self.heads.count_documents(query)
        items = list(self.heads.find(query).sort("_id", ASCENDING).skip(offset).limit(limit))
        return items, total

    def lock_key(self, key: str, session: ClientSession) -> None:
        """Serialise concurrent writers of the same (source, target, relationship)."""
        self.keys.update_one({"_id": key}, {"$set": {"lock": ObjectId()}}, upsert=True, session=session)

    def load_events(self, entity_type: str, entity_id: str, session: ClientSession | None = None) -> list[dict]:
        return list(
            self.events.find({"entity_type": entity_type, "entity_id": entity_id}, session=session).sort(
                "seq", ASCENDING
            )
        )

    def insert_events(self, docs: list[dict], session: ClientSession) -> None:
        if docs:
            self.events.insert_many(docs, session=session)

    def retract_events(self, ids: list[str], tx: datetime, by: str, session: ClientSession) -> int:
        if not ids:
            return 0
        res = self.events.update_many(
            {"_id": {"$in": ids}, "retracted_at": None},
            {"$set": {"retracted_at": tx, "retracted_by": by}},
            session=session,
        )
        return res.modified_count

    def find_event_by_client_id(self, client_event_id: str, session: ClientSession | None = None) -> dict | None:
        return self.events.find_one({"client_event_id": client_event_id}, session=session)

    def current_versions(self, dep_id: str, session: ClientSession | None = None) -> list[dict]:
        return list(
            self.versions.find({"dependency_id": dep_id, "tx_to": MAX_TIME}, session=session).sort(
                "valid_from", ASCENDING
            )
        )

    def all_versions(self, dep_id: str, session: ClientSession | None = None) -> list[dict]:
        return list(
            self.versions.find({"dependency_id": dep_id}, session=session).sort(
                [("tx_from", ASCENDING), ("valid_from", ASCENDING)]
            )
        )

    def close_versions(self, ids: list[str], tx: datetime, cause: str, session: ClientSession) -> None:
        if ids:
            self.versions.update_many(
                {"_id": {"$in": ids}, "tx_to": MAX_TIME},
                {"$set": {"tx_to": tx, "closed_by_event": cause}},
                session=session,
            )

    def insert_versions(self, docs: list[dict], session: ClientSession) -> None:
        if docs:
            self.versions.insert_many(docs, session=session)

    def overlapping_same_triple(
        self, source: str, target: str, relationship: str, exclude_dep: str,
        intervals: list[tuple[datetime, datetime]], session: ClientSession,
        dataset: str | None = None,
    ) -> dict | None:
        if not intervals:
            return None
        q: dict = {
            "source": source, "target": target, "relationship": relationship,
            "tx_to": MAX_TIME, "dependency_id": {"$ne": exclude_dep},
            "$or": [{"valid_from": {"$lt": b}, "valid_to": {"$gt": a}} for a, b in intervals],
        }
        if dataset:
            q["dataset"] = dataset
        return self.versions.find_one(q, session=session)

    def visible_versions_for_node(
        self, node_id: str, direction: str, valid_at: datetime, known_at: datetime,
        relationship: str | None, offset: int, limit: int,
    ) -> tuple[list[dict], int]:
        if direction == "outgoing":
            endpoint: dict = {"source": node_id}
        elif direction == "incoming":
            endpoint = {"target": node_id}
        else:
            endpoint = {"$or": [{"source": node_id}, {"target": node_id}]}
        query = {**endpoint, **mongo_visibility_filter(valid_at, known_at)}
        if relationship:
            query["relationship"] = relationship
        total = self.versions.count_documents(query)
        items = list(self.versions.find(query).sort("dependency_id", ASCENDING).skip(offset).limit(limit))
        return items, total

    # ------------------------------------------------------------------- outbox
    @staticmethod
    def new_outbox_doc(aggregate_type: str, aggregate_id: str, rev: int, cause_event_id: str) -> dict:
        now = utc_now()
        return {
            "_id": ObjectId(),
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "rev": rev,
            "cause_event_id": cause_event_id,
            "status": "pending",
            "attempts": 0,
            "next_attempt_at": now,
            "created_at": now,
            "updated_at": now,
            "processed_at": None,
            "lease_token": None,
            "lease_until": None,
            "last_error": None,
            "last_error_type": None,
        }

    def enqueue(self, docs: list[dict], session: ClientSession | None = None) -> None:
        if docs:
            self.outbox.insert_many(docs, session=session)

    def claim_outbox(
        self, limit: int, lease_seconds: float, ids: list[ObjectId] | None = None
    ) -> tuple[str, list[dict]]:
        """Atomically lease up to ``limit`` claimable outbox docs (oldest first)."""
        now = utc_now()
        claimable = {
            "$or": [
                {"status": "pending", "next_attempt_at": {"$lte": now}},
                {"status": "processing", "lease_until": {"$lt": now}},
            ]
        }
        query: dict = claimable if ids is None else {"$and": [claimable, {"_id": {"$in": ids}}]}
        candidates = [d["_id"] for d in self.outbox.find(query, {"_id": 1}).sort("_id", ASCENDING).limit(limit)]
        token = uuid.uuid4().hex
        if not candidates:
            return token, []
        self.outbox.update_many(
            {"$and": [claimable, {"_id": {"$in": candidates}}]},
            {"$set": {
                "status": "processing", "lease_token": token,
                "lease_until": now + timedelta(seconds=lease_seconds), "updated_at": now,
            }},
        )
        return token, list(self.outbox.find({"lease_token": token}).sort("_id", ASCENDING))

    def mark_done(self, token: str, ids: list[ObjectId]) -> int:
        now = utc_now()
        res = self.outbox.update_many(
            {"_id": {"$in": ids}, "lease_token": token},
            {"$set": {"status": "done", "processed_at": now, "updated_at": now,
                      "lease_token": None, "lease_until": None}},
        )
        return res.modified_count

    def mark_failure(
        self, token: str, outbox_id: ObjectId, attempts: int, error: str, error_type: str,
        next_attempt_at: datetime, dead: bool,
    ) -> None:
        self.outbox.update_one(
            {"_id": outbox_id, "lease_token": token},
            {"$set": {
                "status": "failed" if dead else "pending", "attempts": attempts,
                "last_error": error, "last_error_type": error_type,
                "next_attempt_at": next_attempt_at, "updated_at": utc_now(),
                "lease_token": None, "lease_until": None,
            }},
        )

    def outbox_statuses(self, ids: list[ObjectId]) -> dict[ObjectId, str]:
        return {d["_id"]: d["status"] for d in self.outbox.find({"_id": {"$in": ids}}, {"status": 1})}

    def outbox_counts(self) -> dict[str, int]:
        counts = {"pending": 0, "processing": 0, "done": 0, "failed": 0}
        for row in self.outbox.aggregate([{"$group": {"_id": "$status", "n": {"$sum": 1}}}]):
            counts[row["_id"]] = row["n"]
        return counts

    def oldest_pending(self) -> dict | None:
        return self.outbox.find_one({"status": {"$in": list(CLAIMABLE_STATUSES)}}, sort=[("_id", ASCENDING)])

    def outbox_failures(self, limit: int = 50) -> list[dict]:
        return list(
            self.outbox.find({"$or": [{"status": "failed"}, {"attempts": {"$gt": 0}, "status": "pending"}]})
            .sort("_id", ASCENDING)
            .limit(limit)
        )

    def reset_failed(self) -> int:
        now = utc_now()
        res = self.outbox.update_many(
            {"status": "failed"},
            {"$set": {"status": "pending", "attempts": 0, "next_attempt_at": now, "updated_at": now}},
        )
        return res.modified_count

    # ------------------------------------------------------------ reconcile i/o
    def node_revs(self) -> dict[str, int]:
        return {d["_id"]: d["rev"] for d in self.nodes.find({}, {"rev": 1})}

    def dependency_revs(self) -> dict[str, tuple[int, int]]:
        """dep_id -> (rev, number of version rows)."""
        return {d["_id"]: (d["rev"], d["version_seq"]) for d in self.heads.find({}, {"rev": 1, "version_seq": 1})}

    def count_versions(self) -> int:
        return self.versions.estimated_document_count()
