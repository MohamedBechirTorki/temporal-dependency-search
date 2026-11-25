"""Node service.

Node attributes are versioned in transaction time (node_history snapshots).
Node existence has a valid-time window [valid_from, valid_to). Deleting a node
is a soft delete that sets valid_to; every dependency touching the node is
re-materialised and clipped to the endpoint window, so history before the
deletion is preserved.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from bson import ObjectId
from pymongo.client_session import ClientSession

from app.api.schemas.common import Page, SyncInfo, WriteMeta
from app.api.schemas.nodes import NodeCreate, NodeHistoryEntry, NodeUpdate, NodeView, NodeWriteResult
from app.config import Settings
from app.domain.errors import ConflictError, InvalidRequestError, NotFoundError
from app.domain.time import MAX_TIME, MIN_TIME, as_utc, end_to_api, start_to_api
from app.ingestion.synchronization import SyncService
from app.repositories.mongodb_repository import MongoRepository
from app.services.common import TxClock, event_doc, find_duplicate, run_idempotent
from app.services.dependency_service import DependencyService

log = logging.getLogger(__name__)

SNAPSHOT_FIELDS = ("name", "type", "description", "metadata", "valid_from", "valid_to", "deleted")


def node_view(doc: dict) -> NodeView:
    return NodeView(
        id=doc["_id"],
        name=doc["name"],
        type=doc["type"],
        description=doc.get("description"),
        metadata=doc.get("metadata") or {},
        valid_from=start_to_api(doc["valid_from"]),
        valid_to=end_to_api(doc["valid_to"]),
        deleted=doc["deleted"],
        created_at=as_utc(doc["created_at"]),
        updated_at=as_utc(doc["updated_at"]),
        rev=doc["rev"],
    )


class _NodeResult:
    def __init__(self, node_id: str, tx: datetime, event_ids: list[str], outbox_ids: list[ObjectId],
                 duplicate: bool = False, affected: list[str] | None = None) -> None:
        self.node_id = node_id
        self.tx = tx
        self.event_ids = event_ids
        self.outbox_ids = outbox_ids
        self.duplicate = duplicate
        self.affected = affected or []


class NodeService:
    def __init__(self, repo: MongoRepository, sync: SyncService, deps: DependencyService,
                 settings: Settings, clock: TxClock | None = None) -> None:
        self.repo = repo
        self.sync = sync
        self.deps = deps
        self.settings = settings
        self.clock = clock or deps.clock

    # ------------------------------------------------------------------ reads
    def get(self, node_id: str) -> NodeView:
        doc = self.repo.get_node(node_id)
        if doc is None:
            raise NotFoundError(f"node {node_id!r} not found")
        return node_view(doc)

    def list(self, type_: str | None, include_deleted: bool, offset: int, limit: int) -> Page[NodeView]:
        q: dict[str, Any] = {}
        if type_:
            q["type"] = type_
        if not include_deleted:
            q["deleted"] = False
        items, total = self.repo.list_nodes(q, offset, limit)
        return Page[NodeView](items=[node_view(d) for d in items], total=total, limit=limit, offset=offset)

    def history(self, node_id: str) -> list[NodeHistoryEntry]:
        if self.repo.get_node(node_id) is None:
            raise NotFoundError(f"node {node_id!r} not found")
        out = []
        for h in self.repo.node_history_for(node_id):
            snap = dict(h["snapshot"])
            snap["valid_from"] = start_to_api(snap.get("valid_from"))
            snap["valid_to"] = end_to_api(snap.get("valid_to"))
            out.append(NodeHistoryEntry(node_id=h["node_id"], rev=h["rev"], operation=h["operation"],
                                        tx_time=h["tx_time"], event_id=h["event_id"], snapshot=snap))
        return out

    # ---------------------------------------------------------------- helpers
    def _history(self, doc: dict, operation: str, tx: datetime, event_id: str, s: ClientSession) -> None:
        self.repo.insert_node_history({
            "node_id": doc["_id"], "rev": doc["rev"], "operation": operation, "tx_time": tx,
            "event_id": event_id, "snapshot": {k: doc.get(k) for k in SNAPSHOT_FIELDS},
        }, s)

    def _duplicate(self, ev: dict) -> _NodeResult:
        return _NodeResult(ev["entity_id"], as_utc(ev["tx_time"]), [ev["_id"]], [], duplicate=True)

    def _finish(self, res: _NodeResult) -> NodeWriteResult:
        status = self.sync.process_ids(res.outbox_ids) if res.outbox_ids else "none"
        return NodeWriteResult(
            node=node_view(self.repo.get_node(res.node_id)),
            meta=WriteMeta(event_ids=res.event_ids, tx_time=res.tx, duplicate=res.duplicate,
                           changed=not res.duplicate,
                           sync=SyncInfo(status=status, outbox_ids=[str(i) for i in res.outbox_ids])),
            affected_dependencies=res.affected,
        )

    # ----------------------------------------------------------------- writes
    def create(self, req: NodeCreate) -> NodeWriteResult:
        def txn(s: ClientSession) -> _NodeResult:
            dup = find_duplicate(self.repo, req.client_event_id, "node", req.id, s)
            if dup:
                return self._duplicate(dup)
            existing = self.repo.get_node(req.id, s)
            if existing:
                state = "deleted" if existing["deleted"] else "existing"
                raise ConflictError(f"node {req.id!r} already exists ({state})")
            tx = self.clock.assign(req.recorded_at, None)
            doc = {
                "_id": req.id, "name": req.name or req.id, "type": req.type, "description": req.description,
                "metadata": req.metadata, "valid_from": req.valid_from or MIN_TIME, "valid_to": MAX_TIME,
                "deleted": False, "created_at": tx, "updated_at": tx, "rev": 1, "event_seq": 1,
            }
            self.repo.insert_node(doc, s)
            ev = event_doc("node", req.id, "created", req.valid_from, 1, tx,
                           {k: doc[k] for k in ("name", "type", "description", "metadata")},
                           client_event_id=req.client_event_id)
            self.repo.insert_events([ev], s)
            self._history(doc, "created", tx, ev["_id"], s)
            out = self.repo.new_outbox_doc("node", req.id, 1, ev["_id"])
            self.repo.enqueue([out], s)
            return _NodeResult(req.id, tx, [ev["_id"]], [out["_id"]])

        return self._finish(run_idempotent(self.repo, txn, req.client_event_id, "node", req.id, self._duplicate))

    def update(self, node_id: str, req: NodeUpdate) -> NodeWriteResult:
        changes = req.model_dump(exclude_unset=True, exclude={"client_event_id", "recorded_at"})
        for f in ("name", "type", "metadata"):
            if f in changes and changes[f] is None:
                raise InvalidRequestError(f"{f} cannot be null")
        if not changes:
            raise InvalidRequestError("no fields to update")

        def txn(s: ClientSession) -> _NodeResult:
            dup = find_duplicate(self.repo, req.client_event_id, "node", node_id, s)
            if dup:
                return self._duplicate(dup)
            doc = self.repo.get_node(node_id, s)
            if doc is None:
                raise NotFoundError(f"node {node_id!r} not found")
            if doc["deleted"]:
                raise ConflictError(f"node {node_id!r} is deleted")
            tx = self.clock.assign(req.recorded_at, as_utc(doc["updated_at"]))
            updated = self.repo.update_node(
                node_id, doc["rev"], {**changes, "updated_at": tx, "event_seq": doc["event_seq"] + 1}, s
            )
            if updated is None:
                raise ConflictError(f"concurrent modification of node {node_id!r}; retry")
            ev = event_doc("node", node_id, "updated", None, doc["event_seq"] + 1, tx, changes,
                           client_event_id=req.client_event_id)
            self.repo.insert_events([ev], s)
            self._history(updated, "updated", tx, ev["_id"], s)
            out = self.repo.new_outbox_doc("node", node_id, updated["rev"], ev["_id"])
            self.repo.enqueue([out], s)
            return _NodeResult(node_id, tx, [ev["_id"]], [out["_id"]])

        return self._finish(run_idempotent(self.repo, txn, req.client_event_id, "node", node_id, self._duplicate))

    def delete(self, node_id: str, effective_from: datetime | None = None, client_event_id: str | None = None,
               recorded_at: datetime | None = None) -> NodeWriteResult:
        def txn(s: ClientSession) -> _NodeResult:
            dup = find_duplicate(self.repo, client_event_id, "node", node_id, s)
            if dup:
                return self._duplicate(dup)
            doc = self.repo.get_node(node_id, s)
            if doc is None:
                raise NotFoundError(f"node {node_id!r} not found")
            if doc["deleted"]:
                raise ConflictError(f"node {node_id!r} is already deleted")
            heads = self.repo.heads_for_node(node_id, s)
            floor = max([as_utc(doc["updated_at"])] + [as_utc(h["last_tx"]) for h in heads])
            tx = self.clock.assign(recorded_at, floor)
            t = effective_from or tx
            if t <= as_utc(doc["valid_from"]):
                raise InvalidRequestError("effective_from must be after the node's valid_from")
            updated = self.repo.update_node(
                node_id, doc["rev"],
                {"valid_to": t, "deleted": True, "updated_at": tx, "event_seq": doc["event_seq"] + 1}, s,
            )
            if updated is None:
                raise ConflictError(f"concurrent modification of node {node_id!r}; retry")
            ev = event_doc("node", node_id, "deleted", t, doc["event_seq"] + 1, tx, {},
                           client_event_id=client_event_id)
            self.repo.insert_events([ev], s)
            self._history(updated, "deleted", tx, ev["_id"], s)
            out = self.repo.new_outbox_doc("node", node_id, updated["rev"], ev["_id"])
            self.repo.enqueue([out], s)
            outbox_ids = [out["_id"]]
            affected: list[str] = []
            for head in heads:
                r = self.deps.apply_in_session(s, head, tx, [], cause_event_id=ev["_id"])
                if r.changed:
                    affected.append(head["_id"])
                    outbox_ids.extend(r.outbox_ids)
            return _NodeResult(node_id, tx, [ev["_id"]], outbox_ids, affected=affected)

        return self._finish(run_idempotent(self.repo, txn, client_event_id, "node", node_id, self._duplicate))
