"""Helpers shared by write services."""
from __future__ import annotations

import threading
import uuid
from datetime import datetime
from typing import Any, Callable, TypeVar

from pymongo.errors import DuplicateKeyError

from app.config import Settings
from app.domain.errors import ConflictError, InvalidRequestError
from app.domain.time import MAX_TIME, MIN_TIME, ONE_MS, as_utc, end_to_api, start_to_api, utc_now
from app.repositories.mongodb_repository import MongoRepository

T = TypeVar("T")


def new_event_id() -> str:
    return f"evt_{uuid.uuid4().hex}"


def event_doc(
    entity_type: str, entity_id: str, type_: str, valid_at: datetime | None, seq: int, tx: datetime,
    fields: dict[str, Any], kind: str = "change", cause: str | None = None,
    client_event_id: str | None = None, event_id: str | None = None,
) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "_id": event_id or new_event_id(),
        "entity_type": entity_type,
        "entity_id": entity_id,
        "type": type_,
        "kind": kind,
        "valid_at": valid_at,
        "tx_time": tx,
        "seq": seq,
        "fields": fields,
        "retracted_at": None,
        "retracted_by": None,
        "cause": cause,
    }
    if client_event_id:
        doc["client_event_id"] = client_event_id
    return doc


class TxClock:
    """Assigns transaction time.

    Wall-clock mode: max(now, floor + 1ms), i.e. strictly increasing per entity,
    while tracking a process-local high-water mark so immediate reads without
    an explicit timestamp observe rapid sub-millisecond writes.
    Override mode (dataset replay, ALLOW_TX_TIME_OVERRIDE=true): the given
    recorded_at must be >= the entity's last transaction time.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = threading.Lock()
        self._hwm: datetime | None = None

    def now(self) -> datetime:
        """Current wall-clock instant, not behind any wall-clock write assigned here."""
        cur = utc_now()
        with self._lock:
            if self._hwm is not None and cur < self._hwm:
                return self._hwm
            return cur

    def assign(self, recorded_at: datetime | None, floor: datetime | None) -> datetime:
        if recorded_at is not None:
            if not self.settings.allow_tx_time_override:
                raise InvalidRequestError(
                    "recorded_at (transaction-time override) is disabled; set ALLOW_TX_TIME_OVERRIDE=true for replay"
                )
            if floor is not None and recorded_at < floor:
                raise ConflictError(
                    "transaction time must be non-decreasing per entity",
                    details={"recorded_at": recorded_at.isoformat(), "last_recorded_at": floor.isoformat()},
                )
            return recorded_at
        now = utc_now()
        tx = floor + ONE_MS if (floor is not None and now <= floor) else now
        with self._lock:
            if self._hwm is None or tx > self._hwm:
                self._hwm = tx
        return tx


def find_duplicate(
    repo: MongoRepository, client_event_id: str | None, entity_type: str, entity_id: str | None, session=None
) -> dict | None:
    """Return the earlier event recorded with this idempotency key, or None."""
    if not client_event_id:
        return None
    ev = repo.find_event_by_client_id(client_event_id, session=session)
    if ev is None:
        return None
    if ev["entity_type"] != entity_type or (entity_id is not None and ev["entity_id"] != entity_id):
        raise ConflictError(
            "client_event_id already used for a different entity",
            details={"client_event_id": client_event_id, "entity": f'{ev["entity_type"]}:{ev["entity_id"]}'},
        )
    return ev


def run_idempotent(
    repo: MongoRepository, fn: Callable[[Any], T], client_event_id: str | None,
    entity_type: str, entity_id: str | None, on_duplicate: Callable[[dict], T],
) -> T:
    """Run a write transaction; a concurrent duplicate key resolves to the first write."""
    try:
        return repo.run_transaction(fn)
    except DuplicateKeyError:
        dup = find_duplicate(repo, client_event_id, entity_type, entity_id)
        if dup is None:
            raise
        return on_duplicate(dup)


def resolve_node_snapshots(
    repo: MongoRepository, raw_nodes: dict[str, dict[str, Any]], valid_at: datetime, known_at: datetime
) -> dict[str, dict[str, Any]]:
    """Resolve bitemporal node state at (valid_at, known_at) using MongoDB node_history when needed."""
    if not raw_nodes:
        return {}
    need_history = [
        nid for nid, n in raw_nodes.items()
        if n.get("updated_at") is None or n["updated_at"] > known_at
    ]
    snapshots: dict[str, dict[str, Any]] = {}
    if need_history:
        cursor = (
            repo.node_history
            .find({"node_id": {"$in": need_history}, "tx_time": {"$lte": known_at}})
            .sort([("node_id", 1), ("rev", -1)])
        )
        for doc in cursor:
            nid = doc["node_id"]
            if nid not in snapshots:
                snapshots[nid] = doc["snapshot"]

    resolved: dict[str, dict[str, Any]] = {}
    for nid, n in raw_nodes.items():
        if n.get("created_at") is not None and n["created_at"] > known_at:
            continue
        if nid in need_history:
            snap = snapshots.get(nid)
            if snap is None:
                continue
            vf = as_utc(snap["valid_from"]) if snap.get("valid_from") else MIN_TIME
            vt = as_utc(snap["valid_to"]) if snap.get("valid_to") else MAX_TIME
            if not (vf <= valid_at < vt):
                continue
            resolved[nid] = {
                "id": nid,
                "name": snap["name"],
                "type": snap["type"],
                "description": snap.get("description"),
                "metadata": snap.get("metadata") or {},
                "valid_from": start_to_api(vf),
                "valid_to": end_to_api(vt),
                "deleted": bool(snap.get("deleted", False)),
                "hops": n.get("hops"),
            }
        else:
            vf = n["valid_from"] or MIN_TIME
            vt = n["valid_to"] or MAX_TIME
            if not (vf <= valid_at < vt):
                continue
            resolved[nid] = {
                "id": nid,
                "name": n["name"],
                "type": n["type"],
                "description": n.get("description"),
                "metadata": n.get("metadata") or {},
                "valid_from": n["valid_from"],
                "valid_to": n["valid_to"],
                "deleted": n["deleted"],
                "hops": n.get("hops"),
            }
    return resolved
