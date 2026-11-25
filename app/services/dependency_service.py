"""Dependency write/read service implementing the bitemporal model.

See app/domain/temporal.py for the semantics. Every write, inside one
MongoDB transaction:
1. appends events (events are never rewritten; corrections only retract),
2. recomputes the valid-time timeline from all currently known events,
3. diffs it against the current version rows: closes rows in transaction
   time and inserts new rows (rows are never deleted),
4. enqueues an outbox entry for the Neo4j projection.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from bson import ObjectId
from pymongo.client_session import ClientSession

from app.api.schemas.common import Page, SyncInfo, WriteMeta
from app.api.schemas.dependencies import (
    DependencyCorrection, DependencyCreate, DependencyEventView, DependencyModify, DependencyRestore,
    DependencySummary, DependencyVersion, DependencyView, DependencyWriteResult,
)
from app.config import CyclePolicy, Settings
from app.domain.errors import (
    ConflictError, CycleDetectedError, DuplicateDependencyError, InvalidRequestError, NotFoundError,
)
from app.domain.temporal import EVENT_TYPES, DepEvent, diff_rows, endpoint_window, is_visible, row_key, timeline
from app.domain.time import MAX_TIME, as_utc, end_to_api
from app.graph.cycle_detection import CycleDetector
from app.ingestion.synchronization import SyncService
from app.repositories.mongodb_repository import MongoRepository
from app.services.common import TxClock, event_doc, find_duplicate, new_event_id, run_idempotent

log = logging.getLogger(__name__)


@dataclass
class ApplyResult:
    dependency_id: str
    tx: datetime
    event_ids: list[str] = field(default_factory=list)
    outbox_ids: list[ObjectId] = field(default_factory=list)
    changed: bool = False
    duplicate: bool = False
    cycle_path: list[str] | None = None


def triple_key(source: str, target: str, relationship: str) -> str:
    return f"{source}|{target}|{relationship}"


def to_dep_event(doc: dict) -> DepEvent:
    return DepEvent(
        event_id=doc["_id"], type=doc["type"], valid_at=as_utc(doc["valid_at"]), seq=doc["seq"],
        tx_time=as_utc(doc["tx_time"]), fields=doc.get("fields") or {},
        retracted_at=as_utc(doc["retracted_at"]) if doc.get("retracted_at") else None,
    )


def version_view(row: dict) -> DependencyVersion:
    return DependencyVersion(
        version_id=row["_id"], dependency_id=row["dependency_id"], source=row["source"], target=row["target"],
        relationship=row["relationship"], weight=row["weight"], metadata=row.get("metadata") or {},
        valid_from=as_utc(row["valid_from"]), valid_to=end_to_api(row["valid_to"]),
        recorded_at=as_utc(row["tx_from"]), superseded_at=end_to_api(row["tx_to"]),
    )


class DependencyService:
    def __init__(self, repo: MongoRepository, sync: SyncService, settings: Settings,
                 clock: TxClock | None = None, cycles: CycleDetector | None = None) -> None:
        self.repo = repo
        self.sync = sync
        self.settings = settings
        self.clock = clock or TxClock(settings)
        self.cycles = cycles

    # ------------------------------------------------------------------ reads
    def build_view(
        self, head: dict, rows: list[dict], valid_at: datetime | None = None,
        known_at: datetime | None = None, include_history: bool = False,
    ) -> DependencyView:
        now = self.clock.now()
        v = valid_at or now
        k = known_at or max(now, as_utc(head["last_tx"]))
        known = sorted(
            (r for r in rows if as_utc(r["tx_from"]) <= k < as_utc(r["tx_to"])),
            key=lambda r: r["valid_from"],
        )
        current = next(
            (r for r in known if is_visible(as_utc(r["valid_from"]), as_utc(r["valid_to"]),
                                            as_utc(r["tx_from"]), as_utc(r["tx_to"]), v, k)),
            None,
        )
        if current is not None:
            status = "active"
        elif any(as_utc(r["valid_from"]) > v for r in known):
            status = "scheduled"
        else:
            status = "inactive"
        return DependencyView(
            id=head["_id"], source=head["source"], target=head["target"], relationship=head["relationship"],
            status=status, current=version_view(current) if current else None,
            timeline=[version_view(r) for r in known],
            history=[version_view(r) for r in rows] if include_history else None,
            valid_at=v, known_at=k, created_at=as_utc(head["created_at"]),
            updated_at=as_utc(head["updated_at"]), rev=head["rev"],
        )

    def get(self, dep_id: str, valid_at: datetime | None = None, known_at: datetime | None = None,
            include_history: bool = False) -> DependencyView:
        head = self.repo.get_head(dep_id)
        if head is None:
            raise NotFoundError(f"dependency {dep_id!r} not found")
        return self.build_view(head, self.repo.all_versions(dep_id), valid_at, known_at, include_history)

    def list(self, source: str | None, target: str | None, relationship: str | None,
             offset: int, limit: int) -> Page[DependencySummary]:
        q: dict[str, Any] = {}
        if source:
            q["source"] = source
        if target:
            q["target"] = target
        if relationship:
            q["relationship"] = relationship
        items, total = self.repo.list_heads(q, offset, limit)
        return Page[DependencySummary](
            items=[DependencySummary(id=h["_id"], source=h["source"], target=h["target"],
                                     relationship=h["relationship"], created_at=h["created_at"],
                                     updated_at=h["updated_at"], rev=h["rev"]) for h in items],
            total=total, limit=limit, offset=offset,
        )

    def events(self, dep_id: str) -> list[DependencyEventView]:
        if self.repo.get_head(dep_id) is None:
            raise NotFoundError(f"dependency {dep_id!r} not found")
        return [
            DependencyEventView(
                event_id=e["_id"], dependency_id=e["entity_id"], type=e["type"], kind=e["kind"],
                valid_at=e.get("valid_at"), tx_time=e["tx_time"], seq=e["seq"], fields=e.get("fields") or {},
                retracted_at=e.get("retracted_at"), retracted_by=e.get("retracted_by"), cause=e.get("cause"),
                client_event_id=e.get("client_event_id"),
            )
            for e in self.repo.load_events("dependency", dep_id)
        ]

    def records_for_node(self, node_id: str, direction: str, valid_at: datetime | None,
                         known_at: datetime | None, relationship: str | None,
                         offset: int, limit: int) -> Page[DependencyVersion]:
        if self.repo.get_node(node_id) is None:
            raise NotFoundError(f"node {node_id!r} not found")
        now = self.clock.now()
        rows, total = self.repo.visible_versions_for_node(
            node_id, direction, valid_at or now, known_at or now, relationship, offset, limit
        )
        return Page[DependencyVersion](items=[version_view(r) for r in rows], total=total,
                                       limit=limit, offset=offset)

    # ------------------------------------------------------------------- core
    def apply_in_session(
        self, s: ClientSession, head: dict, tx: datetime, new_events: list[dict], *,
        retract_ids: list[str] | None = None, retracted_by: str | None = None,
        cause_event_id: str, makes_valid: bool = False,
    ) -> ApplyResult:
        """Append events/retractions and re-materialise version rows (inside a txn)."""
        dep_id = head["_id"]
        retract_ids = retract_ids or []
        existing = self.repo.load_events("dependency", dep_id, s)
        by_id = {e["_id"]: e for e in existing}
        for rid in retract_ids:
            ev = by_id.get(rid)
            if ev is None or ev["type"] not in EVENT_TYPES:
                raise InvalidRequestError(f"event {rid!r} is not a retractable event of dependency {dep_id!r}")
            if ev.get("retracted_at") is not None:
                raise ConflictError(f"event {rid!r} is already retracted")
        retract_set = set(retract_ids)

        nodes = self.repo.get_nodes([head["source"], head["target"]], s)
        src, tgt = nodes[head["source"]], nodes[head["target"]]
        window = endpoint_window(as_utc(src["valid_from"]), as_utc(src["valid_to"]),
                                 as_utc(tgt["valid_from"]), as_utc(tgt["valid_to"]))
        if makes_valid:
            # serialise against concurrent node deletion and same-triple writers
            self.repo.touch_node(head["source"], s)
            if head["target"] != head["source"]:
                self.repo.touch_node(head["target"], s)
            self.repo.lock_key(triple_key(head["source"], head["target"], head["relationship"]), s)

        known = [
            to_dep_event(d) for d in existing + new_events
            if d["type"] in EVENT_TYPES and d.get("retracted_at") is None and d["_id"] not in retract_set
        ]
        segments = timeline(known, window)
        current = self.repo.current_versions(dep_id, s)
        close, insert = diff_rows(
            [(r["_id"], row_key(as_utc(r["valid_from"]), as_utc(r["valid_to"]), r["weight"], r.get("metadata")))
             for r in current],
            segments,
        )

        if insert:
            clash = self.repo.overlapping_same_triple(
                head["source"], head["target"], head["relationship"], dep_id,
                [(seg.valid_from, seg.valid_to) for seg in insert], s,
            )
            if clash:
                raise DuplicateDependencyError(
                    "an equivalent dependency (same source, target, relationship) is valid in an overlapping period",
                    details={"conflicting_dependency_id": clash["dependency_id"],
                             "conflicting_version_id": clash["_id"]},
                )

        self.repo.insert_events(new_events, s)
        if retract_set:
            self.repo.retract_events(list(retract_set), tx, retracted_by or cause_event_id, s)

        version_docs = []
        for i, seg in enumerate(insert, start=1):
            version_docs.append({
                "_id": f'{dep_id}#v{head["version_seq"] + i}',
                "dependency_id": dep_id, "source": head["source"], "target": head["target"],
                "relationship": head["relationship"], "weight": float(seg.weight), "metadata": seg.metadata,
                "valid_from": seg.valid_from, "valid_to": seg.valid_to,
                "tx_from": tx, "tx_to": MAX_TIME, "created_by_event": cause_event_id,
            })
        self.repo.close_versions(close, tx, cause_event_id, s)
        self.repo.insert_versions(version_docs, s)

        changed = bool(close or version_docs)
        new_rev = head["rev"] + (1 if changed else 0)
        ok = self.repo.update_head(
            dep_id, head["event_seq"], {"last_tx": tx, "updated_at": tx},
            {"event_seq": len(new_events), "version_seq": len(version_docs), "rev": 1 if changed else 0}, s,
        )
        if not ok:
            raise ConflictError(f"concurrent modification of dependency {dep_id!r}; retry")

        outbox_ids: list[ObjectId] = []
        if changed:
            doc = self.repo.new_outbox_doc("dependency", dep_id, new_rev, cause_event_id)
            self.repo.enqueue([doc], s)
            outbox_ids.append(doc["_id"])
        return ApplyResult(dep_id, tx, [e["_id"] for e in new_events], outbox_ids, changed)

    # -------------------------------------------------------------- finishing
    def _duplicate_result(self, ev: dict) -> ApplyResult:
        return ApplyResult(ev["entity_id"], as_utc(ev["tx_time"]), [ev["_id"]], [], False, duplicate=True)

    def _finish(self, res: ApplyResult) -> DependencyWriteResult:
        sync_status = self.sync.process_ids(res.outbox_ids) if res.outbox_ids else "none"
        head = self.repo.get_head(res.dependency_id)
        view = self.build_view(head, self.repo.all_versions(res.dependency_id))
        return DependencyWriteResult(
            dependency=view,
            meta=WriteMeta(
                event_ids=res.event_ids, tx_time=res.tx, duplicate=res.duplicate, changed=res.changed,
                sync=SyncInfo(status=sync_status, outbox_ids=[str(i) for i in res.outbox_ids]),
                cycle_detected=bool(res.cycle_path), cycle_path=res.cycle_path,
            ),
        )

    def _enforce_cycle_policy(
        self, source: str, target: str, valid_at: datetime, known_at: datetime
    ) -> list[str] | None:
        policy = self.settings.cycle_policy
        if policy == CyclePolicy.ALLOW or self.cycles is None:
            return None
        if policy == CyclePolicy.REJECT and self.repo.oldest_pending() is not None:
            self.sync.drain()
        res = self.cycles.would_create_cycle(
            source, target, valid_at=valid_at, known_at=known_at, max_depth=self.settings.max_traversal_depth
        )
        if not res["creates_cycle"]:
            return None
        cycle_nodes = res["cycle_nodes"]
        if policy == CyclePolicy.REJECT:
            raise CycleDetectedError(
                f"dependency {source!r} -> {target!r} creates a cycle",
                details={"cycle_nodes": cycle_nodes, "cycle_length": res["cycle_length"]},
            )
        log.warning("dependency creates a cycle", extra={"source": source, "target": target, "cycle": cycle_nodes})
        return cycle_nodes

    def _load_head(self, dep_id: str, s: ClientSession) -> dict:
        head = self.repo.get_head(dep_id, s)
        if head is None:
            raise NotFoundError(f"dependency {dep_id!r} not found")
        return head

    def _check_write_cycle(
        self, source: str, target: str, valid_at: datetime | None, recorded_at: datetime | None
    ) -> list[str] | None:
        if self.settings.cycle_policy == CyclePolicy.ALLOW or self.cycles is None:
            return None
        now = self.clock.now()
        k = recorded_at or now
        v = valid_at or k
        return self._enforce_cycle_policy(source, target, v, k)

    # ----------------------------------------------------------------- writes
    def create(self, req: DependencyCreate) -> DependencyWriteResult:
        dep_id = req.id or f"dep_{uuid.uuid4().hex}"
        cycle_path: list[str] | None = None
        if self.settings.cycle_policy != CyclePolicy.ALLOW and self.cycles is not None:
            if not find_duplicate(self.repo, req.client_event_id, "dependency", dep_id):
                nodes = self.repo.get_nodes([req.source, req.target])
                if req.source in nodes and req.target in nodes:
                    cycle_path = self._check_write_cycle(req.source, req.target, req.valid_from, req.recorded_at)

        def txn(s: ClientSession) -> ApplyResult:
            dup = find_duplicate(self.repo, req.client_event_id, "dependency", dep_id, s)
            if dup:
                return self._duplicate_result(dup)
            if self.repo.get_head(dep_id, s):
                raise ConflictError(f"dependency {dep_id!r} already exists")
            nodes = self.repo.get_nodes([req.source, req.target], s)
            missing = [n for n in (req.source, req.target) if n not in nodes]
            if missing:
                raise InvalidRequestError("unknown node(s)", details={"missing_nodes": missing})
            tx = self.clock.assign(req.recorded_at, None)
            valid_from = req.valid_from or tx
            if req.valid_to and req.valid_to <= valid_from:
                raise InvalidRequestError("valid_to must be after valid_from")
            head = {
                "_id": dep_id, "source": req.source, "target": req.target, "relationship": req.relationship,
                "rev": 0, "event_seq": 0, "version_seq": 0, "created_at": tx, "updated_at": tx, "last_tx": tx,
            }
            self.repo.insert_head(head, s)
            created = event_doc("dependency", dep_id, "created", valid_from, 1, tx,
                                {"weight": req.weight, "metadata": req.metadata},
                                client_event_id=req.client_event_id)
            events = [created]
            if req.valid_to:
                events.append(event_doc("dependency", dep_id, "removed", req.valid_to, 2, tx, {},
                                        cause="created_with_valid_to"))
            res = self.apply_in_session(s, head, tx, events, cause_event_id=created["_id"], makes_valid=True)
            if not res.changed:
                raise ConflictError(
                    "dependency would never be valid: requested interval lies outside the validity of its endpoints",
                    details={"source": req.source, "target": req.target},
                )
            return res

        res = run_idempotent(self.repo, txn, req.client_event_id, "dependency", dep_id, self._duplicate_result)
        if not res.duplicate and cycle_path:
            res.cycle_path = cycle_path
        return self._finish(res)

    def _single_event_write(self, dep_id: str, type_: str, effective_from: datetime | None,
                            fields: dict[str, Any], kind: str, client_event_id: str | None,
                            recorded_at: datetime | None, makes_valid: bool) -> DependencyWriteResult:
        def txn(s: ClientSession) -> ApplyResult:
            dup = find_duplicate(self.repo, client_event_id, "dependency", dep_id, s)
            if dup:
                return self._duplicate_result(dup)
            head = self._load_head(dep_id, s)
            tx = self.clock.assign(recorded_at, as_utc(head["last_tx"]))
            ev = event_doc("dependency", dep_id, type_, effective_from or tx, head["event_seq"] + 1, tx,
                           fields, kind=kind, client_event_id=client_event_id)
            return self.apply_in_session(s, head, tx, [ev], cause_event_id=ev["_id"], makes_valid=makes_valid)

        res = run_idempotent(self.repo, txn, client_event_id, "dependency", dep_id, self._duplicate_result)
        return self._finish(res)

    def modify(self, dep_id: str, req: DependencyModify) -> DependencyWriteResult:
        fields: dict[str, Any] = {}
        if req.weight is not None:
            fields["weight"] = req.weight
        if req.metadata is not None:
            fields["metadata"] = req.metadata
        return self._single_event_write(dep_id, "modified", req.effective_from, fields, req.kind,
                                        req.client_event_id, req.recorded_at, makes_valid=False)

    def remove(self, dep_id: str, effective_from: datetime | None, kind: str = "change",
               client_event_id: str | None = None, recorded_at: datetime | None = None) -> DependencyWriteResult:
        return self._single_event_write(dep_id, "removed", effective_from, {}, kind,
                                        client_event_id, recorded_at, makes_valid=False)

    def restore(self, dep_id: str, req: DependencyRestore) -> DependencyWriteResult:
        cycle_path: list[str] | None = None
        if self.settings.cycle_policy != CyclePolicy.ALLOW and self.cycles is not None:
            if not find_duplicate(self.repo, req.client_event_id, "dependency", dep_id):
                head = self.repo.get_head(dep_id)
                if head is not None:
                    cycle_path = self._check_write_cycle(
                        head["source"], head["target"], req.effective_from, req.recorded_at
                    )
        fields: dict[str, Any] = {}
        if req.weight is not None:
            fields["weight"] = req.weight
        if req.metadata is not None:
            fields["metadata"] = req.metadata
        res = self._single_event_write(dep_id, "restored", req.effective_from, fields, req.kind,
                                       req.client_event_id, req.recorded_at, makes_valid=True)
        if not res.meta.duplicate and cycle_path:
            res.meta.cycle_detected = True
            res.meta.cycle_path = cycle_path
        return res

    def correct(self, dep_id: str, req: DependencyCorrection) -> DependencyWriteResult:
        cycle_path: list[str] | None = None
        if self.settings.cycle_policy != CyclePolicy.ALLOW and self.cycles is not None:
            if not find_duplicate(self.repo, req.client_event_id, "dependency", dep_id):
                head = self.repo.get_head(dep_id)
                if head is not None:
                    for e in req.add_events:
                        if e.type in ("created", "restored"):
                            cp = self._check_write_cycle(head["source"], head["target"], e.valid_at, req.recorded_at)
                            if cp:
                                cycle_path = cp
                                break

        def txn(s: ClientSession) -> ApplyResult:
            dup = find_duplicate(self.repo, req.client_event_id, "dependency", dep_id, s)
            if dup:
                return self._duplicate_result(dup)
            head = self._load_head(dep_id, s)
            tx = self.clock.assign(req.recorded_at, as_utc(head["last_tx"]))
            audit_id = new_event_id()
            seq = head["event_seq"]
            added = []
            for i, e in enumerate(req.add_events, start=2):
                fields: dict[str, Any] = {}
                if e.weight is not None:
                    fields["weight"] = e.weight
                if e.metadata is not None:
                    fields["metadata"] = e.metadata
                added.append(event_doc("dependency", dep_id, e.type, e.valid_at, seq + i, tx, fields,
                                       kind="correction", cause=audit_id))
            audit = event_doc(
                "dependency", dep_id, "corrected", None, seq + 1, tx,
                {"retracted": req.retract_event_ids, "added": [a["_id"] for a in added], "reason": req.reason},
                kind="correction", client_event_id=req.client_event_id, event_id=audit_id,
            )
            makes_valid = any(e.type in ("created", "restored") for e in req.add_events) or bool(req.retract_event_ids)
            return self.apply_in_session(s, head, tx, [audit, *added], retract_ids=req.retract_event_ids,
                                         retracted_by=audit_id, cause_event_id=audit_id, makes_valid=makes_valid)

        res = run_idempotent(self.repo, txn, req.client_event_id, "dependency", dep_id, self._duplicate_result)
        if not res.duplicate and cycle_path:
            res.cycle_path = cycle_path
        return self._finish(res)
