"""Efficient bulk & file ingestion into MongoDB + outbox (and optional Neo4j sync).

Designed so single-item API semantics (bitemporal timelines, node-window
clipping, append-only versions, outbox events) are preserved while chunking
MongoDB transactions and Neo4j UNWIND batches for 100k+ node scalability.
"""
from __future__ import annotations

import csv
import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

from bson import ObjectId
from pymongo import UpdateMany, UpdateOne
from pymongo.client_session import ClientSession

from app.api.schemas.sync import ProcessReport
from app.config import Settings
from app.domain.errors import ConflictError, DuplicateDependencyError, InvalidRequestError
from app.domain.temporal import (
    EVENT_TYPES, DepEvent, diff_rows, endpoint_window, row_key, timeline,
)
from app.domain.time import MAX_TIME, MIN_TIME, ONE_MS, as_utc
from app.ingestion.synchronization import SyncService
from app.ingestion.validator import parse_optional_utc, validate_batch
from app.repositories.mongodb_repository import MongoRepository
from app.services.common import TxClock, event_doc, new_event_id

log = logging.getLogger(__name__)


@dataclass
class IngestMetrics:
    nodes_ingested: int = 0
    dependencies_ingested: int = 0
    events_ingested: int = 0
    versions_created: int = 0
    versions_closed: int = 0
    mongo_ingest_ms: float = 0.0
    sync_ms: float = 0.0
    total_ms: float = 0.0
    sync_report: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _iter_chunks(items: Iterable[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    buf: list[dict[str, Any]] = []
    for item in items:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def iter_jsonl(path: str | Path) -> Iterator[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def iter_csv(path: str | Path) -> Iterator[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            parsed: dict[str, Any] = {}
            for k, v in row.items():
                if v == "" or v is None:
                    parsed[k] = None
                elif k == "metadata":
                    parsed[k] = json.loads(v)
                elif k == "weight":
                    parsed[k] = float(v)
                elif k in ("retract_event_ids", "add_events"):
                    parsed[k] = json.loads(v)
                else:
                    parsed[k] = v
            yield parsed


def load_records(path: str | Path) -> Iterator[dict[str, Any]]:
    p = Path(path)
    if p.suffix.lower() == ".csv":
        return iter_csv(p)
    return iter_jsonl(p)


class BatchImporter:
    """Chunked transactional importer into MongoDB with optional Neo4j drain."""

    def __init__(
        self, repo: MongoRepository, sync: SyncService, settings: Settings, clock: TxClock | None = None
    ) -> None:
        self.repo = repo
        self.sync = sync
        self.settings = settings
        self.clock = clock or TxClock(settings)

    def _resolve_tx(self, raw_recorded_at: Any, floor: datetime | None = None) -> datetime:
        dt = parse_optional_utc(raw_recorded_at)
        return self.clock.assign(dt, floor)

    # ------------------------------------------------------------------ nodes
    def ingest_nodes(
        self,
        nodes: Iterable[dict[str, Any]],
        *,
        chunk_size: int = 1000,
        validate: bool = True,
        sync: bool = False,
        ignore_existing: bool = False,
        dataset: str = "default",
    ) -> IngestMetrics:
        t_start = time.perf_counter()
        metrics = IngestMetrics()
        t_mongo = 0.0

        for chunk in _iter_chunks(nodes, chunk_size):
            if validate:
                existing = set(self.repo.get_nodes([n["id"] for n in chunk if "id" in n], dataset=dataset).keys())
                rep = validate_batch(chunk, [], existing_node_ids=existing)
                if not rep.valid:
                    first = rep.errors[0]
                    if first.code in ("node_already_exists", "duplicate_node_in_batch"):
                        raise ConflictError(first.message, details={"errors": [e.__dict__ for e in rep.errors[:20]]})
                    raise InvalidRequestError(first.message, details={"errors": [e.__dict__ for e in rep.errors[:20]]})

            t0 = time.perf_counter()

            def txn(s: ClientSession) -> int:
                node_docs: list[dict[str, Any]] = []
                hist_docs: list[dict[str, Any]] = []
                ev_docs: list[dict[str, Any]] = []
                out_docs: list[dict[str, Any]] = []

                effective_chunk = chunk
                existing_in_txn = self.repo.get_nodes([n["id"] for n in chunk], dataset=dataset, session=s)
                if existing_in_txn:
                    if ignore_existing:
                        effective_chunk = [n for n in chunk if n["id"] not in existing_in_txn]
                        if not effective_chunk:
                            return 0
                    else:
                        dup_id = next(iter(existing_in_txn))
                        raise ConflictError(f"node {dup_id!r} already exists")

                for n in effective_chunk:
                    nid = n["id"]
                    tx = self._resolve_tx(n.get("recorded_at"))
                    vf = parse_optional_utc(n.get("valid_from")) or MIN_TIME
                    vt = parse_optional_utc(n.get("valid_to")) or MAX_TIME
                    meta = dict(n.get("metadata") or {})
                    ds = n.get("dataset") or dataset
                    doc_id = f"{ds}:{nid}" if ds != "default" else nid
                    doc = {
                        "_id": doc_id,
                        "id": nid,
                        "dataset": ds,
                        "name": n.get("name") or nid,
                        "type": n.get("type") or "component",
                        "description": n.get("description"),
                        "metadata": meta,
                        "valid_from": vf,
                        "valid_to": vt,
                        "deleted": False,
                        "created_at": tx,
                        "updated_at": tx,
                        "rev": 1,
                        "event_seq": 1,
                    }
                    ev = event_doc(
                        "node", nid, "created", parse_optional_utc(n.get("valid_from")), 1, tx,
                        {"name": doc["name"], "type": doc["type"], "description": doc["description"], "metadata": meta},
                        client_event_id=n.get("client_event_id"),
                    )
                    ev["dataset"] = ds
                    hist = {
                        "node_id": nid, "rev": 1, "operation": "created", "tx_time": tx, "event_id": ev["_id"],
                        "dataset": ds,
                        "snapshot": {k: doc[k] for k in ("name", "type", "description", "metadata",
                                                         "valid_from", "valid_to", "deleted")},
                    }
                    node_docs.append(doc)
                    ev_docs.append(ev)
                    hist_docs.append(hist)
                    out_docs.append(self.repo.new_outbox_doc("node", doc_id, 1, ev["_id"]))

                self.repo.nodes.insert_many(node_docs, session=s)
                self.repo.insert_events(ev_docs, s)
                self.repo.node_history.insert_many(hist_docs, session=s)
                self.repo.enqueue(out_docs, s)
                return len(node_docs)

            n_written = self.repo.run_transaction(txn)
            t_mongo += time.perf_counter() - t0
            metrics.nodes_ingested += n_written
            metrics.events_ingested += n_written

        metrics.mongo_ingest_ms = round(t_mongo * 1000, 3)
        if sync and metrics.nodes_ingested:
            t1 = time.perf_counter()
            rep = self.sync.drain()
            metrics.sync_ms = round((time.perf_counter() - t1) * 1000, 3)
            metrics.sync_report = rep.model_dump()
        metrics.total_ms = round((time.perf_counter() - t_start) * 1000, 3)
        return metrics

    # ----------------------------------------------------------- dependencies
    def ingest_dependencies(
        self,
        dependencies: Iterable[dict[str, Any]],
        *,
        chunk_size: int = 1000,
        validate: bool = True,
        sync: bool = False,
        ignore_existing: bool = False,
        dataset: str = "default",
    ) -> IngestMetrics:
        t_start = time.perf_counter()
        metrics = IngestMetrics()
        t_mongo = 0.0

        for chunk in _iter_chunks(dependencies, chunk_size):
            if validate:
                endpoints = {d["source"] for d in chunk if "source" in d} | {
                    d["target"] for d in chunk if "target" in d
                }
                existing_nodes = set(self.repo.get_nodes(endpoints, dataset=dataset).keys())
                dep_ids = [d["id"] for d in chunk if d.get("id")]
                existing_deps = {
                    h["_id"] for h in self.repo.heads.find({"_id": {"$in": dep_ids}}, {"_id": 1})
                } if dep_ids else set()
                rep = validate_batch([], chunk, existing_node_ids=existing_nodes, existing_dep_ids=existing_deps)
                if not rep.valid:
                    first = rep.errors[0]
                    if first.code in ("duplicate_dependency_id", "overlapping_triple", "outside_node_validity"):
                        raise ConflictError(first.message, details={"errors": [e.__dict__ for e in rep.errors[:20]]})
                    raise InvalidRequestError(first.message, details={"errors": [e.__dict__ for e in rep.errors[:20]]})

            t0 = time.perf_counter()

            def txn(s: ClientSession) -> tuple[int, int, int]:
                endpoints = {d["source"] for d in chunk} | {d["target"] for d in chunk}
                nodes_map = self.repo.get_nodes(endpoints, dataset=dataset, session=s)
                missing = sorted(endpoints - set(nodes_map.keys()))
                if missing:
                    raise InvalidRequestError("unknown node(s)", details={"missing_nodes": missing[:50]})

                dep_ids = [d["id"] for d in chunk if d.get("id")]
                if dep_ids:
                    existing_h = self.repo.heads.find_one({"_id": {"$in": dep_ids}}, {"_id": 1}, session=s)
                    if existing_h:
                        raise ConflictError(f"dependency {existing_h['_id']!r} already exists")

                head_docs: list[dict[str, Any]] = []
                ev_docs: list[dict[str, Any]] = []
                ver_docs: list[dict[str, Any]] = []
                out_docs: list[dict[str, Any]] = []
                chunk_triples: dict[tuple[str, str, str], list[tuple[datetime, datetime, str]]] = {}

                for d in chunk:
                    did = d.get("id") or f"dep_{uuid.uuid4().hex}"
                    src_id, tgt_id = d["source"], d["target"]
                    rel = d.get("relationship") or d.get("type") or "depends_on"
                    weight = float(d.get("weight", 1.0) if d.get("weight") is not None else 1.0)
                    meta = dict(d.get("metadata") or {})
                    tx = self._resolve_tx(d.get("recorded_at"))
                    vf = parse_optional_utc(d.get("valid_from")) or tx
                    vt = parse_optional_utc(d.get("valid_to"))
                    if vt is not None and vt <= vf:
                        raise InvalidRequestError(f"dependency {did!r}: valid_to must be after valid_from")

                    src, tgt = nodes_map[src_id], nodes_map[tgt_id]
                    window = endpoint_window(
                        as_utc(src["valid_from"]), as_utc(src["valid_to"]),
                        as_utc(tgt["valid_from"]), as_utc(tgt["valid_to"]),
                    )
                    created = event_doc(
                        "dependency", did, "created", vf, 1, tx, {"weight": weight, "metadata": meta},
                        client_event_id=d.get("client_event_id"),
                    )
                    dep_events = [created]
                    if vt is not None:
                        dep_events.append(
                            event_doc("dependency", did, "removed", vt, 2, tx, {}, cause="created_with_valid_to")
                        )

                    known = [
                        DepEvent(
                            event_id=e["_id"], type=e["type"], valid_at=e["valid_at"], seq=e["seq"],
                            tx_time=e["tx_time"], fields=e["fields"],
                        )
                        for e in dep_events
                    ]
                    segments = timeline(known, window)
                    if not segments:
                        raise ConflictError(
                            f"dependency {did!r} lies outside endpoint node validity",
                            details={"dependency_id": did, "source": src_id, "target": tgt_id},
                        )

                    # Check overlap against already persisted dependencies and earlier items in this chunk
                    triple = (src_id, tgt_id, rel)
                    for p_from, p_to, p_did in chunk_triples.get(triple, ()):
                        if any(seg.valid_from < p_to and seg.valid_to > p_from for seg in segments):
                            raise DuplicateDependencyError(
                                "an equivalent dependency is valid in an overlapping period",
                                details={"dependency_id": did, "conflicting_dependency_id": p_did},
                            )
                    ds = d.get("dataset") or dataset
                    clash = self.repo.overlapping_same_triple(
                        src_id, tgt_id, rel, did, [(seg.valid_from, seg.valid_to) for seg in segments], s,
                        dataset=ds,
                    )
                    if clash:
                        if ignore_existing:
                            continue
                        raise DuplicateDependencyError(
                            "an equivalent dependency is valid in an overlapping period",
                            details={"dependency_id": did, "conflicting_dependency_id": clash["dependency_id"]},
                        )
                    for seg in segments:
                        chunk_triples.setdefault(triple, []).append((seg.valid_from, seg.valid_to, did))

                    head_id = f"{ds}:{did}" if ds != "default" else did
                    tx_to = parse_optional_utc(d.get("recorded_to") or d.get("tx_to")) or MAX_TIME
                    for i, seg in enumerate(segments, start=1):
                        ver_id = f"{ds}:{did}#v{i}" if ds != "default" else f"{did}#v{i}"
                        ver_docs.append({
                            "_id": ver_id,
                            "version_id": f"{did}#v{i}",
                            "dependency_id": did,
                            "dataset": ds,
                            "source": src_id,
                            "target": tgt_id,
                            "relationship": rel,
                            "weight": float(seg.weight),
                            "metadata": seg.metadata,
                            "valid_from": seg.valid_from,
                            "valid_to": seg.valid_to,
                            "tx_from": tx,
                            "tx_to": tx_to,
                            "created_by_event": created["_id"],
                        })

                    head_docs.append({
                        "_id": head_id,
                        "id": did,
                        "dependency_id": did,
                        "dataset": ds,
                        "source": src_id,
                        "target": tgt_id,
                        "relationship": rel,
                        "rev": 1,
                        "event_seq": len(dep_events),
                        "version_seq": len(segments),
                        "created_at": tx,
                        "updated_at": tx,
                        "last_tx": tx,
                    })
                    for e in dep_events:
                        e["dataset"] = ds
                    ev_docs.extend(dep_events)
                    out_docs.append(self.repo.new_outbox_doc("dependency", head_id, 1, created["_id"]))

                self.repo.heads.insert_many(head_docs, session=s)
                self.repo.insert_events(ev_docs, s)
                self.repo.insert_versions(ver_docs, s)
                self.repo.enqueue(out_docs, s)
                return len(head_docs), len(ev_docs), len(ver_docs)

            n_deps, n_evs, n_vers = self.repo.run_transaction(txn)
            t_mongo += time.perf_counter() - t0
            metrics.dependencies_ingested += n_deps
            metrics.events_ingested += n_evs
            metrics.versions_created += n_vers

        metrics.mongo_ingest_ms = round(t_mongo * 1000, 3)
        if sync and metrics.dependencies_ingested:
            t1 = time.perf_counter()
            rep = self.sync.drain()
            metrics.sync_ms = round((time.perf_counter() - t1) * 1000, 3)
            metrics.sync_report = rep.model_dump()
        metrics.total_ms = round((time.perf_counter() - t_start) * 1000, 3)
        return metrics

    # -------------------------------------------------- temporal event replay
    def ingest_dependency_events(
        self,
        events: Iterable[dict[str, Any]],
        *,
        chunk_size: int = 500,
        sync: bool = False,
    ) -> IngestMetrics:
        """Apply a stream of temporal dependency events (modified/removed/restored/corrected).

        Events touching distinct dependencies are batched into one MongoDB
        transaction per chunk; if the same dependency appears again, the current
        chunk flushes first so per-dependency ordering is exact.
        """
        t_start = time.perf_counter()
        metrics = IngestMetrics()
        t_mongo = 0.0

        buf: list[dict[str, Any]] = []
        seen_deps: set[str] = set()

        def flush(batch: list[dict[str, Any]]) -> None:
            nonlocal t_mongo
            if not batch:
                return
            t0 = time.perf_counter()

            def txn(s: ClientSession) -> tuple[int, int, int]:
                dep_ids = [b["dependency_id"] for b in batch]
                heads = {h["_id"]: h for h in self.repo.heads.find({"_id": {"$in": dep_ids}}, session=s)}
                missing_deps = [d for d in dep_ids if d not in heads]
                if missing_deps:
                    raise InvalidRequestError("unknown dependency_id(s)", details={"missing": missing_deps[:20]})

                node_ids = {h["source"] for h in heads.values()} | {h["target"] for h in heads.values()}
                nodes_map = self.repo.get_nodes(node_ids, s)
                existing_events_by_dep: dict[str, list[dict]] = {d: [] for d in dep_ids}
                for ev in self.repo.events.find({"entity_type": "dependency", "entity_id": {"$in": dep_ids}},
                                                session=s).sort("seq", 1):
                    existing_events_by_dep[ev["entity_id"]].append(ev)

                current_vers_by_dep: dict[str, list[dict]] = {d: [] for d in dep_ids}
                for row in self.repo.versions.find({"dependency_id": {"$in": dep_ids}, "tx_to": MAX_TIME},
                                                  session=s).sort("valid_from", 1):
                    current_vers_by_dep[row["dependency_id"]].append(row)

                new_ev_docs: list[dict[str, Any]] = []
                new_ver_docs: list[dict[str, Any]] = []
                out_docs: list[dict[str, Any]] = []
                head_ops: list[UpdateOne] = []
                close_ops: list[UpdateMany] = []
                retract_ops: list[UpdateMany] = []
                closed_total = 0

                for item in batch:
                    dep_id = item["dependency_id"]
                    head = heads[dep_id]
                    floor = as_utc(head["last_tx"])
                    tx = self._resolve_tx(item.get("recorded_at"), floor=floor)
                    ev_type = item["type"]
                    seq = head["event_seq"]
                    kind = item.get("kind", "correction" if ev_type == "corrected" else "change")

                    batch_events: list[dict[str, Any]] = []
                    retract_ids: list[str] = []
                    retracted_by: str | None = None
                    cause_id: str

                    if ev_type == "corrected":
                        audit_id = new_event_id()
                        cause_id = audit_id
                        retracted_by = audit_id
                        retract_ids = list(item.get("retract_event_ids") or [])
                        existing_by_id = {e["_id"]: e for e in existing_events_by_dep[dep_id]}
                        for rid in retract_ids:
                            if rid not in existing_by_id or existing_by_id[rid]["type"] not in EVENT_TYPES:
                                raise InvalidRequestError(f"event {rid!r} not retractable on {dep_id!r}")
                            if existing_by_id[rid].get("retracted_at") is not None:
                                raise ConflictError(f"event {rid!r} already retracted")
                        added_docs: list[dict[str, Any]] = []
                        for i, ae in enumerate(item.get("add_events") or [], start=2):
                            va = parse_optional_utc(ae.get("valid_at"))
                            if va is None:
                                raise InvalidRequestError("correction add_events require valid_at")
                            f: dict[str, Any] = {}
                            if ae.get("weight") is not None:
                                f["weight"] = float(ae["weight"])
                            if ae.get("metadata") is not None:
                                f["metadata"] = dict(ae["metadata"])
                            added_docs.append(
                                event_doc("dependency", dep_id, ae["type"], va, seq + i, tx, f,
                                          kind="correction", cause=audit_id)
                            )
                        audit = event_doc(
                            "dependency", dep_id, "corrected", None, seq + 1, tx,
                            {"retracted": retract_ids, "added": [a["_id"] for a in added_docs],
                             "reason": item.get("reason")},
                            kind="correction", client_event_id=item.get("client_event_id"), event_id=audit_id,
                        )
                        batch_events = [audit, *added_docs]
                    else:
                        if ev_type not in EVENT_TYPES:
                            raise InvalidRequestError(f"unknown dependency event type {ev_type!r}")
                        va = parse_optional_utc(item.get("effective_from") or item.get("valid_at")) or tx
                        f = {}
                        if item.get("weight") is not None:
                            f["weight"] = float(item["weight"])
                        if item.get("metadata") is not None:
                            f["metadata"] = dict(item["metadata"])
                        ev = event_doc(
                            "dependency", dep_id, ev_type, va, seq + 1, tx, f, kind=kind,
                            client_event_id=item.get("client_event_id"), event_id=item.get("event_id"),
                        )
                        cause_id = ev["_id"]
                        batch_events = [ev]

                    retract_set = set(retract_ids)
                    src, tgt = nodes_map[head["source"]], nodes_map[head["target"]]
                    window = endpoint_window(
                        as_utc(src["valid_from"]), as_utc(src["valid_to"]),
                        as_utc(tgt["valid_from"]), as_utc(tgt["valid_to"]),
                    )
                    known = [
                        DepEvent(
                            event_id=d["_id"], type=d["type"], valid_at=as_utc(d["valid_at"]), seq=d["seq"],
                            tx_time=as_utc(d["tx_time"]), fields=d.get("fields") or {},
                        )
                        for d in existing_events_by_dep[dep_id] + batch_events
                        if d["type"] in EVENT_TYPES and d.get("retracted_at") is None and d["_id"] not in retract_set
                    ]
                    segments = timeline(known, window)
                    current = current_vers_by_dep[dep_id]
                    close, insert = diff_rows(
                        [(r["_id"], row_key(as_utc(r["valid_from"]), as_utc(r["valid_to"]),
                                             r["weight"], r.get("metadata")))
                         for r in current],
                        segments,
                    )

                    if insert:
                        clash = self.repo.overlapping_same_triple(
                            head["source"], head["target"], head["relationship"], dep_id,
                            [(seg.valid_from, seg.valid_to) for seg in insert], s,
                            dataset=head.get("dataset"),
                        )
                        if clash:
                            raise DuplicateDependencyError(
                                "overlapping equivalent dependency",
                                details={"dependency_id": dep_id, "conflicting_dependency_id": clash["dependency_id"]},
                            )

                    if retract_ids:
                        retract_ops.append(UpdateMany(
                            {"_id": {"$in": retract_ids}, "retracted_at": None},
                            {"$set": {"retracted_at": tx, "retracted_by": retracted_by}},
                        ))
                    if close:
                        closed_total += len(close)
                        close_ops.append(UpdateMany(
                            {"_id": {"$in": close}, "tx_to": MAX_TIME},
                            {"$set": {"tx_to": tx, "closed_by_event": cause_id}},
                        ))
                    for i, seg in enumerate(insert, start=1):
                        new_ver_docs.append({
                            "_id": f'{dep_id}#v{head["version_seq"] + i}',
                            "dependency_id": dep_id, "source": head["source"], "target": head["target"],
                            "relationship": head["relationship"], "weight": float(seg.weight),
                            "metadata": seg.metadata, "valid_from": seg.valid_from, "valid_to": seg.valid_to,
                            "tx_from": tx, "tx_to": MAX_TIME, "created_by_event": cause_id,
                        })

                    changed = bool(close or insert)
                    new_rev = head["rev"] + (1 if changed else 0)
                    head_ops.append(UpdateOne(
                        {"_id": dep_id, "event_seq": head["event_seq"]},
                        {"$set": {"last_tx": tx, "updated_at": tx},
                         "$inc": {"event_seq": len(batch_events), "version_seq": len(insert),
                                  "rev": 1 if changed else 0}},
                    ))
                    new_ev_docs.extend(batch_events)
                    if changed:
                        out_docs.append(self.repo.new_outbox_doc("dependency", dep_id, new_rev, cause_id))

                self.repo.insert_events(new_ev_docs, s)
                if retract_ops:
                    self.repo.events.bulk_write(retract_ops, ordered=False, session=s)
                if close_ops:
                    self.repo.versions.bulk_write(close_ops, ordered=False, session=s)
                self.repo.insert_versions(new_ver_docs, s)
                if head_ops:
                    self.repo.heads.bulk_write(head_ops, ordered=False, session=s)
                self.repo.enqueue(out_docs, s)
                return len(new_ev_docs), len(new_ver_docs), closed_total

            n_evs, n_vers, n_closed = self.repo.run_transaction(txn)
            t_mongo += time.perf_counter() - t0
            metrics.events_ingested += n_evs
            metrics.versions_created += n_vers
            metrics.versions_closed += n_closed

        for ev in events:
            did = ev["dependency_id"]
            if did in seen_deps or len(buf) >= chunk_size:
                flush(buf)
                buf = []
                seen_deps = set()
            buf.append(ev)
            seen_deps.add(did)
        flush(buf)

        metrics.mongo_ingest_ms = round(t_mongo * 1000, 3)
        if sync and metrics.events_ingested:
            t1 = time.perf_counter()
            rep = self.sync.drain()
            metrics.sync_ms = round((time.perf_counter() - t1) * 1000, 3)
            metrics.sync_report = rep.model_dump()
        metrics.total_ms = round((time.perf_counter() - t_start) * 1000, 3)
        return metrics

    # ------------------------------------------------------------ combined I/O
    def ingest_batch(
        self,
        nodes: Sequence[dict[str, Any]],
        dependencies: Sequence[dict[str, Any]],
        events: Sequence[dict[str, Any]] = (),
        *,
        chunk_size: int = 1000,
        validate: bool = True,
        sync: bool = True,
    ) -> IngestMetrics:
        """Ingest nodes, initial dependencies, and optional temporal events."""
        t_start = time.perf_counter()
        combined = IngestMetrics()
        if nodes:
            m_n = self.ingest_nodes(nodes, chunk_size=chunk_size, validate=validate, sync=False)
            combined.nodes_ingested += m_n.nodes_ingested
            combined.events_ingested += m_n.events_ingested
            combined.mongo_ingest_ms += m_n.mongo_ingest_ms
        if dependencies:
            m_d = self.ingest_dependencies(dependencies, chunk_size=chunk_size, validate=validate, sync=False)
            combined.dependencies_ingested += m_d.dependencies_ingested
            combined.events_ingested += m_d.events_ingested
            combined.versions_created += m_d.versions_created
            combined.mongo_ingest_ms += m_d.mongo_ingest_ms
        if events:
            m_e = self.ingest_dependency_events(events, chunk_size=min(chunk_size, 500), sync=False)
            combined.events_ingested += m_e.events_ingested
            combined.versions_created += m_e.versions_created
            combined.versions_closed += m_e.versions_closed
            combined.mongo_ingest_ms += m_e.mongo_ingest_ms

        combined.mongo_ingest_ms = round(combined.mongo_ingest_ms, 3)
        if sync and (combined.nodes_ingested or combined.dependencies_ingested or combined.events_ingested):
            t1 = time.perf_counter()
            rep: ProcessReport = self.sync.drain()
            combined.sync_ms = round((time.perf_counter() - t1) * 1000, 3)
            combined.sync_report = rep.model_dump()
        combined.total_ms = round((time.perf_counter() - t_start) * 1000, 3)
        return combined

    def ingest_files(
        self,
        nodes_path: str | Path | None = None,
        dependencies_path: str | Path | None = None,
        events_path: str | Path | None = None,
        *,
        chunk_size: int = 1000,
        validate: bool = True,
        sync: bool = True,
    ) -> IngestMetrics:
        """Stream JSONL or CSV files into the engine."""
        t_start = time.perf_counter()
        combined = IngestMetrics()
        if nodes_path is not None:
            m_n = self.ingest_nodes(load_records(nodes_path), chunk_size=chunk_size, validate=validate, sync=False)
            combined.nodes_ingested += m_n.nodes_ingested
            combined.events_ingested += m_n.events_ingested
            combined.mongo_ingest_ms += m_n.mongo_ingest_ms
        if dependencies_path is not None:
            m_d = self.ingest_dependencies(
                load_records(dependencies_path), chunk_size=chunk_size, validate=validate, sync=False
            )
            combined.dependencies_ingested += m_d.dependencies_ingested
            combined.events_ingested += m_d.events_ingested
            combined.versions_created += m_d.versions_created
            combined.mongo_ingest_ms += m_d.mongo_ingest_ms
        if events_path is not None:
            m_e = self.ingest_dependency_events(
                load_records(events_path), chunk_size=min(chunk_size, 500), sync=False
            )
            combined.events_ingested += m_e.events_ingested
            combined.versions_created += m_e.versions_created
            combined.versions_closed += m_e.versions_closed
            combined.mongo_ingest_ms += m_e.mongo_ingest_ms

        combined.mongo_ingest_ms = round(combined.mongo_ingest_ms, 3)
        if sync and (combined.nodes_ingested or combined.dependencies_ingested or combined.events_ingested):
            t1 = time.perf_counter()
            rep = self.sync.drain()
            combined.sync_ms = round((time.perf_counter() - t1) * 1000, 3)
            combined.sync_report = rep.model_dump()
        combined.total_ms = round((time.perf_counter() - t_start) * 1000, 3)
        return combined

    def ingest_universal(
        self,
        path: str | Path,
        *,
        chunk_size: int = 1000,
        validate: bool = False,
        sync: bool = True,
        dataset: str = "default",
        replace: bool = False,
    ) -> IngestMetrics:
        """Stream any archive (.zip, .tar.gz) or data file (.csv, .json, .jsonl, .txt) directly into the engine."""
        from app.ingestion.universal_loader import UniversalDatasetLoader

        if replace:
            log.info("replacing existing dataset", extra={"dataset": dataset})
            self.repo.delete_dataset(dataset)
            self.sync.neo.delete_dataset(dataset)

        loader = UniversalDatasetLoader(path)
        t_start = time.perf_counter()

        def node_stream() -> Iterator[dict[str, Any]]:
            seen: set[str] = set()
            for kind, ent in loader.stream_raw_records():
                if kind == "node":
                    nid = ent.get("id")
                    if nid and nid not in seen:
                        seen.add(nid)
                        ent["dataset"] = dataset
                        yield ent
                elif kind == "dependency":
                    for missing in (ent.get("source"), ent.get("target")):
                        if missing and missing not in seen:
                            seen.add(missing)
                            yield {"id": missing, "name": missing, "type": "component", "dataset": dataset}

        def dep_stream() -> Iterator[dict[str, Any]]:
            for kind, ent in loader.stream_raw_records():
                if kind == "dependency":
                    ent["dataset"] = dataset
                    yield ent

        m_n = self.ingest_nodes(
            node_stream(), chunk_size=chunk_size, validate=validate, sync=False, ignore_existing=True, dataset=dataset
        )
        m_d = self.ingest_dependencies(
            dep_stream(), chunk_size=chunk_size, validate=validate, sync=False, ignore_existing=True, dataset=dataset
        )

        combined = IngestMetrics(
            nodes_ingested=m_n.nodes_ingested,
            dependencies_ingested=m_d.dependencies_ingested,
            events_ingested=m_n.events_ingested + m_d.events_ingested,
            versions_created=m_d.versions_created,
            mongo_ingest_ms=round(m_n.mongo_ingest_ms + m_d.mongo_ingest_ms, 3),
        )
        if sync and (combined.nodes_ingested or combined.dependencies_ingested):
            t1 = time.perf_counter()
            rep = self.sync.drain()
            combined.sync_ms = round((time.perf_counter() - t1) * 1000, 3)
            combined.sync_report = rep.model_dump()
        combined.total_ms = round((time.perf_counter() - t_start) * 1000, 3)
        return combined
