"""MongoDB -> Neo4j synchronisation (transactional outbox, state-based projection).

Guarantees
----------
* Every write commits domain data AND its outbox entry in one MongoDB
  transaction, so no change can be lost between the databases.
* Delivery is at-least-once. Processing is *state-based*: the projector
  reads the latest state of the aggregate from a MongoDB snapshot and upserts
  it, guarded by the aggregate revision (``rev``). Duplicate, reordered or
  concurrent processing therefore converges to the same graph.
* Failures are never ignored: attempts are counted, retried with exponential
  backoff and dead-lettered (status ``failed``) after ``sync_max_attempts``;
  they are logged and exposed via ``/api/v1/sync/status``.
* ``reconcile`` detects drift (revision/version-count mismatch, orphans) and
  can repair it; ``rebuild`` re-projects everything from MongoDB.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import timedelta
from typing import Any

from bson import ObjectId

from app.api.schemas.sync import (
    OutboxFailure, ProcessReport, RebuildReport, ReconcileReport, SyncStatus,
)
from app.config import Settings
from app.domain.temporal import canonical_json
from app.domain.time import utc_now
from app.repositories.mongodb_repository import MongoRepository
from app.repositories.neo4j_repository import Neo4jRepository

log = logging.getLogger(__name__)


# ---------------------------------------------------------------- converters
def node_projection(doc: dict) -> dict[str, Any]:
    dataset = doc.get("dataset") or "default"
    nid = doc.get("id") or doc["_id"]
    return {
        "id": nid,
        "dataset": dataset,
        "node_key": f"{dataset}:{nid}",
        "name": doc["name"],
        "type": doc["type"],
        "description": doc.get("description"),
        "metadata_json": canonical_json(doc.get("metadata") or {}),
        "valid_from": doc["valid_from"],
        "valid_to": doc["valid_to"],
        "deleted": doc["deleted"],
        "created_at": doc["created_at"],
        "updated_at": doc["updated_at"],
        "rev": doc["rev"],
    }


def version_projection(row: dict, rev: int) -> dict[str, Any]:
    dataset = row.get("dataset") or "default"
    vid = row["_id"]
    src = row["source"]
    tgt = row["target"]
    return {
        "version_id": vid,
        "dataset": dataset,
        "version_key": f"{dataset}:{vid}",
        "source": src,
        "target": tgt,
        "source_key": f"{dataset}:{src}",
        "target_key": f"{dataset}:{tgt}",
        "rev": rev,
        "props": {
            "dependency_id": row["dependency_id"],
            "dataset": dataset,
            "relationship": row["relationship"],
            "weight": float(row["weight"]),
            "metadata_json": canonical_json(row.get("metadata") or {}),
            "valid_from": row["valid_from"],
            "valid_to": row["valid_to"],
            "tx_from": row["tx_from"],
            "tx_to": row["tx_to"],
        },
    }


class SyncService:
    def __init__(self, mongo: MongoRepository, neo: Neo4jRepository, settings: Settings) -> None:
        self.mongo = mongo
        self.neo = neo
        self.settings = settings
        self.schema_ready = False
        self._worker: threading.Thread | None = None
        self._stop = threading.Event()

    # ------------------------------------------------------------------ schema
    def ensure_schema(self) -> bool:
        if not self.schema_ready:
            try:
                self.neo.ensure_schema()
                self.schema_ready = True
            except Exception as exc:
                log.warning("neo4j schema setup failed; will retry", extra={"error": type(exc).__name__})
        return self.schema_ready

    # ------------------------------------------------------------- processing
    def load_state(self, node_ids: set[str], dep_ids: set[str]) -> tuple[list[dict], list[dict], list[dict]]:
        """Read projection state from one consistent MongoDB snapshot."""
        with self.mongo.snapshot_session() as s:
            nodes = list(self.mongo.nodes.find({"$or": [{"_id": {"$in": list(node_ids)}}, {"id": {"$in": list(node_ids)}}]}, session=s)) if node_ids else []
            heads = list(self.mongo.heads.find({"$or": [{"_id": {"$in": list(dep_ids)}}, {"id": {"$in": list(dep_ids)}}]}, session=s)) if dep_ids else []
            all_dep_ids = set(dep_ids)
            for h in heads:
                if h.get("dependency_id"):
                    all_dep_ids.add(h["dependency_id"])
                if h.get("id"):
                    all_dep_ids.add(h["id"])
            rows = (
                list(self.mongo.versions.find({"$or": [{"dependency_id": {"$in": list(all_dep_ids)}}, {"_id": {"$in": list(all_dep_ids)}}]}, session=s))
                if all_dep_ids else []
            )
        return nodes, heads, rows

    def _project_aggregates(self, node_ids: set[str], dep_ids: set[str], prune: bool = False) -> tuple[int, int]:
        nodes, heads, rows = self.load_state(node_ids, dep_ids)
        rev_by_dep: dict[str, int] = {}
        for h in heads:
            rev_by_dep[h["_id"]] = h["rev"]
            if "dependency_id" in h:
                rev_by_dep[h["dependency_id"]] = h["rev"]
            if "id" in h:
                rev_by_dep[h["id"]] = h["rev"]
        node_rows = [node_projection(n) for n in nodes]
        version_rows = [version_projection(r, rev_by_dep.get(r.get("dependency_id", ""), rev_by_dep.get(r["_id"], 1))) for r in rows]
        prune_rows = None
        if prune:
            ids_by_dep: dict[str, list[str]] = {d: [] for d in dep_ids}
            for r in rows:
                ids_by_dep[r["dependency_id"]].append(r["_id"])
            prune_rows = [{"id": d, "version_ids": v} for d, v in ids_by_dep.items()]
        self.neo.project(node_rows, version_rows, prune=prune_rows)
        return len(node_rows), len(version_rows)

    def _project_docs(self, docs: list[dict]) -> None:
        node_ids = {d["aggregate_id"] for d in docs if d["aggregate_type"] == "node"}
        dep_ids = {d["aggregate_id"] for d in docs if d["aggregate_type"] == "dependency"}
        self._project_aggregates(node_ids, dep_ids)

    def _backoff(self, attempts: int) -> float:
        s = self.settings
        return min(s.sync_backoff_base_seconds * (2 ** max(attempts - 1, 0)), s.sync_backoff_max_seconds)

    def _record_failure(self, token: str, doc: dict, exc: Exception, report: ProcessReport) -> None:
        attempts = doc.get("attempts", 0) + 1
        dead = attempts >= self.settings.sync_max_attempts
        next_at = utc_now() + timedelta(seconds=self._backoff(attempts))
        self.mongo.mark_failure(
            token, doc["_id"], attempts, f"{type(exc).__name__}: {str(exc)[:500]}", type(exc).__name__, next_at, dead
        )
        extra = {"outbox_id": str(doc["_id"]), "aggregate": f'{doc["aggregate_type"]}:{doc["aggregate_id"]}',
                 "attempts": attempts, "error": type(exc).__name__}
        if dead:
            report.dead_lettered += 1
            log.error("sync event dead-lettered", extra=extra)
        else:
            report.retried += 1
            log.warning("sync event failed; will retry", extra=extra)

    def _process(self, token: str, docs: list[dict], report: ProcessReport) -> None:
        if not docs:
            return
        try:
            self._project_docs(docs)
            report.done += self.mongo.mark_done(token, [d["_id"] for d in docs])
        except Exception as exc:
            if len(docs) > 1:
                # isolate the poison event(s): retry one by one
                for d in docs:
                    self._process(token, [d], report)
            else:
                self._record_failure(token, docs[0], exc, report)

    def process_pending(self, limit: int | None = None) -> ProcessReport:
        t0 = time.perf_counter()
        report = ProcessReport()
        token, docs = self.mongo.claim_outbox(limit or self.settings.sync_batch_size, self.settings.sync_lease_seconds)
        report.claimed = len(docs)
        self._process(token, docs, report)
        report.duration_ms = round((time.perf_counter() - t0) * 1000, 3)
        return report

    def drain(self, max_batches: int = 1000) -> ProcessReport:
        """Process until nothing is claimable (or max_batches)."""
        total = ProcessReport()
        for _ in range(max_batches):
            r = self.process_pending()
            for f in ("claimed", "done", "retried", "dead_lettered"):
                setattr(total, f, getattr(total, f) + getattr(r, f))
            total.duration_ms = round(total.duration_ms + r.duration_ms, 3)
            if r.claimed == 0:
                break
        return total

    def process_ids(self, ids: list[ObjectId]) -> str:
        """Inline (best-effort) projection of specific outbox entries."""
        if not ids:
            return "none"
        if not self.settings.sync_inline:
            return "deferred"
        try:
            report = ProcessReport()
            token, docs = self.mongo.claim_outbox(len(ids), self.settings.sync_lease_seconds, ids=ids)
            self._process(token, docs, report)
            statuses = self.mongo.outbox_statuses(ids)
        except Exception as exc:  # never fail the (already committed) write
            log.warning("inline sync failed; left for worker", extra={"error": type(exc).__name__})
            return "pending"
        values = set(statuses.values())
        if values == {"done"}:
            return "synced"
        if "failed" in values:
            return "failed"
        return "pending"

    def retry_failed(self) -> int:
        return self.mongo.reset_failed()

    # ------------------------------------------------------------------ status
    def status(self) -> SyncStatus:
        oldest = self.mongo.oldest_pending()
        lag = None
        if oldest:
            lag = round((utc_now() - oldest["created_at"]).total_seconds(), 3)
        return SyncStatus(
            counts=self.mongo.outbox_counts(),
            oldest_pending_created_at=oldest["created_at"] if oldest else None,
            pending_lag_seconds=lag,
            failures=[
                OutboxFailure(
                    outbox_id=str(d["_id"]), aggregate_type=d["aggregate_type"], aggregate_id=d["aggregate_id"],
                    attempts=d["attempts"], status=d["status"], last_error_type=d.get("last_error_type"),
                    next_attempt_at=d.get("next_attempt_at"), created_at=d["created_at"],
                )
                for d in self.mongo.outbox_failures()
            ],
            worker_running=bool(self._worker and self._worker.is_alive()),
            neo4j_schema_ready=self.schema_ready,
        )

    # --------------------------------------------------------------- reconcile
    def reconcile(self, repair: bool = False) -> ReconcileReport:
        t0 = time.perf_counter()
        m_nodes = self.mongo.node_revs()
        m_deps = self.mongo.dependency_revs()
        n_nodes = self.neo.node_revs()
        n_deps = self.neo.dependency_revs()

        missing_nodes = sorted(k for k in m_nodes if k not in n_nodes)
        stale_nodes = sorted(k for k, rev in m_nodes.items() if k in n_nodes and n_nodes[k] != rev)
        orphan_nodes = sorted(k for k in n_nodes if k not in m_nodes)
        stale_deps: list[str] = []
        for dep_id, (rev, n_versions) in m_deps.items():
            proj = n_deps.get(dep_id)
            if proj is None:
                if n_versions > 0:
                    stale_deps.append(dep_id)
            elif not (proj[0] == proj[1] == rev and proj[2] == n_versions):
                stale_deps.append(dep_id)
        orphan_deps = sorted(k for k in n_deps if k not in m_deps)
        stale_deps.sort()

        consistent = not (missing_nodes or stale_nodes or orphan_nodes or stale_deps or orphan_deps)
        repaired = False
        if repair and not consistent:
            if orphan_nodes or orphan_deps:
                self.neo.delete_orphans(orphan_nodes, orphan_deps)
            if missing_nodes or stale_nodes or stale_deps:
                self._project_aggregates(set(missing_nodes) | set(stale_nodes), set(stale_deps), prune=True)
            repaired = True
            log.warning("projection repaired", extra={
                "missing_nodes": len(missing_nodes), "stale_nodes": len(stale_nodes),
                "orphan_nodes": len(orphan_nodes), "stale_dependencies": len(stale_deps),
                "orphan_dependencies": len(orphan_deps)})

        return ReconcileReport(
            consistent=consistent,
            mongo_nodes=len(m_nodes), neo4j_nodes=len(n_nodes),
            mongo_dependencies=len(m_deps),
            mongo_versions=sum(v for _, v in m_deps.values()),
            neo4j_versions=sum(v[2] for v in n_deps.values()),
            stale_nodes=stale_nodes[:1000], missing_nodes=missing_nodes[:1000], orphan_nodes=orphan_nodes[:1000],
            stale_dependencies=stale_deps[:1000], orphan_dependencies=orphan_deps[:1000],
            repaired=repaired,
            duration_ms=round((time.perf_counter() - t0) * 1000, 3),
        )

    def rebuild(self, node_batch: int = 5000, dep_batch: int = 2000) -> RebuildReport:
        """Wipe Neo4j and re-project the full state from MongoDB."""
        t0 = time.perf_counter()
        self.neo.wipe()
        self.ensure_schema()
        n_nodes = n_deps = n_versions = 0
        batch: list[str] = []
        for d in self.mongo.nodes.find({}, {"_id": 1}).sort("_id", 1):
            batch.append(d["_id"])
            if len(batch) >= node_batch:
                n_nodes += self._project_aggregates(set(batch), set())[0]
                batch = []
        if batch:
            n_nodes += self._project_aggregates(set(batch), set())[0]
        batch = []
        for d in self.mongo.heads.find({}, {"_id": 1}).sort("_id", 1):
            batch.append(d["_id"])
            if len(batch) >= dep_batch:
                n_versions += self._project_aggregates(set(), set(batch))[1]
                n_deps += len(batch)
                batch = []
        if batch:
            n_versions += self._project_aggregates(set(), set(batch))[1]
            n_deps += len(batch)
        log.info("projection rebuilt", extra={"nodes": n_nodes, "dependencies": n_deps, "versions": n_versions})
        return RebuildReport(nodes=n_nodes, dependencies=n_deps, versions=n_versions,
                             duration_ms=round((time.perf_counter() - t0) * 1000, 3))

    # ------------------------------------------------------------------ worker
    def start_worker(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        self._stop.clear()
        self._worker = threading.Thread(target=self._run, name="sync-worker", daemon=True)
        self._worker.start()

    def stop_worker(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._worker:
            self._worker.join(timeout)

    def _run(self) -> None:
        log.info("sync worker started")
        while not self._stop.is_set():
            claimed = 0
            try:
                if self.ensure_schema():
                    claimed = self.process_pending().claimed
            except Exception as exc:
                log.warning("sync worker iteration failed", extra={"error": type(exc).__name__})
            if claimed == 0:
                self._stop.wait(self.settings.sync_poll_interval_seconds)
        log.info("sync worker stopped")
