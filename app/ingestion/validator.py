"""Shared domain & batch-ingestion validation rules.

Used by both Pydantic API schemas and the bulk ingestion pipeline so single-item
API writes and batch dataset imports enforce identical invariants.
"""
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Sequence

from app.domain.time import MAX_TIME, MIN_TIME, normalize

ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:@\-]{0,255}$"
RELATIONSHIP_PATTERN = r"^[A-Za-z][A-Za-z0-9_.:\-]{0,63}$"
NODE_TYPE_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.:\- ]{0,63}$"
MAX_METADATA_BYTES = 16 * 1024

_ID_RE = re.compile(ID_PATTERN)
_REL_RE = re.compile(RELATIONSHIP_PATTERN)
_TYPE_RE = re.compile(NODE_TYPE_PATTERN)


def check_metadata_keys(value: Any, path: str = "metadata") -> None:
    if isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str) or not k or k.startswith("$") or "." in k:
                raise ValueError(
                    f"invalid key {k!r} in {path}: keys must be non-empty, not start with '$', no '.'"
                )
            check_metadata_keys(v, f"{path}.{k}")
    elif isinstance(value, list):
        for i, v in enumerate(value):
            check_metadata_keys(v, f"{path}[{i}]")


def validate_metadata(value: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("metadata must be a JSON object")
    check_metadata_keys(value)
    if len(json.dumps(value, default=str)) > MAX_METADATA_BYTES:
        raise ValueError(f"metadata exceeds {MAX_METADATA_BYTES} bytes")
    return value


def validate_entity_id(value: str, field_name: str = "id") -> str:
    if not isinstance(value, str) or not _ID_RE.match(value):
        raise ValueError(f"{field_name} {value!r} does not match {ID_PATTERN}")
    return value


def validate_relationship(value: str) -> str:
    if not isinstance(value, str) or not _REL_RE.match(value):
        raise ValueError(f"relationship {value!r} does not match {RELATIONSHIP_PATTERN}")
    return value


def validate_node_type(value: str) -> str:
    if not isinstance(value, str) or not _TYPE_RE.match(value):
        raise ValueError(f"type {value!r} does not match {NODE_TYPE_PATTERN}")
    return value


def validate_weight(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"weight must be a number, got {type(value).__name__}")
    w = float(value)
    if not math.isfinite(w) or w < 0.0:
        raise ValueError(f"weight must be finite and >= 0, got {value!r}")
    return w


def parse_optional_utc(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return normalize(value)
    if isinstance(value, str):
        return normalize(datetime.fromisoformat(value.replace("Z", "+00:00")))
    raise ValueError(f"invalid datetime value: {value!r}")


@dataclass(frozen=True)
class ValidationIssue:
    kind: str  # "node" | "dependency" | "event"
    index: int
    entity_id: str | None
    code: str
    message: str


@dataclass
class ValidationReport:
    errors: list[ValidationIssue] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    checked_nodes: int = 0
    checked_dependencies: int = 0

    @property
    def valid(self) -> bool:
        return len(self.errors) == 0


def validate_batch(
    nodes: Sequence[dict[str, Any]],
    dependencies: Sequence[dict[str, Any]],
    *,
    existing_node_ids: Iterable[str] = (),
    existing_dep_ids: Iterable[str] = (),
    max_errors: int = 500,
) -> ValidationReport:
    """Validate a batch of node and dependency records before ingestion."""
    report = ValidationReport(checked_nodes=len(nodes), checked_dependencies=len(dependencies))
    known_nodes = set(existing_node_ids)
    known_deps = set(existing_dep_ids)
    batch_nodes: dict[str, tuple[datetime, datetime]] = {}
    batch_deps: set[str] = set()
    # (source, target, relationship) -> list of (valid_from, valid_to, dep_id)
    triple_intervals: dict[tuple[str, str, str], list[tuple[datetime, datetime, str]]] = {}

    def add_error(kind: str, idx: int, eid: str | None, code: str, msg: str) -> None:
        if len(report.errors) < max_errors:
            report.errors.append(ValidationIssue(kind, idx, eid, code, msg))

    for idx, n in enumerate(nodes):
        nid = n.get("id")
        try:
            validate_entity_id(nid, "id")
        except ValueError as exc:
            add_error("node", idx, str(nid) if nid is not None else None, "invalid_id", str(exc))
            continue
        if nid in batch_nodes:
            add_error("node", idx, nid, "duplicate_node_in_batch", f"duplicate node id {nid!r} in batch")
            continue
        if nid in known_nodes:
            add_error("node", idx, nid, "node_already_exists", f"node {nid!r} already exists")
            continue
        try:
            validate_node_type(n.get("type", "component"))
            if n.get("metadata") is not None:
                validate_metadata(n["metadata"])
            vf = parse_optional_utc(n.get("valid_from")) or MIN_TIME
            vt = parse_optional_utc(n.get("valid_to")) or MAX_TIME
            if vt <= vf:
                raise ValueError("valid_to must be strictly after valid_from")
        except ValueError as exc:
            add_error("node", idx, nid, "invalid_node", str(exc))
            continue
        batch_nodes[nid] = (vf, vt)
        known_nodes.add(nid)

    for idx, d in enumerate(dependencies):
        did = d.get("id")
        if did is not None:
            try:
                validate_entity_id(did, "id")
            except ValueError as exc:
                add_error("dependency", idx, str(did), "invalid_id", str(exc))
                continue
            if did in batch_deps or did in known_deps:
                add_error("dependency", idx, did, "duplicate_dependency_id", f"duplicate dependency id {did!r}")
                continue
            batch_deps.add(did)

        src = d.get("source")
        tgt = d.get("target")
        rel = d.get("relationship", "depends_on")
        try:
            validate_entity_id(src, "source")
            validate_entity_id(tgt, "target")
            validate_relationship(rel)
            validate_weight(d.get("weight", 1.0))
            if d.get("metadata") is not None:
                validate_metadata(d["metadata"])
            vf = parse_optional_utc(d.get("valid_from")) or MIN_TIME
            vt = parse_optional_utc(d.get("valid_to")) or MAX_TIME
            if vt <= vf:
                raise ValueError("valid_to must be strictly after valid_from")
        except ValueError as exc:
            add_error("dependency", idx, did, "invalid_dependency", str(exc))
            continue

        missing = [x for x in (src, tgt) if x not in known_nodes]
        if missing:
            add_error("dependency", idx, did, "unknown_node", f"unknown endpoint node(s): {missing}")
            continue

        if src == tgt:
            report.warnings.append(f"dependency[{idx}] ({did or 'auto'}): self-loop on {src!r}")

        if src in batch_nodes and tgt in batch_nodes:
            w_from = max(batch_nodes[src][0], batch_nodes[tgt][0])
            w_to = min(batch_nodes[src][1], batch_nodes[tgt][1])
            eff_from, eff_to = max(vf, w_from), min(vt, w_to)
            if eff_from >= eff_to:
                add_error(
                    "dependency", idx, did, "outside_node_validity",
                    f"interval [{vf.isoformat()}, {vt.isoformat()}) outside endpoint validity",
                )
                continue
        else:
            eff_from, eff_to = vf, vt

        triple = (src, tgt, rel)
        for prev_from, prev_to, prev_id in triple_intervals.get(triple, ()):
            if eff_from < prev_to and eff_to > prev_from:
                add_error(
                    "dependency", idx, did, "overlapping_triple",
                    f"overlaps dependency {prev_id!r} for {triple}",
                )
                break
        else:
            triple_intervals.setdefault(triple, []).append((eff_from, eff_to, did or f"#{idx}"))

    return report
