"""Bitemporal semantics for dependencies (pure functions, no I/O).

Model
-----
Every logical dependency (``dependency_id``) owns an append-only log of
*events*. Each event has

* ``valid_at``  - valid time: when the change happened in the modeled system;
* ``tx_time``   - transaction time: when the engine recorded the event;
* ``seq``       - per-dependency arrival order (tie-breaker).

Event types:

* ``created``   - dependency starts to exist; sets weight and metadata.
* ``modified``  - sets the given attributes from ``valid_at`` onwards.
* ``removed``   - dependency stops existing from ``valid_at``.
* ``restored``  - dependency exists again from ``valid_at`` (optional attrs).

Corrections retract previously recorded events (``retracted_at`` = tx time of
the correction) and/or add new events with arbitrary valid times.
Late-arriving events are simply events whose ``valid_at`` lies in the past.

Valid-time timeline
-------------------
The valid-time state as known at transaction time ``k`` is the *fold* of all
events with ``tx_time <= k`` that were not retracted by ``k``, ordered by
``(valid_at, seq)``, clipped to the validity window of both endpoint nodes.
The fold yields non-overlapping half-open segments ``[valid_from, valid_to)``
with constant attributes (adjacent equal segments are coalesced).

Materialised version rows
-------------------------
After every write, the engine diffs the new timeline against the currently
known rows (``tx_to = MAX``): unchanged rows are kept, others are closed by
setting ``tx_to = now`` and new rows are inserted with ``tx_from = now``.
Rows are never deleted. Therefore, for any (valid_at v, known_at k):

    visible(row) <=> valid_from <= v < valid_to  AND  tx_from <= k < tx_to

returns exactly the state that the fold of the events known at ``k`` gives at
``v``. This invariant is verified by a randomized property test.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from itertools import groupby
from typing import Any, Iterable, Sequence

from app.domain.time import MAX_TIME

EVENT_TYPES = ("created", "modified", "removed", "restored")
DEFAULT_WEIGHT = 1.0


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


@dataclass(frozen=True, eq=False)
class DepEvent:
    event_id: str
    type: str
    valid_at: datetime
    seq: int
    tx_time: datetime
    fields: dict[str, Any] = field(default_factory=dict)
    retracted_at: datetime | None = None

    def is_known(self, known_at: datetime | None = None) -> bool:
        """Known at ``known_at``; ``None`` means 'as currently known'."""
        if known_at is None:
            return self.retracted_at is None
        return self.tx_time <= known_at and (self.retracted_at is None or self.retracted_at > known_at)


@dataclass(frozen=True)
class Segment:
    valid_from: datetime
    valid_to: datetime
    weight: float
    metadata: dict[str, Any]

    def attrs_key(self) -> tuple[float, str]:
        return (float(self.weight), canonical_json(self.metadata))

    def key(self) -> tuple:
        return (self.valid_from, self.valid_to, *self.attrs_key())

    def contains(self, t: datetime) -> bool:
        return self.valid_from <= t < self.valid_to


def fold_events(events: Iterable[DepEvent]) -> list[Segment]:
    """Fold events (already filtered for knowledge) into a valid-time timeline."""
    ordered = sorted(events, key=lambda e: (e.valid_at, e.seq))
    exists = False
    weight = DEFAULT_WEIGHT
    metadata: dict[str, Any] = {}
    points: list[tuple[datetime, bool, float, dict[str, Any]]] = []

    for t, group in groupby(ordered, key=lambda e: e.valid_at):
        for e in group:
            f = e.fields or {}
            if e.type == "created":
                exists = True
                weight = float(f.get("weight", DEFAULT_WEIGHT) if f.get("weight") is not None else DEFAULT_WEIGHT)
                metadata = dict(f.get("metadata") or {})
            elif e.type in ("modified", "restored"):
                if f.get("weight") is not None:
                    weight = float(f["weight"])
                if f.get("metadata") is not None:
                    metadata = dict(f["metadata"])
                if e.type == "restored":
                    exists = True
            elif e.type == "removed":
                exists = False
            else:
                raise ValueError(f"unknown event type {e.type!r}")
        points.append((t, exists, weight, metadata))

    segments: list[Segment] = []
    for i, (t, ex, w, m) in enumerate(points):
        end = points[i + 1][0] if i + 1 < len(points) else MAX_TIME
        if ex and t < end:
            segments.append(Segment(t, end, w, m))
    return coalesce(segments)


def coalesce(segments: Sequence[Segment]) -> list[Segment]:
    out: list[Segment] = []
    for s in segments:
        if out and out[-1].valid_to == s.valid_from and out[-1].attrs_key() == s.attrs_key():
            out[-1] = Segment(out[-1].valid_from, s.valid_to, s.weight, s.metadata)
        else:
            out.append(s)
    return out


def clip(segments: Sequence[Segment], window_from: datetime, window_to: datetime) -> list[Segment]:
    """Restrict segments to [window_from, window_to) (endpoint node validity)."""
    out: list[Segment] = []
    for s in segments:
        a, b = max(s.valid_from, window_from), min(s.valid_to, window_to)
        if a < b:
            out.append(Segment(a, b, s.weight, s.metadata))
    return out


def endpoint_window(
    src_from: datetime, src_to: datetime, tgt_from: datetime, tgt_to: datetime
) -> tuple[datetime, datetime]:
    return max(src_from, tgt_from), min(src_to, tgt_to)


def timeline(
    events: Iterable[DepEvent],
    window: tuple[datetime, datetime],
    known_at: datetime | None = None,
) -> list[Segment]:
    known = [e for e in events if e.is_known(known_at)]
    return clip(fold_events(known), *window)


def row_key(valid_from: datetime, valid_to: datetime, weight: float, metadata: dict[str, Any]) -> tuple:
    return (valid_from, valid_to, float(weight), canonical_json(metadata or {}))


def diff_rows(
    current: Sequence[tuple[str, tuple]], new: Sequence[Segment]
) -> tuple[list[str], list[Segment]]:
    """Return (version_ids_to_close, segments_to_insert).

    ``current`` holds (version_id, row_key) for rows with tx_to == MAX.
    """
    new_keys = {s.key() for s in new}
    close = [vid for vid, key in current if key not in new_keys]
    kept = {key for _, key in current if key in new_keys}
    insert = [s for s in new if s.key() not in kept]
    return close, insert


# --------------------------------------------------------------------------
# Single definition of the bitemporal visibility predicate, rendered for each
# backend. Keeping it here avoids divergent business logic.
# --------------------------------------------------------------------------
def is_visible(
    valid_from: datetime, valid_to: datetime, tx_from: datetime, tx_to: datetime,
    valid_at: datetime, known_at: datetime,
) -> bool:
    return valid_from <= valid_at < valid_to and tx_from <= known_at < tx_to


def mongo_visibility_filter(valid_at: datetime, known_at: datetime) -> dict[str, Any]:
    return {
        "valid_from": {"$lte": valid_at},
        "valid_to": {"$gt": valid_at},
        "tx_from": {"$lte": known_at},
        "tx_to": {"$gt": known_at},
    }


def cypher_visibility(var: str = "r", valid_param: str = "valid_at", known_param: str = "known_at") -> str:
    return (
        f"{var}.valid_from <= ${valid_param} AND ${valid_param} < {var}.valid_to "
        f"AND {var}.tx_from <= ${known_param} AND ${known_param} < {var}.tx_to"
    )
