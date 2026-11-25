"""Unit tests for bitemporal timeline folding, clipping, diffing, and invariants."""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

import pytest

from app.domain.temporal import (
    DepEvent,
    Segment,
    clip,
    diff_rows,
    endpoint_window,
    fold_events,
    is_visible,
    row_key,
    timeline,
)
from app.domain.time import MAX_TIME, MIN_TIME, normalize

UTC = timezone.utc


def dt(day: int, hour: int = 0) -> datetime:
    return datetime(2026, 1, day, hour, 0, 0, tzinfo=UTC)


def test_normalize_truncates_ms_and_rejects_naive():
    aware = datetime(2026, 1, 1, 12, 0, 0, 123999, tzinfo=UTC)
    assert normalize(aware).microsecond == 123000
    with pytest.raises(ValueError):
        normalize(datetime(2026, 1, 1, 12, 0, 0))


def test_fold_create_modify_remove_restore_and_coalesce():
    events = [
        DepEvent("e1", "created", dt(1), 1, dt(1), {"weight": 1.0, "metadata": {"a": 1}}),
        # No-op modification with identical attributes -> coalesced
        DepEvent("e2", "modified", dt(3), 2, dt(3), {"weight": 1.0, "metadata": {"a": 1}}),
        DepEvent("e3", "modified", dt(5), 3, dt(5), {"weight": 2.5}),
        DepEvent("e4", "removed", dt(7), 4, dt(7), {}),
        DepEvent("e5", "restored", dt(10), 5, dt(10), {"weight": 4.0}),
    ]
    segs = fold_events(events)
    assert segs == [
        Segment(dt(1), dt(5), 1.0, {"a": 1}),
        Segment(dt(5), dt(7), 2.5, {"a": 1}),
        Segment(dt(10), MAX_TIME, 4.0, {"a": 1}),
    ]


def test_fold_late_arriving_event_and_tie_breaker():
    events = [
        DepEvent("e1", "created", dt(1), 1, dt(1), {"weight": 1.0}),
        DepEvent("e2", "modified", dt(10), 2, dt(10), {"weight": 3.0}),
        # Late-arriving event at dt(5) recorded at dt(12)
        DepEvent("e3", "modified", dt(5), 3, dt(12), {"weight": 2.0}),
        # Same valid_at dt(5), higher seq wins
        DepEvent("e4", "modified", dt(5), 4, dt(13), {"weight": 2.2}),
    ]
    segs = fold_events(events)
    assert segs == [
        Segment(dt(1), dt(5), 1.0, {}),
        Segment(dt(5), dt(10), 2.2, {}),
        Segment(dt(10), MAX_TIME, 3.0, {}),
    ]


def test_clip_to_endpoint_window():
    segs = [Segment(dt(1), dt(10), 1.0, {}), Segment(dt(15), dt(25), 2.0, {})]
    w = endpoint_window(dt(3), dt(20), MIN_TIME, MAX_TIME)
    assert clip(segs, *w) == [Segment(dt(3), dt(10), 1.0, {}), Segment(dt(15), dt(20), 2.0, {})]


def test_fold_unknown_event_raises():
    with pytest.raises(ValueError):
        fold_events([DepEvent("e1", "bogus", dt(1), 1, dt(1), {})])


def test_randomized_bitemporal_materialisation_invariant():
    """Verify materialised rows match fold_events across all (valid_at, known_at)."""
    rng = random.Random(42)
    base = datetime(2026, 1, 1, tzinfo=UTC)
    window = (base, base + timedelta(days=30))

    for _trial in range(30):
        events: list[DepEvent] = []
        rows: list[dict] = []  # materialised rows with tx_from/tx_to
        v_counter = 0

        # Start with a creation
        t_tx = base + timedelta(hours=1)
        events.append(DepEvent("e0", "created", base + timedelta(days=2), 1, t_tx, {"weight": 1.0, "metadata": {}}))
        segs = timeline(events, window, known_at=t_tx)
        for s in segs:
            v_counter += 1
            rows.append({
                "id": f"v{v_counter}", "valid_from": s.valid_from, "valid_to": s.valid_to,
                "weight": s.weight, "metadata": s.metadata, "tx_from": t_tx, "tx_to": MAX_TIME,
            })

        for step in range(1, 20):
            t_tx = base + timedelta(hours=step + 1)
            action = rng.choice(["modified", "removed", "restored", "retract"])
            if action == "retract":
                active_evs = [e for e in events if e.retracted_at is None]
                if not active_evs:
                    continue
                victim = rng.choice(active_evs)
                events = [
                    DepEvent(e.event_id, e.type, e.valid_at, e.seq, e.tx_time, e.fields,
                             t_tx if e is victim else e.retracted_at)
                    for e in events
                ]
            else:
                v_at = base + timedelta(days=rng.randint(0, 25))
                fields = {}
                if action in ("modified", "restored"):
                    fields["weight"] = float(rng.randint(1, 5))
                    fields["metadata"] = {"step": step % 3}
                events.append(DepEvent(f"e{step}", action, v_at, step + 1, t_tx, fields))

            segs = timeline(events, window, known_at=t_tx)
            current = [
                (r["id"], row_key(r["valid_from"], r["valid_to"], r["weight"], r["metadata"]))
                for r in rows if r["tx_to"] == MAX_TIME
            ]
            close_ids, inserts = diff_rows(current, segs)
            for r in rows:
                if r["id"] in close_ids:
                    r["tx_to"] = t_tx
            for s in inserts:
                v_counter += 1
                rows.append({
                    "id": f"v{v_counter}", "valid_from": s.valid_from, "valid_to": s.valid_to,
                    "weight": s.weight, "metadata": s.metadata, "tx_from": t_tx, "tx_to": MAX_TIME,
                })

        # Sample all valid times and transaction times (plus +30min offsets)
        tx_points = sorted({e.tx_time for e in events} | {e.tx_time + timedelta(minutes=30) for e in events})
        v_points = sorted({base + timedelta(days=d) for d in range(-1, 32)})

        for k in tx_points:
            expected_segs = timeline(events, window, known_at=k)
            for v in v_points:
                expected = next((s for s in expected_segs if s.contains(v)), None)
                matched = [
                    r for r in rows
                    if is_visible(r["valid_from"], r["valid_to"], r["tx_from"], r["tx_to"], v, k)
                ]
                assert len(matched) <= 1
                if expected is None:
                    assert matched == []
                else:
                    assert len(matched) == 1
                    assert matched[0]["weight"] == expected.weight
                    assert matched[0]["metadata"] == expected.metadata
