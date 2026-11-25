"""Shared Cypher pattern builders and Neo4j record converters."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Sequence

from app.domain.temporal import cypher_visibility
from app.domain.time import as_utc, end_to_api

Direction = Literal["outgoing", "incoming", "both"]


def qpp_hop_pattern(
    direction: Direction,
    *,
    has_rel_filter: bool = False,
    var: str = "r",
    valid_param: str = "valid_at",
    known_param: str = "known_at",
    rels_param: str = "relationships",
    dataset_param: str = "dataset",
) -> str:
    """Return a Neo4j Quantified Path Pattern single-hop atom with bitemporal filter."""
    preds = [cypher_visibility(var, valid_param, known_param)]
    if dataset_param:
        preds.append(f"(${dataset_param} IS NULL OR {var}.dataset = ${dataset_param} OR ({var}.dataset IS NULL AND ${dataset_param} = 'default'))")
    if has_rel_filter:
        preds.append(f"{var}.relationship IN ${rels_param}")
    where_clause = " AND ".join(preds)

    if direction == "outgoing":
        return f"(()-[{var}:DEPENDS_ON WHERE {where_clause}]->())"
    if direction == "incoming":
        return f"(()<-[{var}:DEPENDS_ON WHERE {where_clause}]-())"
    if direction == "both":
        return f"(()-[{var}:DEPENDS_ON WHERE {where_clause}]-())"
    raise ValueError(f"invalid direction {direction!r}")


def quantifier(min_hops: int = 1, max_hops: int | None = None) -> str:
    min_h = int(min_hops)
    if min_h < 1:
        raise ValueError("min_hops must be >= 1")
    if max_hops is None:
        return "+" if min_h == 1 else f"{{{min_h},}}"
    max_h = int(max_hops)
    if max_h < min_h:
        raise ValueError("max_hops must be >= min_hops")
    return f"{{{min_h}, {max_h}}}"


def edge_filter(
    *,
    has_rel_filter: bool = False,
    var: str = "r",
    valid_param: str = "valid_at",
    known_param: str = "known_at",
    rels_param: str = "relationships",
    dataset_param: str = "dataset",
) -> str:
    preds = [cypher_visibility(var, valid_param, known_param)]
    if dataset_param:
        preds.append(f"(${dataset_param} IS NULL OR {var}.dataset = ${dataset_param} OR ({var}.dataset IS NULL AND ${dataset_param} = 'default'))")
    if has_rel_filter:
        preds.append(f"{var}.relationship IN ${rels_param}")
    return " AND ".join(preds)


def normalize_rels(relationship_types: Sequence[str] | None) -> list[str] | None:
    if not relationship_types:
        return None
    # Preserve order while deduplicating
    return list(dict.fromkeys(relationship_types))


def format_node(props: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": props["id"],
        "name": props.get("name") or props["id"],
        "type": props.get("type") or "component",
        "deleted": bool(props.get("deleted", False)),
    }


def format_edge(props: dict[str, Any], source: str, target: str) -> dict[str, Any]:
    return {
        "dependency_id": props["dependency_id"],
        "version_id": props["version_id"],
        "source": source,
        "target": target,
        "relationship": props["relationship"],
        "weight": float(props["weight"]),
        "valid_from": as_utc(props["valid_from"]),
        "valid_to": end_to_api(props["valid_to"]),
        "recorded_at": as_utc(props["tx_from"]),
    }


def query_params(
    valid_at: datetime, known_at: datetime, rels: list[str] | None, dataset: str | None = None, **extra: Any
) -> dict[str, Any]:
    params: dict[str, Any] = {"valid_at": valid_at, "known_at": known_at, "dataset": dataset or "default", **extra}
    if rels:
        params["relationships"] = rels
    return params
