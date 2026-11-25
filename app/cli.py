"""Command-line interface for the Temporal Dependency Search Engine.

Usage:
    # 1. Ingest files (JSON, CSV, JSONL)
    python -m app.cli ingest --file sample.json
    python -m app.cli ingest --nodes nodes.csv --dependencies deps.csv

    # 2. Search dependencies / paths
    python -m app.cli search --source web-app --target user-db
    python -m app.cli deps --node auth-service --direction outgoing --depth 2
    python -m app.cli impact --node user-db
    python -m app.cli cycles
    python -m app.cli as-of --at "2024-06-01T00:00:00Z"
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from app.api.schemas.analytics import ImpactRequest
from app.api.schemas.search import (
    DirectSearchRequest,
    MultiHopSearchRequest,
    ShortestPathRequest,
)
from app.api.schemas.temporal import AsOfRequest
from app.config import get_settings
from app.container import Container
from app.ingestion.universal_loader import UniversalDatasetLoader
from app.ingestion.validator import parse_optional_utc


def _print_json(data: Any) -> None:
    print(json.dumps(data, indent=2, default=str))


def cmd_convert(args: argparse.Namespace) -> None:
    p = Path(args.input)
    if not p.exists():
        print(f"Error: input file '{p}' not found", file=sys.stderr)
        sys.exit(1)
    out = Path(args.output or f"{p.stem}_converted.{args.format}")
    print(f"Converting '{p}' -> '{out}' (format={args.format})...")

    def progress(n: int, d: int) -> None:
        print(f"  Processed {n} nodes, {d} dependencies...", end="\r", flush=True)

    loader = UniversalDatasetLoader(p)
    res = loader.convert_to_json_file(out, format_type=args.format, progress_callback=progress)
    print()
    print("Conversion complete!")
    _print_json(res)


def cmd_ingest(args: argparse.Namespace, c: Container) -> None:
    dataset = getattr(args, "dataset", "default") or "default"
    replace = getattr(args, "replace", False)

    if args.file:
        p = Path(args.file)
        if not p.exists():
            print(f"Error: file '{p}' not found", file=sys.stderr)
            sys.exit(1)

        print(f"Streaming and ingesting '{p}' (dataset={dataset}, replace={replace})...")
        metrics = c.importer.ingest_universal(
            p,
            chunk_size=args.chunk_size,
            validate=args.validate,
            sync=True,
            dataset=dataset,
            replace=replace,
        )
        _print_json(metrics.to_dict())
        return

    if not args.nodes and not args.dependencies and not args.events:
        print("Error: Specify either --file <file_or_zip> or --nodes/--dependencies files.", file=sys.stderr)
        sys.exit(1)

    print(f"Streaming files into engine (dataset={dataset}, replace={replace})...")
    if replace:
        c.repo.delete_dataset(dataset)
        c.sync.neo.delete_dataset(dataset)

    metrics = c.importer.ingest_files(
        nodes_path=args.nodes,
        dependencies_path=args.dependencies,
        events_path=args.events,
        chunk_size=args.chunk_size,
        validate=args.validate,
        sync=True,
        dataset=dataset,
    )
    _print_json(metrics.to_dict())


def cmd_search(args: argparse.Namespace, c: Container) -> None:
    dataset = getattr(args, "dataset", "default") or "default"
    print(f"Finding shortest path from '{args.source}' to '{args.target}' (dataset={dataset})...")
    req = ShortestPathRequest(
        source_id=args.source,
        target_id=args.target,
        weighted=args.weighted,
        valid_at=parse_optional_utc(args.valid_at) if args.valid_at else None,
        known_at=parse_optional_utc(args.known_at) if args.known_at else None,
        dataset=dataset,
    )
    res = c.search.shortest_path(req)
    _print_json(res.model_dump())


def cmd_deps(args: argparse.Namespace, c: Container) -> None:
    dataset = getattr(args, "dataset", "default") or "default"
    if args.depth == 1:
        print(f"Direct {args.direction} dependencies for '{args.node}' (dataset={dataset})...")
        req = DirectSearchRequest(
            node_id=args.node,
            direction=args.direction,
            valid_at=parse_optional_utc(args.valid_at) if args.valid_at else None,
            known_at=parse_optional_utc(args.known_at) if args.known_at else None,
            dataset=dataset,
        )
        res = c.search.direct(req)
        _print_json(res.model_dump())
    else:
        print(f"Multi-hop (depth={args.depth}) {args.direction} dependencies for '{args.node}' (dataset={dataset})...")
        req = MultiHopSearchRequest(
            node_id=args.node,
            direction=args.direction,
            max_depth=args.depth,
            include_paths=True,
            valid_at=parse_optional_utc(args.valid_at) if args.valid_at else None,
            known_at=parse_optional_utc(args.known_at) if args.known_at else None,
            dataset=dataset,
        )
        res = c.search.multi_hop(req)
        _print_json(res.model_dump())


def cmd_impact(args: argparse.Namespace, c: Container) -> None:
    dataset = getattr(args, "dataset", "default") or "default"
    print(f"Calculating blast-radius / impact analysis for node '{args.node}' (dataset={dataset})...")
    req = ImpactRequest(
        node_id=args.node,
        max_depth=args.depth,
        valid_at=parse_optional_utc(args.valid_at) if args.valid_at else None,
        known_at=parse_optional_utc(args.known_at) if args.known_at else None,
        dataset=dataset,
    )
    res = c.analytics.impact(req)
    _print_json(res.model_dump())


def cmd_cycles(args: argparse.Namespace, c: Container) -> None:
    dataset = getattr(args, "dataset", "default") or "default"
    at = getattr(args, "at", None) or getattr(args, "valid_at", None)
    print(f"Detecting cycles in active topology (dataset={dataset})...")
    res = c.analytics.cycles(
        valid_at=parse_optional_utc(at) if at else None,
        known_at=parse_optional_utc(args.known_at) if args.known_at else None,
        max_cycle_length=args.depth,
        dataset=dataset,
    )
    _print_json(res.model_dump())


def cmd_as_of(args: argparse.Namespace, c: Container) -> None:
    dataset = getattr(args, "dataset", "default") or "default"
    print(f"Time-travel query: graph state as of valid_at={args.at} (dataset={dataset})...")
    req = AsOfRequest(
        valid_at=parse_optional_utc(args.at) if args.at else None,
        node_id=args.node,
        max_depth=args.depth,
        dataset=dataset,
    )
    res = c.temporal.as_of(req)
    _print_json(res.model_dump())


def main() -> None:
    parser = argparse.ArgumentParser(description="Temporal Dependency Search CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Convert
    p_conv = subparsers.add_parser("convert", help="Auto-convert any file/archive into canonical JSON or JSONL")
    p_conv.add_argument("input", help="Path to input file (.zip, .tar.gz, .csv, .tsv, .json, .txt, etc.)")
    p_conv.add_argument("--output", "-o", help="Path for output JSON file (default: <input>_converted.json)")
    p_conv.add_argument("--format", choices=["json", "jsonl"], default="json", help="Output format: json (default) or jsonl for massive datasets")

    # Ingest
    p_ingest = subparsers.add_parser("ingest", help="Ingest graph data from files or archives")
    p_ingest.add_argument("--file", help="Path to any file or archive (.zip, .tar.gz, .csv, .json, .jsonl)")
    p_ingest.add_argument("--nodes", help="Path to nodes file (.csv or .jsonl)")
    p_ingest.add_argument("--dependencies", help="Path to dependencies file (.csv or .jsonl)")
    p_ingest.add_argument("--events", help="Path to temporal events file (.csv or .jsonl)")
    p_ingest.add_argument("--chunk-size", type=int, default=1000, help="Batch chunk size for streaming (default: 1000)")
    p_ingest.add_argument("--validate", action="store_true", help="Perform pre-validation checks")
    p_ingest.add_argument("--dataset", default="default", help="Dataset name for graph isolation (default: default)")
    p_ingest.add_argument("--replace", action="store_true", help="Cleanly replace existing dataset before importing")

    # Search shortest path
    p_search = subparsers.add_parser("search", help="Find path between two nodes")
    p_search.add_argument("--source", required=True, help="Source node id")
    p_search.add_argument("--target", required=True, help="Target node id")
    p_search.add_argument("--weighted", action="store_true", help="Use weighted Dijkstra")
    p_search.add_argument("--valid-at", help="Historical valid timestamp (ISO-8601)")
    p_search.add_argument("--known-at", help="Transaction/belief timestamp (ISO-8601)")
    p_search.add_argument("--dataset", default="default", help="Dataset name for graph isolation (default: default)")

    # Dependencies / Traversal
    p_deps = subparsers.add_parser("deps", help="Traverse dependencies for a node")
    p_deps.add_argument("--node", required=True, help="Target node id")
    p_deps.add_argument("--direction", choices=["outgoing", "incoming", "both"], default="outgoing")
    p_deps.add_argument("--depth", type=int, default=1, help="Hop depth (1 = direct)")
    p_deps.add_argument("--valid-at", help="Historical valid timestamp (ISO-8601)")
    p_deps.add_argument("--known-at", help="Transaction/belief timestamp (ISO-8601)")
    p_deps.add_argument("--dataset", default="default", help="Dataset name for graph isolation (default: default)")

    # Impact analysis
    p_impact = subparsers.add_parser("impact", help="Blast-radius impact analysis")
    p_impact.add_argument("--node", required=True, help="Root node id")
    p_impact.add_argument("--direction", choices=["dependents", "dependencies"], default="dependents")
    p_impact.add_argument("--depth", type=int, default=10, help="Max depth")
    p_impact.add_argument("--valid-at", help="Historical valid timestamp (ISO-8601)")
    p_impact.add_argument("--known-at", help="Transaction/belief timestamp (ISO-8601)")
    p_impact.add_argument("--dataset", default="default", help="Dataset name for graph isolation (default: default)")

    # Cycles
    p_cycles = subparsers.add_parser("cycles", help="Detect cycles in the graph")
    p_cycles.add_argument("--depth", type=int, default=15, help="Max cycle search depth")
    p_cycles.add_argument("--valid-at", help="Historical valid timestamp (ISO-8601)")
    p_cycles.add_argument("--at", help="Alias for --valid-at (ISO-8601)")
    p_cycles.add_argument("--known-at", help="Transaction/belief timestamp (ISO-8601)")
    p_cycles.add_argument("--dataset", default="default", help="Dataset name for graph isolation (default: default)")

    # As-of time travel
    p_as_of = subparsers.add_parser("as-of", help="Point-in-time snapshot query")
    p_as_of.add_argument("--at", required=True, help="Historical valid timestamp (ISO-8601)")
    p_as_of.add_argument("--node", help="Optional root node to restrict subgraph")
    p_as_of.add_argument("--depth", type=int, default=2, help="Depth around root node")
    p_as_of.add_argument("--dataset", default="default", help="Dataset name for graph isolation (default: default)")

    args = parser.parse_args()

    if args.command == "convert":
        cmd_convert(args)
        return

    settings = get_settings()
    c = Container.build(settings)
    try:
        if args.command == "ingest":
            cmd_ingest(args, c)
        elif args.command == "search":
            cmd_search(args, c)
        elif args.command == "deps":
            cmd_deps(args, c)
        elif args.command == "impact":
            cmd_impact(args, c)
        elif args.command == "cycles":
            cmd_cycles(args, c)
        elif args.command == "as-of":
            cmd_as_of(args, c)
    finally:
        c.dbs.close()


if __name__ == "__main__":
    main()
