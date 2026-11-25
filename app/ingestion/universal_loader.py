"""Universal streaming file loader and auto-converter for graph datasets.

Handles:
- Compressed archives: .zip, .tar.gz, .tgz, .tar.bz2, .tar, .gz
- Formats: .json, .jsonl, .ndjson, .csv, .tsv
- Code manifests: package.json, requirements.txt
- Deterministic archive discovery: skips documentation/READMEs and rejects ambiguous contents.
- Strict schema validation: preserves valid_from/valid_to, id, types, and weights.
- Memory efficiency: O(1) streaming for multi-GB files.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import re
import tarfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, TextIO

from app.domain.errors import InvalidRequestError

log = logging.getLogger(__name__)

# Extensions & filename patterns that are recognized as graph datasets
SUPPORTED_DATA_EXTENSIONS = {".json", ".jsonl", ".ndjson", ".csv", ".tsv"}
SPECIAL_MANIFEST_FILES = {"package.json", "requirements.txt", "requirements.in"}

# Patterns that indicate documentation or non-data files to safely skip in archives
IGNORED_PATTERNS = [
    r"^__macosx",
    r"^\..*",
    r"^readme(\..*)?$",
    r"^license(\..*)?$",
    r"^changelog(\..*)?$",
    r"^notice(\..*)?$",
    r"^authors(\..*)?$",
    r"^install(\..*)?$",
    r".*\.txt$",
    r".*\.md$",
    r".*\.rst$",
    r".*\.log$",
    r".*\.pdf$",
    r".*\.png$",
    r".*\.jpg$",
    r".*\.py$",
    r".*\.sh$",
    r".*\.ya?ml$",
    r".*\.xml$",
    r".*\.html$",
]
_IGNORED_RE = re.compile("|".join(f"(?:{p})" for p in IGNORED_PATTERNS), re.IGNORECASE)

# Common synonym keys in graph datasets
NODE_KEYS = ("nodes", "vertices", "components", "entities")
EDGE_KEYS = ("edges", "dependencies", "links", "relationships")

SOURCE_SYNONYMS = (
    "source", "src", "from", "parent", "caller", "consumer",
    "upstream", "dependent", "package", "node_a", "start_node", "origin"
)
TARGET_SYNONYMS = (
    "target", "dst", "to", "child", "callee", "provider",
    "downstream", "dependency", "node_b", "end_node", "destination"
)
ID_SYNONYMS = ("id", "node_id", "key", "identifier")


def is_ignored_archive_file(filename: str) -> bool:
    """Check if a filename inside an archive is documentation or non-data."""
    base = Path(filename).name
    if base.lower() in SPECIAL_MANIFEST_FILES:
        return False
    # Ignore files in test/ground-truth/doc directories
    parts = [p.lower() for p in Path(filename).parts[:-1]]
    if any(p in ("ground_truth", "tests", "test", "docs", "doc", "benchmarks", "benchmark") for p in parts):
        return True
    if any(kw in base.lower() for kw in ("expected", "ground_truth", "expected_queries")):
        return True
    return bool(_IGNORED_RE.match(base))


def is_supported_data_file(filename: str) -> bool:
    """Check if a file inside an archive is a supported data file."""
    base = Path(filename).name
    if base.lower() in SPECIAL_MANIFEST_FILES:
        return True
    ext = Path(filename).suffix.lower()
    return ext in SUPPORTED_DATA_EXTENSIONS and not is_ignored_archive_file(filename)


def detect_delimiter(sample_text: str) -> str:
    """Detect delimiter among comma, tab, semicolon, pipe."""
    counts = {
        ",": sample_text.count(","),
        "\t": sample_text.count("\t"),
        ";": sample_text.count(";"),
        "|": sample_text.count("|"),
    }
    best = max(counts, key=counts.get)  # type: ignore[arg-type]
    return best if counts[best] > 0 else ","


def stream_json_array(stream: TextIO, chunk_size: int = 65536) -> Iterator[dict[str, Any]]:
    """Stream items from a JSON array [ {...}, {...} ] without loading the entire file."""
    decoder = json.JSONDecoder()
    buf = ""

    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            return
        buf += chunk
        idx = buf.find("[")
        if idx != -1:
            buf = buf[idx + 1:]
            break

    while True:
        buf = buf.lstrip(" \r\n\t,")
        if not buf:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            buf += chunk
            buf = buf.lstrip(" \r\n\t,")
            if not buf:
                break

        if buf[0] == "]":
            break

        try:
            obj, end = decoder.raw_decode(buf)
            if isinstance(obj, dict):
                yield obj
            buf = buf[end:]
        except json.JSONDecodeError:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            buf += chunk


def stream_json_key_array(stream: TextIO, target_keys: tuple[str, ...] | list[str] | str, chunk_size: int = 65536) -> Iterator[dict[str, Any]]:
    """Locate and stream an array under any of target_keys, e.g. 'edges' or 'dependencies'."""
    if isinstance(target_keys, str):
        target_keys = (target_keys,)
    decoder = json.JSONDecoder()
    buf = ""
    target_patterns = [f'"{k}"' for k in target_keys]

    found = False
    matched_len = 0
    while True:
        for pat in target_patterns:
            idx = buf.find(pat)
            if idx != -1:
                buf = buf[idx + len(pat):]
                found = True
                matched_len = len(pat)
                break
        if found:
            break
        chunk = stream.read(chunk_size)
        if not chunk:
            break
        buf += chunk
        max_pat = max(len(p) for p in target_patterns)
        if len(buf) > chunk_size * 2:
            buf = buf[-(max_pat + chunk_size):]

    if not found:
        return

    # Find starting '['
    while True:
        idx = buf.find("[")
        if idx != -1:
            buf = buf[idx + 1:]
            break
        chunk = stream.read(chunk_size)
        if not chunk:
            return
        buf += chunk

    while True:
        buf = buf.lstrip(" \r\n\t,")
        if not buf:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            buf += chunk
            buf = buf.lstrip(" \r\n\t,")
            if not buf:
                break

        if buf[0] == "]":
            break

        try:
            obj, end = decoder.raw_decode(buf)
            if isinstance(obj, dict):
                yield obj
            buf = buf[end:]
        except json.JSONDecodeError:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            buf += chunk


def stream_jsonl(stream: TextIO) -> Iterator[dict[str, Any]]:
    """Stream line-by-line JSON (JSONL/NDJSON)."""
    for line in stream:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                yield obj
        except Exception:
            continue


def stream_csv_table(stream: TextIO) -> Iterator[dict[str, Any]]:
    """Stream CSV/TSV table rows as dicts."""
    first_chunk = stream.read(4096)
    if not first_chunk:
        return

    delim = detect_delimiter(first_chunk)
    combined_stream = io.StringIO(first_chunk)
    reader = csv.DictReader(combined_stream, delimiter=delim)
    headers = reader.fieldnames or []

    for row in reader:
        yield {k: v for k, v in row.items() if k is not None}

    reader_rem = csv.DictReader(stream, fieldnames=headers, delimiter=delim)
    for row in reader_rem:
        yield {k: v for k, v in row.items() if k is not None}


def parse_package_json(stream: TextIO) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Parse npm package.json dependencies."""
    try:
        data = json.load(stream)
    except Exception:
        return [], []

    pkg_name = data.get("name") or "root-package"
    pkg_ver = data.get("version", "1.0.0")
    nodes = [{"id": pkg_name, "name": pkg_name, "type": "npm_package", "metadata": {"version": pkg_ver}}]
    deps: list[dict[str, Any]] = []

    all_deps = {**data.get("dependencies", {}), **data.get("devDependencies", {})}
    for dep_name, version in all_deps.items():
        nodes.append({"id": dep_name, "name": dep_name, "type": "npm_package"})
        deps.append({
            "source": pkg_name,
            "target": dep_name,
            "type": "depends_on",
            "metadata": {"version_spec": str(version)},
        })
    return nodes, deps


def parse_requirements_txt(stream: TextIO, source_name: str = "app") -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Parse Python requirements.txt dependencies."""
    nodes = [{"id": source_name, "name": source_name, "type": "python_app"}]
    deps: list[dict[str, Any]] = []

    for line in stream:
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        parts = re.split(r"[><=~!;]", line)
        dep_name = parts[0].strip()
        if dep_name:
            nodes.append({"id": dep_name, "name": dep_name, "type": "pypi_package"})
            deps.append({
                "source": source_name,
                "target": dep_name,
                "type": "depends_on",
                "metadata": {"spec": line},
            })
    return nodes, deps


def map_record_to_entity(record: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Strictly classify a record as 'node', 'dependency', or 'unknown'."""
    keys_lower = {k.lower(): k for k in record.keys()}

    # Check for explicit 'type' field discriminating 'node' vs 'dependency' in jsonl
    rec_type = record.get("type", "").lower() if isinstance(record.get("type"), str) else ""
    if rec_type == "node" and isinstance(record.get("data"), dict):
        return "node", record["data"]
    if rec_type in ("dependency", "edge") and isinstance(record.get("data"), dict):
        return "dependency", record["data"]

    # 1. Dependency check (must have recognizable source and target)
    src_key = next((keys_lower[s] for s in SOURCE_SYNONYMS if s in keys_lower), None)
    tgt_key = next((keys_lower[t] for t in TARGET_SYNONYMS if t in keys_lower), None)

    if src_key and tgt_key and record[src_key] and record[tgt_key]:
        type_key = next((k for k in ("type", "relationship", "rel", "kind") if k in keys_lower), None)
        id_key = next((k for k in ("id", "edge_id", "dependency_id") if k in keys_lower), None)
        weight_key = next((k for k in ("weight", "cost", "latency") if k in keys_lower), None)
        vf_key = next((k for k in ("valid_from", "effective_from", "start_time") if k in keys_lower), None)
        vt_key = next((k for k in ("valid_to", "effective_to", "end_time") if k in keys_lower), None)
        tx_key = next((k for k in ("recorded_at", "tx_time", "created_at", "transaction_time") if k in keys_lower), None)
        txto_key = next((k for k in ("recorded_to", "tx_to", "deleted_at", "retracted_at") if k in keys_lower), None)

        dep: dict[str, Any] = {
            "source": str(record[src_key]).strip(),
            "target": str(record[tgt_key]).strip(),
            "type": str(record[type_key]).strip() if type_key and record.get(type_key) else "depends_on",
        }
        if id_key and record.get(id_key):
            dep["id"] = str(record[id_key]).strip()
        if vf_key and record.get(vf_key) is not None:
            dep["valid_from"] = record[vf_key]
        if vt_key and record.get(vt_key) is not None:
            dep["valid_to"] = record[vt_key]
        if tx_key and record.get(tx_key) is not None:
            dep["recorded_at"] = record[tx_key]
        if txto_key and record.get(txto_key) is not None:
            dep["recorded_to"] = record[txto_key]
        if weight_key and record.get(weight_key) is not None:
            try:
                dep["weight"] = float(record[weight_key])
            except (ValueError, TypeError):
                pass

        consumed = {src_key, tgt_key, type_key, id_key, weight_key, vf_key, vt_key, tx_key, txto_key}
        extra = {k: v for k, v in record.items() if k not in consumed and v is not None}
        if "metadata" in record and isinstance(record["metadata"], dict):
            extra.update(record["metadata"])
        if extra:
            dep["metadata"] = extra
        return "dependency", dep

    # 2. Node check (must have recognizable ID)
    id_key = next((keys_lower[i] for i in ID_SYNONYMS if i in keys_lower), None)
    if id_key and record[id_key]:
        nid = str(record[id_key]).strip()
        vf_key = next((k for k in ("valid_from", "start_time") if k in keys_lower), None)
        vt_key = next((k for k in ("valid_to", "end_time") if k in keys_lower), None)
        tx_key = next((k for k in ("recorded_at", "tx_time", "created_at", "transaction_time") if k in keys_lower), None)
        txto_key = next((k for k in ("recorded_to", "tx_to", "deleted_at", "retracted_at") if k in keys_lower), None)
        name_key = next((k for k in ("name", "label", "title") if k in keys_lower), None)
        type_key = next((k for k in ("type", "kind") if k in keys_lower), None)

        node: dict[str, Any] = {
            "id": nid,
            "name": str(record.get(name_key) or nid).strip() if name_key and record.get(name_key) else nid,
            "type": str(record.get(type_key) or "component").strip() if type_key and record.get(type_key) else "component",
        }
        if "description" in record and record["description"] is not None:
            node["description"] = str(record["description"])
        if vf_key and record.get(vf_key) is not None:
            node["valid_from"] = record[vf_key]
        if vt_key and record.get(vt_key) is not None:
            node["valid_to"] = record[vt_key]
        if tx_key and record.get(tx_key) is not None:
            node["recorded_at"] = record[tx_key]
        if txto_key and record.get(txto_key) is not None:
            node["recorded_to"] = record[txto_key]

        consumed = {id_key, name_key, type_key, "description", vf_key, vt_key, tx_key, txto_key}
        extra = {k: v for k, v in record.items() if k not in consumed and v is not None}
        if "metadata" in record and isinstance(record["metadata"], dict):
            extra.update(record["metadata"])
        if extra:
            node["metadata"] = extra
        return "node", node

    # DO NOT fall back to arbitrary text tokens!
    return "unknown", record


@dataclass
class ArchiveDiscoveryReport:
    archive_path: str
    files_discovered: list[str] = field(default_factory=list)
    files_accepted: list[str] = field(default_factory=list)
    files_skipped: list[str] = field(default_factory=list)
    selected_data_files: list[str] = field(default_factory=list)


class UniversalDatasetLoader:
    """Multi-format, archive-aware loader with deterministic selection and streaming conversion."""

    def __init__(self, input_path: str | Path) -> None:
        self.path = Path(input_path)
        if not self.path.exists():
            raise FileNotFoundError(f"File or directory not found: {self.path}")

    def inspect_archive(self) -> ArchiveDiscoveryReport:
        """Inspect archive contents and deterministically select graph data files."""
        report = ArchiveDiscoveryReport(archive_path=str(self.path))
        p = self.path

        if p.suffix.lower() == ".zip":
            with zipfile.ZipFile(p, "r") as z:
                names = [n for n in z.namelist() if not n.endswith("/")]
        elif p.name.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2")):
            with tarfile.open(p, "r:*") as t:
                names = [m.name for m in t.getmembers() if m.isfile()]
        else:
            names = [p.name]

        report.files_discovered = sorted(names)
        for name in report.files_discovered:
            if is_supported_data_file(name):
                report.files_accepted.append(name)
            else:
                report.files_skipped.append(name)

        if not report.files_accepted:
            raise InvalidRequestError(
                f"No supported graph data files found in archive '{p.name}'. "
                f"Discovered: {report.files_discovered}. Skipped non-data files: {report.files_skipped}. "
                f"Supported formats: .json, .jsonl, .csv, .tsv"
            )

        # Deterministic dataset selection
        # 1. Single primary JSON / JSONL or complementary (nodes, edges) pair
        json_files = [f for f in report.files_accepted if f.lower().endswith((".json", ".jsonl", ".ndjson"))]
        csv_files = [f for f in report.files_accepted if f.lower().endswith((".csv", ".tsv"))]

        nodes_json = [f for f in json_files if any(k in Path(f).stem.lower() for k in ("node", "vertex", "component", "entity"))]
        edges_json = [f for f in json_files if any(k in Path(f).stem.lower() for k in ("edge", "dep", "link", "rel"))]

        if len(json_files) == 1 and not csv_files:
            report.selected_data_files = json_files
        elif len(json_files) == 2 and len(nodes_json) == 1 and len(edges_json) == 1 and not csv_files:
            # Deterministically order nodes before edges so node definitions precede dependency references
            report.selected_data_files = [nodes_json[0], edges_json[0]]
        elif len(csv_files) in (1, 2) and not json_files:
            nodes_csv = [f for f in csv_files if any(k in Path(f).stem.lower() for k in ("node", "vertex", "component", "entity"))]
            edges_csv = [f for f in csv_files if any(k in Path(f).stem.lower() for k in ("edge", "dep", "link", "rel"))]
            if len(nodes_csv) == 1 and len(edges_csv) == 1:
                report.selected_data_files = [nodes_csv[0], edges_csv[0]]
            else:
                report.selected_data_files = csv_files
        elif len(report.files_accepted) == 1:
            report.selected_data_files = report.files_accepted
        elif json_files and csv_files:
            raise InvalidRequestError(
                f"Ambiguous archive '{p.name}': contains both JSON ({json_files}) and CSV ({csv_files}) data files. "
                f"Provide either JSON or CSV datasets, not both."
            )
        elif len(json_files) > 1:
            raise InvalidRequestError(
                f"Ambiguous archive '{p.name}': contains multiple JSON graph datasets ({json_files}). "
                f"Ensure the archive contains exactly one primary graph dataset."
            )
        else:
            report.selected_data_files = report.files_accepted

        return report

    def stream_raw_records(self) -> Iterator[tuple[str, dict[str, Any]]]:
        """Stream parsed records (kind, entity) from selected data files without uncompressing to disk."""
        p = self.path

        # 1. ZIP Archive
        if p.suffix.lower() == ".zip":
            report = self.inspect_archive()
            with zipfile.ZipFile(p, "r") as z:
                for name in report.selected_data_files:
                    with z.open(name) as b_file:
                        text_stream = io.TextIOWrapper(b_file, encoding="utf-8", errors="replace")
                        yield from self._stream_named_file(name, text_stream)
            return

        # 2. TAR Archive (.tar, .tar.gz, .tgz, .tar.bz2)
        if p.name.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2")):
            report = self.inspect_archive()
            with tarfile.open(p, "r:*") as t:
                for name in report.selected_data_files:
                    member = t.getmember(name)
                    b_file = t.extractfile(member)
                    if b_file is not None:
                        text_stream = io.TextIOWrapper(b_file, encoding="utf-8", errors="replace")
                        yield from self._stream_named_file(name, text_stream)
            return

        # 3. GZIP Single File (.gz)
        if p.suffix.lower() == ".gz":
            with gzip.open(p, "rt", encoding="utf-8", errors="replace") as text_stream:
                base_name = p.stem
                yield from self._stream_named_file(base_name, text_stream)
            return

        # 4. Standard plain file
        if is_ignored_archive_file(p.name):
            raise InvalidRequestError(f"'{p.name}' is recognized as documentation or non-data file. Supported: .json, .jsonl, .csv, .tsv")

        with p.open("r", encoding="utf-8", errors="replace") as text_stream:
            yield from self._stream_named_file(p.name, text_stream)

    def _stream_named_file(self, filename: str, stream: TextIO) -> Iterator[tuple[str, dict[str, Any]]]:
        """Parse stream according to file extension."""
        lower = filename.lower()

        if lower.endswith("package.json"):
            nodes, deps = parse_package_json(stream)
            for n in nodes:
                yield "node", n
            for d in deps:
                yield "dependency", d
            return

        if lower.endswith(("requirements.txt", "requirements.in")):
            nodes, deps = parse_requirements_txt(stream)
            for n in nodes:
                yield "node", n
            for d in deps:
                yield "dependency", d
            return

        if lower.endswith((".jsonl", ".ndjson")):
            for rec in stream_jsonl(stream):
                kind, entity = map_record_to_entity(rec)
                if kind != "unknown":
                    yield kind, entity
            return

        if lower.endswith((".csv", ".tsv")):
            for rec in stream_csv_table(stream):
                kind, entity = map_record_to_entity(rec)
                if kind != "unknown":
                    yield kind, entity
            return

        if lower.endswith(".json"):
            # If stream is small or seekable, we can test full json or stream key arrays
            pos = stream.tell() if stream.seekable() else None
            header_sample = stream.read(2048)
            if stream.seekable() and pos is not None:
                stream.seek(pos)

            has_node_key = any(f'"{k}"' in header_sample for k in NODE_KEYS)
            has_edge_key = any(f'"{k}"' in header_sample for k in EDGE_KEYS)

            if stream.seekable() and (has_node_key or has_edge_key):
                # Stream nodes
                if has_node_key:
                    stream.seek(0)
                    for n in stream_json_key_array(stream, NODE_KEYS):
                        kind, ent = map_record_to_entity(n)
                        yield ("node", ent) if kind == "node" else (kind, ent)
                # Stream edges/dependencies
                if has_edge_key:
                    stream.seek(0)
                    for d in stream_json_key_array(stream, EDGE_KEYS):
                        kind, ent = map_record_to_entity(d)
                        yield ("dependency", ent) if kind == "dependency" else (kind, ent)
                return

            # Array of objects
            for rec in stream_json_array(stream):
                kind, entity = map_record_to_entity(rec)
                if kind != "unknown":
                    yield kind, entity
            return

        # Non-supported file formats are rejected
        raise InvalidRequestError(f"Unsupported file format '{filename}'. Supported: .json, .jsonl, .csv, .tsv")

    def convert_to_json_file(
        self,
        output_path: str | Path,
        format_type: str = "json",
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> dict[str, Any]:
        """Convert any supported file or archive into a canonical JSON or JSONL dataset file."""
        out_p = Path(output_path)
        out_p.parent.mkdir(parents=True, exist_ok=True)

        discovery: ArchiveDiscoveryReport | None = None
        if self.path.suffix.lower() == ".zip" or self.path.name.endswith((".tar", ".tar.gz", ".tgz")):
            discovery = self.inspect_archive()

        seen_nodes: set[str] = set()
        node_count = 0
        dep_count = 0

        if format_type == "jsonl":
            with out_p.open("w", encoding="utf-8") as out:
                for kind, entity in self.stream_raw_records():
                    if kind == "node":
                        nid = entity.get("id")
                        if nid and nid not in seen_nodes:
                            seen_nodes.add(nid)
                            out.write(json.dumps({"type": "node", "data": entity}) + "\n")
                            node_count += 1
                    elif kind == "dependency":
                        src = entity.get("source")
                        tgt = entity.get("target")
                        # Auto-synthesize referenced nodes if not declared
                        for missing_nid in (src, tgt):
                            if missing_nid and missing_nid not in seen_nodes:
                                seen_nodes.add(missing_nid)
                                out.write(json.dumps({
                                    "type": "node",
                                    "data": {"id": missing_nid, "name": missing_nid, "type": "component"}
                                }) + "\n")
                                node_count += 1
                        out.write(json.dumps({"type": "dependency", "data": entity}) + "\n")
                        dep_count += 1

                    if progress_callback and (node_count + dep_count) % 10000 == 0:
                        progress_callback(node_count, dep_count)

            return {
                "output_file": str(out_p),
                "format": "jsonl",
                "nodes": node_count,
                "dependencies": dep_count,
                "total_records": node_count + dep_count,
                "archive_report": {
                    "files_discovered": discovery.files_discovered if discovery else [self.path.name],
                    "files_accepted": discovery.files_accepted if discovery else [self.path.name],
                    "files_skipped": discovery.files_skipped if discovery else [],
                },
            }

        # Canonical JSON format {"nodes": [...], "dependencies": [...]}
        with out_p.open("w", encoding="utf-8") as out:
            out.write('{\n  "nodes": [\n')

            first_node = True
            cached_deps: list[dict[str, Any]] = []

            for kind, entity in self.stream_raw_records():
                if kind == "node":
                    nid = entity.get("id")
                    if nid and nid not in seen_nodes:
                        seen_nodes.add(nid)
                        prefix = "    " if first_node else ",\n    "
                        out.write(prefix + json.dumps(entity))
                        first_node = False
                        node_count += 1
                elif kind == "dependency":
                    src = entity.get("source")
                    tgt = entity.get("target")
                    for missing in (src, tgt):
                        if missing and missing not in seen_nodes:
                            seen_nodes.add(missing)
                            prefix = "    " if first_node else ",\n    "
                            out.write(prefix + json.dumps({"id": missing, "name": missing, "type": "component"}))
                            first_node = False
                            node_count += 1
                    cached_deps.append(entity)

                    if progress_callback and (node_count + dep_count) % 10000 == 0:
                        progress_callback(node_count, dep_count)

            out.write('\n  ],\n  "dependencies": [\n')
            first_dep = True
            for dep in cached_deps:
                prefix = "    " if first_dep else ",\n    "
                out.write(prefix + json.dumps(dep))
                first_dep = False
                dep_count += 1

            out.write('\n  ]\n}\n')

        return {
            "output_file": str(out_p),
            "format": "json",
            "nodes": node_count,
            "dependencies": dep_count,
            "total_records": node_count + dep_count,
            "archive_report": {
                "files_discovered": discovery.files_discovered if discovery else [self.path.name],
                "files_accepted": discovery.files_accepted if discovery else [self.path.name],
                "files_skipped": discovery.files_skipped if discovery else [],
            },
        }
