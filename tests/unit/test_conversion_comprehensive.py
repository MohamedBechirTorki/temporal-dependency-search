"""Comprehensive conversion tests according to task specifications."""
import io
import json
import zipfile
from pathlib import Path

import pytest

from app.domain.errors import InvalidRequestError
from app.ingestion.universal_loader import UniversalDatasetLoader


def test_convert_valid_json(tmp_path: Path):
    p = tmp_path / "valid.json"
    p.write_text(json.dumps({
        "nodes": [{"id": "n1", "name": "Node 1", "type": "service"}, {"id": "n2", "name": "Node 2", "type": "database"}],
        "edges": [{"id": "e1", "source": "n1", "target": "n2", "type": "calls"}]
    }))
    out = tmp_path / "out.json"
    loader = UniversalDatasetLoader(p)
    rep = loader.convert_to_json_file(out, format_type="json")
    assert rep["nodes"] == 2
    assert rep["dependencies"] == 1
    with out.open() as f:
        data = json.load(f)
    assert len(data["nodes"]) == 2
    assert len(data["dependencies"]) == 1


def test_convert_valid_jsonl(tmp_path: Path):
    p = tmp_path / "valid.jsonl"
    p.write_text('{"id": "n1", "name": "Node 1"}\n{"id": "n2", "name": "Node 2"}\n{"source": "n1", "target": "n2", "type": "calls"}\n')
    out = tmp_path / "out.jsonl"
    loader = UniversalDatasetLoader(p)
    rep = loader.convert_to_json_file(out, format_type="jsonl")
    assert rep["nodes"] == 2
    assert rep["dependencies"] == 1
    with out.open() as f:
        lines = [json.loads(line) for line in f if line.strip()]
    assert len(lines) == 3


def test_convert_zip_only_json(tmp_path: Path):
    zip_p = tmp_path / "only_json.zip"
    with zipfile.ZipFile(zip_p, "w") as z:
        z.writestr("graph.json", json.dumps({
            "nodes": [{"id": "a"}, {"id": "b"}],
            "dependencies": [{"source": "a", "target": "b"}]
        }))
    loader = UniversalDatasetLoader(zip_p)
    out = tmp_path / "out.json"
    rep = loader.convert_to_json_file(out)
    assert rep["nodes"] == 2
    assert rep["dependencies"] == 1
    assert rep["archive_report"]["files_accepted"] == ["graph.json"]


def test_convert_zip_json_and_readme(tmp_path: Path):
    zip_p = tmp_path / "with_readme.zip"
    with zipfile.ZipFile(zip_p, "w") as z:
        z.writestr("graph.json", json.dumps({
            "nodes": [{"id": "a"}, {"id": "b"}],
            "dependencies": [{"source": "a", "target": "b"}]
        }))
        z.writestr("README.txt", "This is documentation.\nNode 1 -> Node 2 should be ignored.\n")
        z.writestr("license.md", "# MIT License\n")
    loader = UniversalDatasetLoader(zip_p)
    out = tmp_path / "out.json"
    rep = loader.convert_to_json_file(out)
    assert rep["nodes"] == 2
    assert rep["dependencies"] == 1
    assert rep["archive_report"]["files_accepted"] == ["graph.json"]
    assert "README.txt" in rep["archive_report"]["files_skipped"]
    assert "license.md" in rep["archive_report"]["files_skipped"]


def test_convert_zip_multiple_supported_files(tmp_path: Path):
    zip_p = tmp_path / "multi.zip"
    with zipfile.ZipFile(zip_p, "w") as z:
        z.writestr("nodes.csv", "id,name,type\nx,X,srv\ny,Y,srv\n")
        z.writestr("edges.csv", "source,target,type\nx,y,calls\n")
    loader = UniversalDatasetLoader(zip_p)
    out = tmp_path / "out.json"
    rep = loader.convert_to_json_file(out)
    assert rep["nodes"] == 2
    assert rep["dependencies"] == 1
    assert set(rep["archive_report"]["files_accepted"]) == {"nodes.csv", "edges.csv"}


def test_convert_empty_archive(tmp_path: Path):
    zip_p = tmp_path / "empty.zip"
    with zipfile.ZipFile(zip_p, "w"):
        pass
    loader = UniversalDatasetLoader(zip_p)
    out = tmp_path / "out.json"
    with pytest.raises(InvalidRequestError, match="No supported graph data files found"):
        loader.convert_to_json_file(out)


def test_convert_unsupported_files(tmp_path: Path):
    zip_p = tmp_path / "unsupported.zip"
    with zipfile.ZipFile(zip_p, "w") as z:
        z.writestr("data.bin", b"\x00\x01\x02")
        z.writestr("doc.pdf", b"%PDF-1.4")
    loader = UniversalDatasetLoader(zip_p)
    out = tmp_path / "out.json"
    with pytest.raises(InvalidRequestError, match="No supported graph data files found"):
        loader.convert_to_json_file(out)


def test_convert_ambiguous_archive(tmp_path: Path):
    zip_p = tmp_path / "ambiguous.zip"
    with zipfile.ZipFile(zip_p, "w") as z:
        z.writestr("first_graph.json", '{"nodes": []}')
        z.writestr("second_graph.json", '{"nodes": []}')
    loader = UniversalDatasetLoader(zip_p)
    out = tmp_path / "out.json"
    with pytest.raises(InvalidRequestError, match="Ambiguous archive"):
        loader.convert_to_json_file(out)
