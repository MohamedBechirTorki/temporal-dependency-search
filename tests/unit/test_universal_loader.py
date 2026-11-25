"""Unit tests for the Universal streaming file loader."""
import io
import json
import zipfile
from pathlib import Path

from app.ingestion.universal_loader import (
    UniversalDatasetLoader,
    map_record_to_entity,
    stream_csv_table,
    stream_json_array,
    stream_json_key_array,
    stream_jsonl,
)


def test_schema_mapping_heuristics():
    # Various column synonyms for dependencies
    assert map_record_to_entity({"from": "app", "to": "db"}) == (
        "dependency", {"source": "app", "target": "db", "type": "depends_on"}
    )
    assert map_record_to_entity({"caller": "service_a", "callee": "service_b", "relationship": "calls", "latency": 15.5}) == (
        "dependency", {"source": "service_a", "target": "service_b", "type": "calls", "weight": 15.5}
    )
    assert map_record_to_entity({"parent": "node1", "child": "node2", "weight": "2.5"}) == (
        "dependency", {"source": "node1", "target": "node2", "type": "depends_on", "weight": 2.5}
    )

    # Node detection
    assert map_record_to_entity({"node_id": "srv1", "name": "Server 1", "type": "host"}) == (
        "node", {"id": "srv1", "name": "Server 1", "type": "host"}
    )


def test_streaming_json_array():
    sample = io.StringIO('[{"id": "a"}, {"id": "b"}, {"id": "c"}]')
    items = list(stream_json_array(sample, chunk_size=5))
    assert items == [{"id": "a"}, {"id": "b"}, {"id": "c"}]


def test_streaming_json_key_array():
    sample = io.StringIO('{"info": "meta", "nodes": [{"id": "n1"}], "dependencies": [{"source": "n1", "target": "n2"}]}')
    nodes = list(stream_json_key_array(sample, "nodes", chunk_size=8))
    assert nodes == [{"id": "n1"}]
    sample.seek(0)
    deps = list(stream_json_key_array(sample, "dependencies", chunk_size=8))
    assert deps == [{"source": "n1", "target": "n2"}]


def test_zip_streaming_and_auto_conversion(tmp_path: Path):
    zip_p = tmp_path / "dataset.zip"
    with zipfile.ZipFile(zip_p, "w") as z:
        z.writestr("network/nodes.csv", "id,name,type\nweb,Web App,service\nsql,SQL DB,database\n")
        z.writestr("network/edges.csv", "src,dst,kind\nweb,sql,connects_to\n")

    loader = UniversalDatasetLoader(zip_p)
    out_json = tmp_path / "converted.json"
    rep = loader.convert_to_json_file(out_json, format_type="json")

    assert rep["nodes"] == 2
    assert rep["dependencies"] == 1

    with out_json.open("r") as f:
        data = json.load(f)
    assert len(data["nodes"]) == 2
    assert len(data["dependencies"]) == 1
    assert data["dependencies"][0]["source"] == "web"
    assert data["dependencies"][0]["target"] == "sql"
