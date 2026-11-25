# Temporal Dependency Search Engine

A high-performance **bitemporal directed graph database and search engine** designed for tracking, traversing, and analyzing complex dependency graphs over valid time (modeled real-world time) and transaction time (system recording time).

The engine pairs **MongoDB** as the authoritative event-sourced store with **Neo4j** as a low-latency graph projection, synchronizing mutations via a resilient **Transactional Outbox Pattern** with at-least-once delivery, idempotency, and dead-letter recovery.

---

## Architecture & Temporal Semantics

```
                     ┌──────────────────────────────────┐
                     │ Universal Importer / Archive ETL │
                     │  (ZIP, TAR, JSON, JSONL, CSV)   │
                     └────────────────┬─────────────────┘
                                      │
                                      ▼
               ┌──────────────────────────────────────────────┐
               │         MongoDB (Authoritative Store)        │
               │  - nodes, heads, versions                    │
               │  - events (append-only timeline)             │
               │  - outbox (transactional aggregate queue)    │
               └──────────────────────┬───────────────────────┘
                                      │
                        Transactional Outbox Worker
                         (idempotent batch replay)
                                      │
                                      ▼
               ┌──────────────────────────────────────────────┐
               │          Neo4j (Graph Projection)            │
               │  - (:Component {dataset, id, ...})           │
               │  - [:DEPENDS_ON {valid_from, valid_to, ...}] │
               │  - Tarjan SCC, BFS Shortest Path, Centrality │
               └──────────────────────────────────────────────┘
```

### Bitemporal Interval Semantics
All temporal intervals use strict **half-open $[start, end)$ intervals**:
$$\text{active}(t_V, t_T) \iff (valid\_from \le t_V < valid\_to) \land (tx\_from \le t_T < tx\_to)$$

- **Valid Time ($t_V$ / `valid_at` / `--at`):** When the dependency or component was factually true in the modeled domain.
- **Transaction Time ($t_T$ / `known_at`):** When the system became aware of the record. Supports historical point-in-time belief queries, retrospective corrections, and late-arriving events.
- **Referential Validity Clipping:** A relationship cannot be valid outside the lifetime of its endpoint nodes. The engine automatically clips dependency intervals to their endpoint windows:
  $$\text{window} = [\max(valid\_from_s, valid\_from_t),\; \min(valid\_to_s, valid\_to_t))$$

---

## Features

- **Universal Archive & Format Ingestion:** Seamless streaming ingestion of `.zip`, `.tar.gz`, `.json`, `.jsonl`, `.csv`, `.tsv`, `package.json`, and `requirements.txt` with automatic delimiter detection and schema inference.
- **Dataset Isolation:** Multi-tenancy support across MongoDB and Neo4j (`--dataset <name>`). Unrelated datasets never collide or contaminate analytics.
- **Shortest Path Search:** Unweighted BFS and weighted Dijkstra algorithms with relationship-type filtering and bitemporal constraints.
- **Dependency Traversal:** Direct and multi-hop queries (`outgoing`, `incoming`, `both`) with configurable depth and cycle avoidance policies (`ALLOW`, `WARN`, `REJECT`).
- **Blast Radius & Impact Analysis:** Upstream dependency tracking identifying directly and indirectly affected services/components when a node fails.
- **Temporal Cycle Detection:** Iterative Tarjan strongly connected components (SCC) algorithm that distinguishes non-trivial cycles ($size \ge 2$) from self-loops ($size = 1$), respecting half-open valid-time boundaries and transaction-time states.
- **Historical As-Of Subgraphs:** Point-in-time topological snapshots around any node at any historical valid time.

---

## Quick Start

### 1. Prerequisites
- **Python 3.11+**
- **Docker & Docker Compose** (for MongoDB and Neo4j)

### 2. Start Live Databases
```bash
docker compose up -d
```
This spins up:
- **MongoDB 7.0** on `localhost:27018`
- **Neo4j 5.26** on `localhost:7474` (HTTP) and `localhost:7687` (Bolt)

### 3. Setup Virtual Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4. Run Automated Test Suite
```bash
pytest -v tests/
```
All 61 comprehensive unit, integration, bitemporal cycle detection, and RAG ground-truth verification tests should pass.

---

## CLI Command Reference

The command-line interface provides full access to ETL, search, traversal, analytics, and time-travel features.

### Convert Files to Canonical JSON / JSONL
```bash
# Convert a ZIP or CSV file to canonical JSON
.venv/bin/python -m app.cli convert examples/sample_temporal_graph.zip -o output_graph.json

# Convert to JSONL for massive datasets
.venv/bin/python -m app.cli convert examples/from_zip.jsonl -o output.jsonl --format jsonl
```

### Ingestion & Dataset Management
```bash
# Ingest an archive into the default dataset
.venv/bin/python -m app.cli ingest --file examples/sample_temporal_graph.zip

# Ingest into an isolated dataset (replacing previous records)
.venv/bin/python -m app.cli ingest --file examples/research_rag_dataset.zip --dataset research --replace

# Ingest with historical transaction-time replay
ALLOW_TX_TIME_OVERRIDE=true .venv/bin/python -m app.cli ingest --file examples/research_rag_dataset.zip --dataset research --replace

# Ingest separate nodes and dependencies CSV files
.venv/bin/python -m app.cli ingest --nodes examples/nodes.csv --dependencies examples/dependencies.csv --dataset csv-data
```

### Shortest Path Search
```bash
# Shortest unweighted hop path
.venv/bin/python -m app.cli search --source frontend-app --target orders-db --dataset default

# Shortest weighted path (lowest latency / cost)
.venv/bin/python -m app.cli search --source frontend-app --target orders-db --weighted --dataset default

# Path active at a historical timestamp
.venv/bin/python -m app.cli search --source P_RAG --target C_TRANSFORMER --valid-at "2024-06-01T00:00:00Z" --dataset research
```

### Dependency Traversal
```bash
# Direct outgoing dependencies (what does order-service depend on?)
.venv/bin/python -m app.cli deps --node order-service --direction outgoing --dataset default

# Direct incoming dependencies (what depends on user-db?)
.venv/bin/python -m app.cli deps --node user-db --direction incoming --dataset default

# Multi-hop dependencies up to 3 hops deep
.venv/bin/python -m app.cli deps --node frontend-app --direction outgoing --depth 3 --dataset default
```

### Blast Radius / Impact Analysis
```bash
# Find all upstream services impacted by a failure of 'user-db'
.venv/bin/python -m app.cli impact --node user-db --dataset default

# Historical blast radius as of 2020-06-01
.venv/bin/python -m app.cli impact --node P_BERT --valid-at "2020-06-01T00:00:00Z" --dataset research
```

### Temporal Cycle Detection
```bash
# Detect cycles in the active topology
.venv/bin/python -m app.cli cycles --dataset cycle-test

# Detect cycles at a specific historical valid time
.venv/bin/python -m app.cli cycles --dataset cycle-test --at "2024-06-01T00:00:00Z"

# Verify cycle disappearance after validity expiration
.venv/bin/python -m app.cli cycles --dataset cycle-test --at "2024-08-01T00:00:00Z"

# Bitemporal belief query: cycles known as of a transaction timestamp
.venv/bin/python -m app.cli cycles --dataset cycle-test --at "2024-06-01T00:00:00Z" --known-at "2024-01-01T00:00:00Z"
```

### Historical Subgraph Snapshot (`as-of`)
```bash
# Point-in-time subgraph active at 2024-06-01 around order-service
.venv/bin/python -m app.cli as-of --node order-service --at "2024-06-01T00:00:00Z" --dataset default
```

---

## REST API & Interactive Documentation

Start the FastAPI application:
```bash
.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

- **Interactive Swagger UI:** [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc Documentation:** [http://localhost:8000/redoc](http://localhost:8000/redoc)
- **Liveness & Readiness Health:** `curl http://localhost:8000/health`

### Key Endpoints
| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/ingest/batch` | Bulk ingestion of nodes, dependencies, and temporal events |
| `POST` | `/api/v1/search/shortest-path` | Unweighted BFS or Dijkstra shortest path search |
| `POST` | `/api/v1/search/direct` | Direct incoming/outgoing/bidirectional dependency traversal |
| `POST` | `/api/v1/search/multi-hop` | Deep multi-hop graph exploration |
| `GET` | `/api/v1/analytics/cycles` | Strongly connected components (SCC) & cycle detection |
| `POST` | `/api/v1/analytics/impact` | Blast-radius and upstream dependency impact analysis |
| `GET` | `/api/v1/analytics/centrality` | Degree, PageRank, and betweenness centrality metrics |
| `GET` | `/api/v1/temporal/as-of` | Historical graph snapshot reconstruction |
| `POST` | `/api/v1/temporal/diff` | Valid-time and belief-state differential comparisons |

---

## Configuration

Configuration is managed via environment variables or a `.env` file:

| Variable | Default | Description |
|---|---|---|
| `APP_ENV` | `development` | Runtime environment (`development`, `production`, `test`) |
| `MONGO_URI` | `mongodb://localhost:27018/?replicaSet=rs0` | MongoDB connection URI |
| `MONGO_DB` | `temporal_deps` | Database name |
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j Bolt endpoint |
| `NEO4J_USER` | `neo4j` | Neo4j username |
| `NEO4J_PASSWORD` | `temporal_password` | Neo4j password |
| `CYCLE_POLICY` | `WARN` | Policy on cycles during ingestion (`ALLOW`, `WARN`, `REJECT`) |
| `MAX_TRAVERSAL_DEPTH` | `20` | Maximum traversal depth limit |
| `ALLOW_TX_TIME_OVERRIDE` | `false` | Enable historical replay of `recorded_at` timestamps |
| `SYNC_WORKER_ENABLED` | `true` | Run background transactional outbox worker |

---

## Project Structure

```
.
├── app/
│   ├── api/                  # FastAPI routers, schemas, dependencies, error handlers
│   ├── benchmarking/         # Performance benchmarking utilities
│   ├── domain/               # Domain models, bitemporal timeline logic, interval math
│   ├── graph/                # Neo4j Cypher queries, shortest path, traversal, Tarjan SCC
│   ├── ingestion/            # Universal loader, importer, transactional sync outbox, validator
│   ├── repositories/         # MongoDB and Neo4j persistence layers
│   ├── services/             # Core service orchestrators (search, analytics, temporal, sync)
│   ├── cli.py                # Command-line interface entry point
│   ├── config.py             # Pydantic Settings configuration
│   ├── container.py          # Dependency injection container
│   ├── db.py                 # Live MongoDB and Neo4j connection lifecycle
│   └── main.py               # FastAPI application factory
├── datasets/                 # Sample datasets (e.g. cycle-test)
├── docker-compose.yml        # Multi-container MongoDB ReplicaSet + Neo4j compose file
├── Dockerfile                # Production container build
├── examples/                 # Sample graphs (ZIP archives, CSV, JSON, RAG benchmarks)
├── pytest.ini                # Pytest configuration
├── requirements.txt          # Python project dependencies
└── tests/
    ├── integration/          # Live database integration, sync, cycle & RAG tests
    └── unit/                 # Unit tests for conversion, cypher, temporal math, validators
```

---

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
