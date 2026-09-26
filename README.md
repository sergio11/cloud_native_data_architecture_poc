# Modern Data Architecture POC
### Data Mesh · Data Fabric · Data Products · Lambda/Kappa · Medallion · Serving Layer

[![100% Open Source](https://img.shields.io/badge/Open%20Source-100%25-brightgreen.svg?style=flat-square)](#)
[![Kubernetes / kind](https://img.shields.io/badge/Runtime-Kubernetes%20%2F%20kind-blue.svg?style=flat-square)](#)
[![Apache Iceberg](https://img.shields.io/badge/Lakehouse-Apache%20Iceberg%20v2-blueviolet.svg?style=flat-square)](#)
[![Apache Spark](https://img.shields.io/badge/Batch-Apache%20Spark%203.5-E25A1C.svg?style=flat-square)](#)
[![Apache Flink](https://img.shields.io/badge/Streaming-Apache%20Flink%201.18-E6526F.svg?style=flat-square)](#)
[![Trino](https://img.shields.io/badge/Serving-Trino%20SQL-DD00A1.svg?style=flat-square)](#)
[![OpenLineage](https://img.shields.io/badge/Metadata-OpenLineage%20%2F%20Marquez-00B4D8.svg?style=flat-square)](#)
[![Kyverno](https://img.shields.io/badge/Governance-Kyverno%20Policies-blue.svg?style=flat-square)](#)
[![Dagster](https://img.shields.io/badge/Orchestrator-Dagster%20SDA-4F46E5.svg?style=flat-square)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](LICENSE)

---

## Abstract & Project Goals

The **Modern Data Architecture Proof of Concept (POC)** is a reference implementation designed to synthesize the five most transformative paradigms of contemporary enterprise data engineering into a single, cohesive, reproducible, and verifiable open-source platform:
1. **Data Mesh**: Domain-oriented decentralized data ownership and self-serve infrastructure.
2. **Data Fabric**: Active metadata management, automated pipeline lineage harvesting, and dynamic quality enforcement.
3. **Data Products & Declarative Contracts**: Analytical datasets packaged as versioned products bounded by formal contracts adhering to the Bitol Open Data Contract Standard (ODCS).
4. **Medallion Lakehouse**: Multi-tier curation (Bronze raw, Silver conformed, Gold aggregated) with ACID semantics, schema evolution, and time travel — implemented locally over embedded SQLite adapters, targeting Apache Iceberg on MinIO (Polaris catalog) in the K8s profile.
5. **Hybrid Lambda / Kappa Processing**: Full-recomputation batch layer for historical depth combined with event-log stream processing (real 5-minute tumbling windows + full replay), unified at query time by a single anti-join SQL.

> **Two execution profiles** share the same hexagonal core (`src/`): the **local profile** (default — pure Python + SQLite, verified by `rake demo` / `rake test`) and the **K8s profile** (`infra/helm`, `infra/profiles` — deployment target). See [`docs/architecture/overview.md`](docs/architecture/overview.md) and the honest RF-by-RF report [`docs/traceability-status.md`](docs/traceability-status.md).

### Strategic Objectives
- **Pedagogical Reference**: Provide a clear, hands-on demonstration showing how theoretical data architecture concepts translate into production-grade open-source code.
- **Technical Architecture Portfolio**: Offer an end-to-end, documented platform featuring automated verification, strict schema compatibility rules, GDPR PII pseudonymization, and comprehensive Architecture Decision Records (ADRs).
- **Resource Efficiency**: Execute the complete platform locally on standard developer hardware constrained to an **8 GB RAM memory budget** through orchestrated operational profiles.

---

## Architecture Overview: The 3 Planes

```
+========================================================================================+
|                                1. ORGANIZATION PLANE                                   |
|                                                                                        |
|   +-----------------------+     +-----------------------+    +-----------------------+ |
|   | Domain: Sales         |     | Domain: Marketing     |    | Domain: Finance       | |
|   | - Product: customers  |     | - Product: campaigns  |    | - Product: budgets    | |
|   | - Product: orders     |     | - Product: leads      |    | - Product: budgets    | |
|   |                       |     |                       |    | - Product: closings   | |
|   | - Contract: v1.2.0    |     | - Contract: v1.0.0    |    | - Contract: v1.0.0    | |
|   +-----------------------+     +-----------------------+    +-----------------------+ |
|               \                             |                            /             |
|                +----------------------------+---------------------------+              |
|                                             |                                          |
|   +----------------------------------------------------------------------------------+ |
|   | Federated Governance & Contracts: Bitol ODCS, Schema Validation, RBAC Policies   | |
|   +----------------------------------------------------------------------------------+ |
+========================================================================================+
|                                2. PROCESSING PLANE                                     |
|                                                                                        |
|   Sources:                                                                             |
|     [PostgreSQL OLTP] ---(JDBC Batch Snapshot)---------> [Apache Spark 3.5]            |
|     [Apache Kafka Bus] ---(Real-Time Event Stream)-----> [Apache Flink 1.18]           |
|                                                                                        |
|   Medallion Lakehouse Storage (Apache Iceberg tables on MinIO S3 Object Store):        |
|     +--------------------+       +--------------------+       +--------------------+   |
|     | BRONZE TIER        |  -->  | SILVER TIER        |  -->  | GOLD TIER          |   |
|     | Raw snapshot audit |       | Cleansed, typed,   |       | Dimensional models |   |
|     | Append-only        |       | Deduplicated       |       | GDPR PII-masked    |   |
|     +--------------------+       +--------------------+       +--------------------+   |
|                                                                                        |
|   Serving Layer:                                                                       |
|     +--------------------------------------------------------------------------------+ |
|     | Trino Distributed SQL Engine (Polaris REST Catalog Integration)                | |
|     | - Unified Serving Views: Batch Gold UNION ALL Stream Windows (5-min lag)       | |
|     | - Cross-Domain Federated Query Surface                                         | |
|     +--------------------------------------------------------------------------------+ |
+========================================================================================+
|                                3. OPERATIONS PLANE                                     |
|                                                                                        |
|   Active Metadata & Lineage:                                                           |
|     [Spark / Flink Listeners] --(OpenLineage JSON)--> [Marquez Metadata Service]      |
|                                                                                        |
|   Quality Gates & Validation:                                                          |
|     [Great Expectations] --(Quality Assertion Rules)--> [Pass: Commit / Fail: Block]  |
|                                                                                        |
|   Orchestration & Telemetry:                                                           |
|     [Dagster] -> Asset-Based Directed Acyclic Graph (SDA) Scheduling                   |
|     [Prometheus & Grafana] -> Pipeline Throughput, Latency, and Memory Footprint       |
+========================================================================================+
```

---

## Paradigm-to-Component Mapping

| Architectural Paradigm | Open-Source Component | Concrete Implementation in Repository |
| :--- | :--- | :--- |
| **Medallion Architecture** | Apache Iceberg + MinIO S3 | [`pipelines/medallion/bronze_ingestion.py`](pipelines/medallion/bronze_ingestion.py)<br>[`pipelines/medallion/silver_transform.py`](pipelines/medallion/silver_transform.py)<br>[`pipelines/medallion/gold_aggregation.py`](pipelines/medallion/gold_aggregation.py) |
| **Lakehouse Table Format** | Apache Iceberg v2 Specification | Snappy-compressed Parquet data with snapshot isolation, hidden partitioning, and time-travel capability. |
| **Lakehouse Catalog** | Apache Polaris (incubating) | REST catalog on port `8181` mapped in [`pipelines/serving/catalog_registration.py`](pipelines/serving/catalog_registration.py). |
| **Lambda Architecture** | Spark Batch + Flink Streaming | Unified querying via `UNION ALL` in [`pipelines/serving/unified_view.sql`](pipelines/serving/unified_view.sql). |
| **Kappa Architecture** | Kafka Replay + Flink Streaming | Historical offset rewind and `--replay` backfill in [`pipelines/stream/flink_orders_stream.py`](pipelines/stream/flink_orders_stream.py). |
| **Serving Layer** | Trino SQL Query Engine | Single SQL interface over batch and streaming buffers with sub-3s P99 latency. |
| **Data Mesh** | Domain Autonomy Layout | Segregated domain boundaries in [`domains/sales`](domains/sales), [`domains/marketing`](domains/marketing), and [`domains/finance`](domains/finance). |
| **Data Products** | Product Manifests (`product.yaml`) | Machine-readable manifests specifying owners, metadata, SLAs, and ports. |
| **Data Contracts** | Bitol ODCS (`contract.yaml`) | Authoritative schemas, quality rules, and backward-compatibility policies. |
| **Data Fabric & Lineage** | OpenLineage + Marquez | Automated runtime metadata harvesting and DAG lineage visualization on port `5000`. |
| **Data Quality Gates** | Great Expectations | Gatekeeper script [`quality/gates/run_quality_gate.py`](quality/gates/run_quality_gate.py) blocking failed runs. |
| **Data Governance** | GDPR PII Masking Engine | Irreversible SHA-256 email hashing and name initial truncation before Gold tier. |

---

## Domain Overview

```
+-----------------------------------------------------------------------------------+
|                                  BUSINESS DOMAINS                                 |
+--------------------------+----------------------------+---------------------------+
| Sales                    | Marketing                  | Finance                   |
+--------------------------+----------------------------+---------------------------+
| - Customers              | - Campaigns                | - Budgets                 |
| - Orders                 | - Leads                    | - Financial Closings      |
| - Gold: daily_metrics,   | - Clickstream Events       | - (consumer domain)       |
|   masked customers       |   (windowed, no funnel)    |                           |
| Contracts: BACKWARD      | Contracts: BACKWARD        | Consumes Sales+Marketing  |
+--------------------------+----------------------------+---------------------------+
```

1. **Sales**:
   - Owns the core transactional customer journey.
   - Publishes two primary data products: `customers` and `orders`.
   - Contract v1.2.0 defines strict backward compatibility, required field nullability constraints, and referential integrity between orders and customer entities.
2. **Marketing**:
   - Owns campaign lifecycle management, multi-channel attribution, and website clickstream telemetry.
   - Publishes data products: `campaigns` and `leads`.
   - Ingests streaming navigation events from Kafka to compute conversion rates and session drop-off funnels.
3. **Finance**:
   - Operates as a downstream analytical consumer domain.
   - Ingests certified Gold data products from `Sales` and `Marketing` to generate executive profit-and-loss reports and campaign return on advertising spend (ROAS).

---

## Subsystems Summary (S1 to S8)

- **S1: Source & Ingestion**: PostgreSQL when `PGPASSWORD` is set, otherwise a loud synthetic fallback + SQLite WAL event log (local); PostgreSQL + Kafka 3.7 KRaft in K8s (`s1-ingestion`, `s4-stream`).
- **S2: Lakehouse Storage & Catalog**: SQLite adapter with append-only idempotent Bronze, row-version time travel, schema evolution (local); MinIO + Iceberg + Polaris in K8s (`s2-lakehouse`).
- **S3: Batch Processing Engine**: Pure-Python Medallion drivers over the hexagonal core — Bronze snapshotting, Silver dedup + fail-closed gate, Gold full recomputation (local); Spark Operator in K8s (`s3-batch`).
- **S4: Stream Processing Engine**: Real 5-minute tumbling windows over the event log with consumer cursors + full replay (local); Flink Operator + Kafka in K8s (`s4-stream`).
- **S5: Serving Layer**: One anti-join SQL fusing batch Gold + stream windows, no double counting — Trino if `TRINO_HOST`, else embedded SQLite (`unified_view.sql` mirrors the semantics for Trino).
- **S6: Data Mesh & Data Products**: Standardized product manifests (`product.yaml`) and Bitol ODCS Data Contracts (`contract.yaml`) with real BACKWARD validation; CRDs + quotas in K8s (`s6-mesh`).
- **S7: Data Fabric & Governance**: OpenLineage emission to local audit trail (+ Marquez if reachable), fail-closed quality engine over GE-format suites, GDPR PII masking in Gold.
- **S8: Orchestration & Observability**: `rake` tasks + Dagster asset definitions locally; Marquez + alert service + Prometheus/Grafana assets in K8s (`s7-fabric`, `s8-ops` values only).

---

## Quickstart Guide & Startup Profiles

The **default way to run and verify** the POC is the local profile: plain Python + SQLite, no containers required. (K8s profiles via `infra/helm` + `infra/profiles/*.yaml` are the deployment target; see `docs/architecture/overview.md`.)

### Prerequisites
- Python 3.11+ (`python --version`)
- Ruby + Rake for the task runner (`rake --version`) — or run the `python ...` commands directly
- Optional dev tools: `ruff`, `mypy`, `pytest`, `pytest-cov` (`pip install ruff mypy pytest pytest-cov` — network required once; pinned ranges in `pyproject.toml` `[project.optional-dependencies] dev`)

### Local execution (default)

```bash
# RF-24 end-to-end demo: contracts -> bronze -> silver -> gold -> stream -> serving -> tests
rake demo
# (equivalently: ./scripts/demo.sh on Git Bash / WSL2 / Linux / macOS)

# Layer-by-layer tasks
rake governance:validate          # RF-15 contract validation
rake medallion:all                # RF-01..RF-07 (bronze -> silver -> gold)
rake stream:orders                # RF-09 speed layer (auto-seeds demo events if log empty)
rake stream:replay                # RF-10 full Kappa replay
rake serving:query                # RF-11 unified query
rake test                         # unit + integration (64 tests)
rake test:coverage                # with coverage report
rake lint                         # ruff
rake typecheck                    # mypy on src/
```

State lives in `data/` (git-ignored SQLite + lineage audit log). Reset with `rm -f data/*.db data/*.jsonl` (PowerShell: `Remove-Item data/*.db, data/*.jsonl -ErrorAction SilentlyContinue`).

### Web Interfaces & Port Reference (K8s profile only)

| Service | Port | Description | Credentials (if applicable) |
| :--- | :--- | :--- | :--- |
| **Trino SQL Console** | `http://localhost:8080` | Interactive ANSI SQL query engine | User: `trino` (no password) |
| **MinIO Console** | `http://localhost:9001` | S3 Object store file & metadata browser | `minioadmin` / `minioadmin` |
| **Marquez Lineage UI** | `http://localhost:3001` | OpenLineage interactive DAG viewer | Public access |
| **Dagster UI** | `http://localhost:3000` | Software-Defined Asset orchestrator | Public access |
| **Apache Polaris API** | `http://localhost:8181` | Iceberg REST catalog OpenAPI endpoint | Bearer token auth |
| **Grafana Dashboards** | `http://localhost:3002` | Platform telemetry & memory metrics | `admin` / `admin` |

---

## Repository Structure

```
poc_data_engineering/
├── README.md                                  # Root architectural documentation & quickstart
├── LICENSE                                    # MIT License (100% Open Source)
├── data-generator/                            # S1: Synthetic data generators
│   ├── oltp_seed.py                          # PostgreSQL relational seeder (Faker, psycopg2)
│   ├── event_producer.py                     # Kafka clickstream event streaming producer
│   └── requirements.txt                      # Python dependencies for generators
├── domains/                                   # S6: Data Mesh domain workspaces
│   ├── sales/                                # Sales domain
│   │   └── data-products/
│   │       ├── customers/                    # Customer data product
│   │       │   ├── contract.yaml             # Bitol ODCS Data Contract
│   │       │   ├── product.yaml              # Data Product Manifest
│   │       │   └── schema.json               # JSON Schema Draft-07
│   │       └── orders/                       # Order data product
│   │           ├── contract.yaml
│   │           ├── product.yaml
│   │           └── schema.json
│   └── marketing/                            # Marketing domain
│       └── data-products/
│           ├── campaigns/                    # Campaigns data product
│           └── leads/                        # Leads & conversion data product
├── pipelines/                                 # S3, S4, S5: Processing & Serving pipelines
│   ├── medallion/                            # S3: Spark Batch Medallion pipelines
│   │   ├── bronze_ingestion.py               # PostgreSQL -> Iceberg Bronze snapshot
│   │   ├── silver_transform.py               # Bronze -> Silver cleaning & deduplication
│   │   └── gold_aggregation.py               # Silver -> Gold aggregation & PII masking
│   ├── stream/                               # S4: Flink Stream processing pipelines
│   │   ├── flink_orders_stream.py            # Orders Kafka -> Iceberg stream buffer
│   │   └── flink_clickstream_stream.py       # Clickstream Kafka -> Funnel metrics
│   └── serving/                              # S5: Trino Serving Layer
│       ├── catalog_registration.py           # Polaris REST catalog sync & Marquez publish
│       └── unified_view.sql                  # Trino batch + stream UNION ALL views
├── quality/                                   # S7: Data Fabric & Quality Gates
│   ├── expectations/                         # Great Expectations JSON validation suites
│   │   ├── silver_orders_suite.json          # Orders completeness, uniqueness, enum rules
│   │   └── silver_customers_suite.json       # Customer schema & validity rules
│   └── gates/
│       └── run_quality_gate.py               # Automated pipeline gatekeeper script
└── docs/                                      # Complete Technical Documentation
    ├── srs.md                                # IEEE 830 / ISO-IEC-IEEE 29148 complete SRS
    ├── concepts-to-components.md             # Theoretical mapping to concrete code
    ├── demo-guide.md                         # Step-by-step RF-24 verification guide
    └── architecture/
        ├── overview.md                       # Detailed 3-plane system design
        └── adr/                              # Architecture Decision Records
            ├── ADR-001-lambda-kappa-hybrid.md
            ├── ADR-002-iceberg-lakehouse.md
            ├── ADR-003-multi-domain-mesh.md
            └── ADR-004-startup-profiles.md
```

---

## Acceptance Criteria Overview (RF-01 to RF-25)

| Req ID | Requirement Summary | Priority | Implementation Component |
| :--- | :--- | :---: | :--- |
| **RF-01** | Synthetic OLTP data generation (PostgreSQL 50k orders, 1k clients) | `[M]` | [`data-generator/oltp_seed.py`](data-generator/oltp_seed.py) |
| **RF-02** | Real-time event generation (Kafka clickstream with rate limiting) | `[M]` | [`data-generator/event_producer.py`](data-generator/event_producer.py) |
| **RF-03** | Batch ingestion from PostgreSQL to Iceberg Bronze | `[M]` | [`pipelines/medallion/bronze_ingestion.py`](pipelines/medallion/bronze_ingestion.py) |
| **RF-04** | Real-time streaming ingestion from Kafka to Iceberg via Flink | `[M]` | [`pipelines/stream/flink_orders_stream.py`](pipelines/stream/flink_orders_stream.py) |
| **RF-05** | Raw data preservation in append-only Bronze Lakehouse | `[M]` | Bronze tables on MinIO S3 object storage |
| **RF-06** | Data cleansing, typing, and deduplication in Silver layer | `[M]` | [`pipelines/medallion/silver_transform.py`](pipelines/medallion/silver_transform.py) |
| **RF-07** | Gold layer business aggregations and dimensional modeling | `[M]` | [`pipelines/medallion/gold_aggregation.py`](pipelines/medallion/gold_aggregation.py) |
| **RF-08** | ACID Lakehouse transactions, snapshots, and time-travel | `[M]` | Apache Iceberg v2 open table format |
| **RF-09** | Centralized REST catalog metadata management | `[M]` | Apache Polaris (incubating) REST catalog |
| **RF-10** | Kappa-style historical replay & reprocessing capability | `[S]` | `--replay` flag in batch + Kafka offset rewind |
| **RF-11** | Unified serving layer merging historical batch & real-time stream | `[M]` | [`pipelines/serving/unified_view.sql`](pipelines/serving/unified_view.sql) in Trino |
| **RF-12** | Interactive ad-hoc cross-domain analytical SQL querying | `[S]` | Trino federated queries across domain catalogs |
| **RF-13** | Domain decentralization and autonomous boundaries | `[M]` | Segregated workspaces in `domains/*` |
| **RF-14** | Formal Data Product manifests defining ownership and SLAs | `[M]` | `product.yaml` descriptors |
| **RF-15** | Declarative Data Contracts adhering to Bitol ODCS v2 | `[M]` | `contract.yaml` files across all products |
| **RF-16** | Automated contract backward compatibility verification | `[M]` | Pre-flight schema validation checks |
| **RF-17** | End-to-end automated metadata and lineage harvesting | `[M]` | OpenLineage emitters to Marquez API |
| **RF-18** | Automated quality gates blocking corrupted pipeline runs | `[M]` | [`quality/gates/run_quality_gate.py`](quality/gates/run_quality_gate.py) |
| **RF-19** | Automated GDPR PII masking (SHA-256 email hashing) | `[M]` | PII masking logic in `gold_aggregation.py` |
| **RF-20** | Policy-as-code governance enforcement | `[S]` | Kyverno container admission policies |
| **RF-21** | Asset-based workflow orchestration (Software-Defined Assets) | `[M]` | Dagster asset DAG & scheduler |
| **RF-22** | Infrastructure and pipeline monitoring telemetry | `[M]` | Prometheus scrapers & Grafana dashboards |
| **RF-23** | Execution profiling optimizing for 8 GB RAM constraint | `[M]` | Docker Compose profiles (`platform`, `stream`, `batch`) |
| **RF-24** | Automated end-to-end interactive demo validation script | `[M]` | [`scripts/demo.sh`](scripts/demo.sh) |
| **RF-25** | Complete production-grade technical documentation & ADRs | `[M]` | Directory [`docs/`](docs/) |

---

## Technical Documentation Sitemap

- [**Software Requirements Specification (IEEE 830 / ISO-IEC-IEEE 29148)**](docs/srs.md): Complete formal requirements, data constraints, sizing, interfaces, and traceability matrix.
- [**Architecture Overview & 3-Plane System Design**](docs/architecture/overview.md): Deep-dive into Organization, Processing, and Operations planes, subsystem breakdown, data flows, and memory budgeting.
- [**Architecture Decision Records (ADRs)**](docs/architecture/adr/):
  - [ADR-001: Hybrid Lambda + Kappa Architecture Selection](docs/architecture/adr/ADR-001-lambda-kappa-hybrid.md)
  - [ADR-002: Apache Iceberg and Apache Polaris as Lakehouse Foundation](docs/architecture/adr/ADR-002-iceberg-lakehouse.md)
  - [ADR-003: Multi-Domain Data Mesh with Data Products & Contracts](docs/architecture/adr/ADR-003-multi-domain-mesh.md)
  - [ADR-004: Resource-Constrained Execution Profiling Under 8 GB RAM](docs/architecture/adr/ADR-004-startup-profiles.md)
- [**Concepts-to-Components Mapping**](docs/concepts-to-components.md): Theoretical cross-reference from Modern Data Engineering to concrete implementation files.
- [**RF-24 End-to-End Demo Guide**](docs/demo-guide.md): Step-by-step verification, pre-flight checklists, UI inspections, and simulated failure testing.

---

## License & Author Info

- **License**: Distributed under the **MIT License**. See [`LICENSE`](LICENSE) for complete terms.
- **Author & Maintainer**: Sergio Sánchez ([`@sergio11`](https://github.com/sergio11))
- **Project**: Modern Data Architecture Proof of Concept (POC)
