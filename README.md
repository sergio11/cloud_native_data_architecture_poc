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

> **Two execution profiles** share the same hexagonal core (`src/`): the **local profile** (default — pure Python + SQLite, verified by `rake demo` / `rake test`) and the **K8s profile** (`infra/helm`, `infra/profiles` — deployment target). See [`docs/architecture/overview.md`](docs/architecture/overview.md) and the RF-by-RF verification status in [Acceptance Criteria Overview](#acceptance-criteria-overview-rf-01-to-rf-25) below.

### Verified scope & known K8s notes

> **The K8s profile (`infra/helm`, `infra/profiles`) is a *deployment target
> not verified in this environment*.** Everything marked VERIFIED in the
> [Acceptance Criteria Overview](#acceptance-criteria-overview-rf-01-to-rf-25) was proven in
> the local profile (Python + SQLite) via `rake demo` / `rake test`. The K8s
> charts now render real workloads (details in `docs/architecture/overview.md`,
> "What exists vs. what is wired"): `s1-ingestion` CronJob runs
> `pipelines/medallion/bronze_ingestion.py --entity all` from image
> `poc/ingest:0.1.0` (built by `infra/docker/ingest.Dockerfile`); `s7-fabric`
> runs the real alert service (`lineage/config/lineage_webhook_service.py`,
> uvicorn port `8000`, `/health` probes, PVC `/data`); `s8-ops` ships
> Prometheus/Grafana templates; `s6-mesh` ships Kyverno ClusterPolicies;
> Spark/Flink images come from `infra/docker/` (`poc/*:0.1.0` in values);
> the Trino views init-job applies `pipelines/serving/unified_view.sql`
> (which now ships only VIEW 1 — former views 2–3 were removed because no
> pipeline produces their source tables); `infra/profiles/*.yaml` are consumed
> via `scripts/profile-up.sh <profile>` (Helm `-f`).
> - `pipelines/serving/catalog_registration.py` requires `POLARIS_URL` /
>   `POLARIS_TOKEN` / `MARQUEZ_URL` (configuration requirement: K8s-only
>   services; it exits non-zero without them) **and is fail-closed**: it also
>   exits `1` when any Polaris/Marquez registration call fails (it never
>   reports "completed successfully" while a step failed). Its exit codes are
>   unit-tested with mocked clients (`tests/unit/test_catalog_manifest.py`); a
>   real run against live services is a K8s-profile activity, not part of any
>   local task.
>
> The K8s profile still has no in-cluster verification in this environment.

### Known limitations (local profile)

1. The speed layer is a **polling CLI**, not a resident daemon: no continuous streaming latency is measurable (RF-09 partial).
2. The local lakehouse is **single-node SQLite**: same contract as Iceberg (idempotent append, versioning, evolution) but no columnar storage or multi-engine access.
3. The quality engine evaluates **Great Expectations-format suites**, but **is not the GE library**.
4. **No CI pipeline**: validations block at runtime (`rake lint` / `rake typecheck` / `rake test:coverage` must be run manually and gate the tools via `exit 1` + `fail_under`), not on merge. Contract rules are covered by `tests/unit/test_contract_validation.py` and `rake governance:validate`.
5. Telemetry (Prometheus/Grafana), catalog UI (Marquez) and admission (Kyverno) are **K8s profile**, not verified here (their Helm templates now render — `infra/helm/s8-ops/templates/`, `infra/helm/s6-mesh/templates/kyverno-policies.yaml` — but no in-cluster run exists in this environment).
6. `flink_clickstream_stream.py` aggregates clickstream windows; funnel/session metrics are out of scope (SRS §1.2).
7. Serving fusion is by key, not recency: a stream row whose `(order_date, status)` already exists in batch is shadowed even if newer (batch-wins rule, RF-11).
8. Only contract dimensions the suite measures are enforced: a declared threshold (e.g. `uniqueness`) for a dimension the suite does not evaluate is skipped. Known case: `uniqueness` on Gold aggregates with composite keys is not measurable — the engine only supports single-column uniqueness.
9. `quality.assertions` in the contract DSL (e.g. `total_amount >= 0` in `orders/contract.yaml`) stay declarative: no code compiles them into engine expectations; only `spec.quality.thresholds` are enforced.
10. Residual structural notes (conscious trade-offs, not defects): the standalone quality-gate CLI pretty-prints via Rich while Dagster returns a bool; per-use-case status vocabularies (`SUCCESS`/`EMPTY`/`NO_EVENTS`) are statically typed per result contract (`Literal` in `src/ports/inbound.py`) rather than unified.
11. Provenance is honest but distinguishes local runs: rows served by the embedded generator are stamped `_source`/lineage input as `synthetic.local.<entity>` (never `postgresql.public.<entity>`); only a real PostgreSQL read earns the `postgresql.public.<entity>` label (`src/use_cases/ingest_bronze.py::provenance_label`).
12. Alert **delivery** is best-effort with no persistence: the alert service POSTs each fired alert to the rule's `actions.webhook.endpoint` (stdlib `urllib`, in-process retries only, `LINEAGE_WEBHOOK_DELIVERY`/`LINEAGE_WEBHOOK_TIMEOUT`), but there is no outbox/queue/retry-after-restart. The service itself is only started in the K8s profile (chart `s7-fabric` runs the real image `poc/lineage-webhook:0.1.0` on port `8000` with `/health` probes and PVC `/data`), so `rake demo` never sends an outbound alert.
13. The local profile registers **nothing** in an external catalog: `pipelines/serving/catalog_registration.py` needs `POLARIS_URL`/`POLARIS_TOKEN`/`MARQUEZ_URL` (K8s services), so Polaris/Marquez registration is exercised only by unit tests with mocked clients.

**Event backend (local profile).** `src/adapters/streaming/event_stream.py`
uses the embedded SQLite WAL log (`data/event_log.db`) by default; when
`KAFKA_BOOTSTRAP_SERVERS` is set and non-empty, Kafka becomes the canonical
backend with fail-fast semantics (unreachable broker raises `RuntimeError` —
never a silent SQLite fallback; no double writes). Both backends share the
same cursor contract: a consumer group without committed offsets reads from
the beginning (`auto.offset.reset=earliest` on Kafka, offset 0 on SQLite),
so a new group never silently skips the backlog.

**Quality-threshold precedence (one policy, every entry point).** Per-dimension
contract thresholds (`spec.quality.thresholds` in `contract.yaml`:
`completeness`, `uniqueness`, `validity`) govern the verdict **whenever the
table's entity has a governing contract** — for Silver/Gold promotion *and* for
the standalone `quality/gates/run_quality_gate.py` CLI alike. The CLI resolves
the contract itself (`contract_entity_for_table` in `src/domain/naming.py` +
`find_contract_thresholds`, wired in `src/use_cases/run_quality_gate.py`), so
the CLI's `--threshold` flag (default `0.95`) — and the `silver_transform.py`
flag of the same name — are **fallbacks used only when the contract declares no
thresholds, or none of its declared dimensions is measurable by the suite**.
Both paths share the same pure verdict (`src/domain/quality_gate.py::assess_gate`
called from `src/use_cases/gate_support.py::enforce_quality_gate` and from the
gate use case), so the CLI can never PASS a table that promotion then rejects
(or vice versa). A declared threshold that is not a number in `[0, 1]` (or a
`thresholds` block that is not a mapping) raises `ValueError` at gate time —
misconfigured governance fails loudly instead of silently degrading to the
global fallback. An engine fail-closed veto (unsupported expectation type)
always blocks, even when the contract dimensions pass.

**What `dimension_rates` means.** The engine reports, per quality dimension, the
share of **records** that pass every attributable assertion of that dimension:
`dimension_rates[dim] = 1 - (distinct rows failing >= 1 attributable assertion
of dim) / (total rows)` (rounded to 4 decimals;
`src/adapters/quality/expectation_engine.py`). Contract thresholds declared as
record-quality percentages (e.g. `validity 0.99`) are therefore evaluated on a
per-record basis. `mostly` keeps its Great Expectations meaning **only** for
each assertion's boolean verdict (tolerated invalid fraction `1 - mostly`) and
never enters the rate arithmetic (proven by
`tests/unit/test_quality_dimension_rates.py::test_mostly_does_not_dilute_record_validity_rate`).
The only exception is a dimension measured *solely* by non-row-attributable
assertions (`expect_table_row_count_to_be_between`), which keeps the legacy
assertion ratio `passed/total` — no shipped suite exercises that case.

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
| **Lakehouse Catalog** | Apache Polaris (incubating) | REST catalog (Polaris default port `8181` in the K8s profile) synced via [`pipelines/serving/catalog_registration.py`](pipelines/serving/catalog_registration.py) — which reads `POLARIS_URL`/`MARQUEZ_URL` from the environment and registers only tables actually produced by the core use cases. |
| **Lambda Architecture** | Spark Batch + Flink Streaming | Unified querying via `UNION ALL` in [`pipelines/serving/unified_view.sql`](pipelines/serving/unified_view.sql). |
| **Kappa Architecture** | Kafka Replay + Flink Streaming | Full-log replay via `--replay` / `--start-offset earliest` in [`pipelines/stream/flink_orders_stream.py`](pipelines/stream/flink_orders_stream.py) (or `rake stream:replay`). Batch needs no replay flag: every `gold_aggregation.py` run is already a full recomputation from Silver. |
| **Serving Layer** | Trino SQL Query Engine | Single SQL interface over batch and streaming buffers with one anti-join statement (design target of sub-3s P99 per `docs/srs.md` RNF-01 — **not measured anywhere in this repo**; local verification is functional, not latencied). |
| **Data Mesh** | Domain Autonomy Layout | Segregated domain boundaries in [`domains/sales`](domains/sales), [`domains/marketing`](domains/marketing), and [`domains/finance`](domains/finance). |
| **Data Products** | Product Manifests (`product.yaml`) | Machine-readable manifests specifying owners, metadata, SLAs, and ports. |
| **Data Contracts** | Bitol ODCS (`contract.yaml`) | Authoritative schemas, quality rules, and backward-compatibility policies. |
| **Data Fabric & Lineage** | OpenLineage + Marquez | Automated runtime metadata harvesting into `data/lineage_audit.jsonl`, with best-effort POST to Marquez; the DAG lineage UI (Marquez, port `3001` in the table below) is a **K8s-profile** service and does not run locally. |
| **Data Quality Gates** | Great Expectations | Gatekeeper script [`quality/gates/run_quality_gate.py`](quality/gates/run_quality_gate.py) blocking failed runs. |
| **Data Governance** | GDPR PII Masking Engine | Salted SHA-256 email hashing (`PII_HASH_SALT`, default `poc-demo-salt-v1`) and name initial truncation enforced in [`src/domain/privacy.py`](src/domain/privacy.py) and applied by [`src/use_cases/aggregate_gold.py`](src/use_cases/aggregate_gold.py) before Gold tier (driven by [`pipelines/medallion/gold_aggregation.py`](pipelines/medallion/gold_aggregation.py)). |

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
   - Ingests streaming navigation events into 5-minute tumbling windows (`stream.marketing.clickstream_windowed`); conversion/funnel/session analytics are out of scope (see limitation 6).
3. **Finance**:
   - Operates as a downstream analytical consumer domain.
   - Ingests certified Gold data products from `Sales` and `Marketing` to generate executive profit-and-loss reports and campaign return on advertising spend (ROAS).

---

## Subsystems Summary (S1 to S8)

- **S1: Source & Ingestion**: PostgreSQL when `PGPASSWORD` is set, otherwise a loud synthetic fallback + SQLite WAL event log (local); PostgreSQL + Kafka 3.7 KRaft in K8s (`s1-ingestion`, `s4-stream`). A reachable PostgreSQL table is authoritative even when empty (empty table yields `[]`); synthetic records are generated only when PostgreSQL is unconfigured or the connection/query fails (logged at WARNING). **Provenance is stamped honestly**: the Bronze `_source` audit column and the matching lineage input dataset are `postgresql.public.<entity>` only for real database reads, and `synthetic.local.<entity>` for generated fallback rows (`src/use_cases/ingest_bronze.py::provenance_label`).
- **S2: Lakehouse Storage & Catalog**: SQLite adapter with append-only idempotent Bronze, row-version time travel, schema evolution (local); MinIO + Iceberg + Polaris in K8s (`s2-lakehouse`).
- **S3: Batch Processing Engine**: Pure-Python Medallion drivers over the hexagonal core — Bronze snapshotting, Silver dedup + fail-closed gate, Gold full recomputation (local); Spark Operator in K8s (`s3-batch`).
- **S4: Stream Processing Engine**: Real 5-minute tumbling windows over the event log with consumer cursors + full replay (local); Flink Operator + Kafka in K8s (`s4-stream`).
- **S5: Serving Layer**: One anti-join SQL fusing batch Gold + stream windows, no double counting — Trino if `TRINO_HOST`, else embedded SQLite (`unified_view.sql` mirrors the semantics for Trino).
- **S6: Data Mesh & Data Products**: Standardized product manifests (`product.yaml`) and Bitol ODCS Data Contracts (`contract.yaml`) with real BACKWARD validation; CRDs + quotas in K8s (`s6-mesh`).
- **S7: Data Fabric & Governance**: OpenLineage emission to local audit trail (+ Marquez if reachable), fail-closed quality engine over GE-format suites, GDPR PII masking in Gold.
- **S8: Orchestration & Observability**: `rake` tasks + Dagster asset definitions locally (the five assets dispatch the real pipeline scripts through `pipelines/orchestration/local_dispatch.py`); Marquez + alert service + Prometheus/Grafana in K8s (real templates in `s7-fabric`, `s8-ops`).

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
rake replay:all                   # RF-12 full reprocessing (medallion:all + stream:replay + serving:query)
rake serving:query                # RF-11 unified query (exits non-zero on an empty result; use run_serving_query.py --allow-empty to tolerate it)
rake test                         # unit + integration (276 tests: 264 unit + 12 integration)
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
├── Rakefile                                   # Cross-platform task runner (demo, medallion, stream, replay:all, serving, test, lint)
├── pyproject.toml                             # Metadata + ruff/mypy/pytest/coverage config (config-only, no build backend)
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
│   ├── marketing/                            # Marketing domain (campaigns, leads)
│   └── finance/                              # Finance domain (downstream consumer: budgets, financial_closings)
├── src/                                       # Hexagonal core: domain, use cases, ports, adapters, container
├── tests/                                     # Unit + integration tests (RF acceptance scenarios)
│   ├── unit/                                  # Fast, isolated tests (ports, use cases, adapters, orchestration dispatch)
│   └── integration/                           # End-to-end RF acceptance scenarios over the local profile
├── scripts/                                   # Operational scripts (demo.sh wrapper, profile-up/down, setup)
├── governance/                                # Data contracts validation (contract_validator.py), policies, schemas, federation
│   ├── contract_validator.py                 # CLI: structure + BACKWARD + product<->contract threshold cross-check
│   ├── federation/global-standards.yaml      # Federated (cross-domain) standards
│   ├── policies/                             # require-owner, require-pii-classification, naming-convention
│   └── schemas/                              # contract-schema.json, product-schema.json
├── lineage/                                   # S7: Data Fabric assets
│   └── config/
│       ├── lineage_webhook_service.py        # FastAPI OpenLineage ingestion + RF-20 rules + best-effort webhook alerts
│       ├── alert-rules.yaml                  # Four RF-20 rules (only keys the service actually reads)
│       ├── marquez-values.yaml               # Marquez Helm values (K8s profile)
│       ├── openlineage.yml                   # OpenLineage / Marquez configuration
│       ├── Dockerfile                        # Image for the alert service (K8s profile)
│       └── requirements.txt                  # Service-only dependencies (fastapi, uvicorn, pyyaml)
├── infra/                                     # K8s deployment target: helm charts per subsystem + kind + profiles + docker
│   ├── helm/                                 # One chart per subsystem (s1-ingestion … s8-ops, with real templates)
│   ├── docker/                               # Container images (poc/*:0.1.0: ingest, spark, flink)
│   ├── kind/                                 # Local kind cluster config + setup script
│   └── profiles/                             # Resource-constrained startup profiles (platform/stream/batch/demo, via scripts/profile-up.sh)
├── pipelines/                                 # S3, S4, S5, S8: Processing, Serving & Orchestration pipelines
│   ├── medallion/                            # S3: batch Medallion drivers (pure Python locally, Spark jobs in K8s)
│   │   ├── bronze_ingestion.py               # OLTP (PG or synthetic) -> Bronze snapshot
│   │   ├── silver_transform.py               # Bronze -> Silver cleaning, dedup & fail-closed gate
│   │   └── gold_aggregation.py               # Silver -> Gold aggregation & PII masking
│   ├── stream/                               # S4: stream processing drivers
│   │   ├── flink_orders_stream.py            # Orders events -> stream.sales.orders_windowed (5-min windows, replay)
│   │   └── flink_clickstream_stream.py       # Clickstream events -> stream.marketing.clickstream_windowed (no funnel metrics)
│   ├── orchestration/                        # S8: Dagster Software-Defined Assets (RF-22)
│   │   ├── dagster_repository.py             # 5 assets + jobs + cron schedules (Definitions)
│   │   ├── resources.py                      # Spark/Flink/Marquez/GreatExpectations resources
│   │   ├── local_dispatch.py                 # Local profile: assets run the real scripts via sys.executable subprocess
│   │   └── dagster-values.yaml               # Helm values for the upstream Dagster chart (K8s profile)
│   └── serving/                              # S5: Trino Serving Layer
│       ├── catalog_registration.py           # Polaris/Marquez registration (K8s env vars; fail-closed exit 1)
│       ├── run_serving_query.py              # RF-11 CLI (exit 1 on empty result unless --allow-empty)
│       └── unified_view.sql                  # Trino batch + stream UNION ALL views (VIEW 1 RF-11 only; former views 2-3 removed)
├── quality/                                   # S7: Data Fabric & Quality Gates
│   ├── expectations/                         # Great-Expectations JSON validation suites
│   │   ├── silver_orders_suite.json          # Orders completeness, uniqueness, enum rules
│   │   ├── silver_customers_suite.json       # Customer schema & validity rules
│   │   ├── gold_daily_metrics_suite.json     # Gold daily metrics gate (required by gold_aggregation)
│   │   └── gold_customers_suite.json         # Gold customers gate (required by gold_aggregation)
│   └── gates/
│       └── run_quality_gate.py               # Automated pipeline gatekeeper script (contract thresholds govern)
└── docs/                                      # Complete Technical Documentation
    ├── srs.md                                # IEEE 830 / ISO-IEC-IEEE 29148 SRS (legacy RF numbering — srs.txt wins)
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

> Canonical numbering: [`srs.txt`](srs.txt) §5 is the source of truth. The table below follows it
> (the legacy English `docs/srs.md` uses a different RF numbering — see the NOTICE at the top of that file).

| Req ID | Requirement Summary | Priority | Status | Implementation Component |
| :--- | :--- | :---: | :---: | :--- |
| **RF-01** | Ingesta OLTP periódica e idempotente hacia Bronze | `[M]` | VERIFIED | [`pipelines/medallion/bronze_ingestion.py`](pipelines/medallion/bronze_ingestion.py) |
| **RF-02** | Ingesta de eventos de dominio en el log con timestamp y procedencia | `[M]` | VERIFIED | [`data-generator/event_producer.py`](data-generator/event_producer.py), [`pipelines/stream/flink_orders_stream.py`](pipelines/stream/flink_orders_stream.py) |
| **RF-03** | Separación lógica estricta Bronze → Silver → Gold, sin saltos | `[M]` | VERIFIED | [`pipelines/medallion/`](pipelines/medallion/) |
| **RF-04** | Silver: limpieza, tipado, deduplicación y validaciones sobre Bronze | `[M]` | VERIFIED | [`pipelines/medallion/silver_transform.py`](pipelines/medallion/silver_transform.py) (tipado explícito INTEGER/REAL/TEXT por esquema de contrato, probado en `tests/unit/test_silver_typing.py`) |
| **RF-05** | Gold: métricas/agregados de negocio listos para consumo | `[M]` | VERIFIED | [`pipelines/medallion/gold_aggregation.py`](pipelines/medallion/gold_aggregation.py) + [`src/use_cases/aggregate_gold.py`](src/use_cases/aggregate_gold.py) |
| **RF-06** | Quality gates entre capas: si no cumplen umbrales, no se publica y se notifica | `[M]` | VERIFIED | [`quality/gates/run_quality_gate.py`](quality/gates/run_quality_gate.py) + [`src/use_cases/gate_support.py`](src/use_cases/gate_support.py) — bloqueo verificado localmente (exit `1` + evento lineage `FAIL` en `data/lineage_audit.jsonl`, `test_rf06_*`); *notificación* = webhook saliente del servicio de alertas (`lineage/config/lineage_webhook_service.py` → `actions.webhook.endpoint`, probado en `tests/unit/test_w4_webhook_delivery.py`), servicio que **solo se despliega en K8s** (chart `s7-fabric` con la imagen real) y queda fuera de `rake demo` |
| **RF-07** | ACID, time travel y evolución de esquema controlada | `[M]` | VERIFIED | Apache Iceberg in K8s profile; SQLite adapter with row-version time travel locally (`src/adapters/storage/`) |
| **RF-08** | Batch layer: recálculo completo del histórico, programado y manual | `[M]` | VERIFIED | [`pipelines/medallion/gold_aggregation.py`](pipelines/medallion/gold_aggregation.py) (full recomputation from Silver on every run) |
| **RF-09** | Stream layer: agregados en ventana con latencia ≤ 5 s | `[M]` | PARTIAL (ventanas reales e inmediatez verificadas; el límite de 5 s no se mide: CLI por polling, no demonio residente) | [`pipelines/stream/flink_orders_stream.py`](pipelines/stream/flink_orders_stream.py) (5-min tumbling windows) |
| **RF-10** | Kappa: regenerar resultados releyendo el log desde su origen | `[M]` | VERIFIED | `rake stream:replay` / `--start-offset earliest` / `--replay` in [`pipelines/stream/flink_orders_stream.py`](pipelines/stream/flink_orders_stream.py); batch needs no flag (full recompute by design). Un replay sobre log vacío reconcilia la tabla Speed a vacío (no deja filas obsoletas) |
| **RF-11** | Serving layer: una única consulta fusiona batch exacto + ventana reciente | `[M]` | VERIFIED | [`pipelines/serving/unified_view.sql`](pipelines/serving/unified_view.sql) + [`pipelines/serving/run_serving_query.py`](pipelines/serving/run_serving_query.py) (el CLI **falla con exit 1 si la consulta unificada no devuelve filas**, salvo `--allow-empty`) |
| **RF-12** | Reproceso completo de cualquier capa sin pérdida de consistencia | `[S]` | VERIFIED | `rake replay:all` (= `medallion:all` + `stream:replay` + `serving:query`): recomputación total de Gold desde Silver + replay completo del log en una invocación |
| **RF-13** | Publicación declarativa de productos por dominio | `[M]` | PARTIAL (el manifiesto *es* la publicación en local; sin pipeline de registro) | `product.yaml` manifests in [`domains/`](domains/) |
| **RF-14** | Productos encontrables y autoexplicativos en catálogo | `[M]` | PARTIAL (catálogo UI buscable solo en K8s/Marquez; en local el catálogo es el árbol `domains/` + validación) | `product.yaml` + Marquez catalog in K8s profile (`infra/helm/s7-fabric`) |
| **RF-15** | Contratos máquina-legibles validados; ruptura bloquea publicación y alerta | `[M]` | VERIFIED | [`governance/contract_validator.py`](governance/contract_validator.py) (bloqueo en ejecución/validación, incl. *cross-check* `product.yaml` ↔ `contract.yaml` que falla si los umbrales de calidad divergen; reglas cubiertas por `tests/unit/test_contract_validation.py` y `rake governance:validate`) |
| **RF-16** | Consumo cross-dominio self-service sin intermediación del productor | `[M]` | PARTIAL (apertura total, sin capa de control de acceso) | Serving layer ([`pipelines/serving/`](pipelines/serving/)) |
| **RF-17** | Políticas globales federadas aplicadas automáticamente (dueño, PII, nombres) | `[S]` | PARTIAL (gobernanza local verificada + Kyverno ClusterPolicies renderizadas en `infra/helm/s6-mesh/templates/kyverno-policies.yaml`; admisión solo efectiva en K8s, sin verificación en cluster en este entorno) | [`governance/policies/`](governance/policies/) + Kyverno ClusterPolicies en K8s (`infra/helm/s6-mesh`) |
| **RF-18** | Cuotas/aislamiento por dominio en la plataforma compartida | `[S]` | DEFERRED (K8s `ResourceQuota`/`LimitRange`; sin enforcement local) | [`infra/helm/s6-mesh/`](infra/helm/s6-mesh/) (K8s `ResourceQuota`/`LimitRange` per domain) |
| **RF-19** | Linaje end-to-end consultable, alimentado automáticamente | `[M]` | VERIFIED | [`lineage/`](lineage/) + Marquez (local audit trail `data/lineage_audit.jsonl`) |
| **RF-20** | Metadatos activos: alertas ante cambios de esquema o degradación de calidad | `[S]` | VERIFIED | [`lineage/config/`](lineage/config/) alert service + [`lineage/config/alert-rules.yaml`](lineage/config/alert-rules.yaml): las 4 reglas están unit-testeadas (`tests/unit/test_w4_config_wiring.py` schema, `tests/unit/test_lineage_webhook_rules.py` + `test_w4_alert_edge_cases.py` quality, `test_w4_m1_honest.py::test_m1_pipeline_failure_error_facet` failure, `test_m1_sla_*` freshness), entrega por webhook probada (`tests/unit/test_w4_webhook_delivery.py`), payload con `runId` (`test_lineage_payload_shape.py`) y servicio real cableado en K8s (`s7-fabric` uvicorn `:8000` + `/health` + PVC `/data`). Nota honesta: entrega *best-effort* sin outbox y servicio sin verificación en cluster en este entorno; fuera de `rake demo` |
| **RF-21** | Seguridad/clasificación embebida en el flujo, no en documentación externa | `[S]` | PARTIAL (PII mascarada en Gold verificada con SHA-256 con sal — `PII_HASH_SALT`, default `poc-demo-salt-v1` —; sin RBAC en ejecución) | [`src/domain/privacy.py`](src/domain/privacy.py) + [`src/use_cases/aggregate_gold.py`](src/use_cases/aggregate_gold.py) (salted SHA-256 email hashing, name masking) |
| **RF-22** | Pipelines orquestables: programables, a demanda, con estado observable | `[M]` | VERIFIED | [`Rakefile`](Rakefile) tasks + 5 Dagster assets ([`pipelines/orchestration/dagster_repository.py`](pipelines/orchestration/dagster_repository.py)) que materializan **localmente** despachando los scripts reales vía [`pipelines/orchestration/local_dispatch.py`](pipelines/orchestration/local_dispatch.py) (`sys.executable` en subprocess; replay Flink = `flink_orders_stream.py --start-offset earliest`) — cubierto por `tests/unit/test_w5_local_dispatch.py` (dagster es el extra opcional `orchestration`) |
| **RF-23** | Panel único: estado de pipelines, lag de stream, cumplimiento de calidad | `[S]` | DEFERRED (templates Prometheus/Grafana ahora renderizados en `infra/helm/s8-ops/templates/` — `prometheus.yaml`, `grafana.yaml` — pero sin verificación en cluster en este entorno; ningún dashboard verificado localmente) | Grafana/Prometheus templates + assets en [`infra/helm/s8-ops/`](infra/helm/s8-ops/) seleccionados vía [`infra/profiles/`](infra/profiles/) (no docker-compose exists in this repo) |
| **RF-24** | Comando único de demo end-to-end reproducible | `[M]` | VERIFIED | [`scripts/demo.sh`](scripts/demo.sh) / `rake demo` |
| **RF-25** | Documentación de arquitectura apta para portfolio | `[M]` | VERIFIED | Directory [`docs/`](docs/) |

> **Status counts — RF:** VERIFIED **17** · PARTIAL **6** · DEFERRED **2**. A requirement is VERIFIED only if an automated test or command in this repository proves it in the local profile. Evidence: `rake test` (276 tests: 264 unit + 12 integration, via `python -m pytest tests --collect-only -q -p no:cacheprovider`), `rake demo` (RF-24 end-to-end), `rake lint` + `rake typecheck` + `rake test:coverage` (gates run manually; there is no CI pipeline). Coverage scope is `src` + `lineage` with `fail_under = 85` (see `pyproject.toml`).

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
