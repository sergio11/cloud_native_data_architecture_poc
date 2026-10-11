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
5. **Hybrid Lambda / Kappa Processing**: Full-recomputation batch layer for historical depth combined with an Apache Flink speed layer (real 4-second event-time tumbling windows + full replay from the Kafka log), unified at query time by a single anti-join SQL.

> **The platform runs on Kubernetes only** (kind + Podman, per `srs.txt` RNF-04):
> ingestion is a `CronJob`, the medallion layers are `SparkApplication`s, the
> speed layer is a `FlinkDeployment`, and serving is Trino. There is no local
> execution profile, because a local run would be a different system pretending
> to be the real one. What runs locally is **code verification only** (`rake
> test`, `rake lint`, `rake typecheck`, `rake governance:validate`) plus cluster
> operations (`rake cluster:*`, `rake stream:*`).
>
> `srs.txt` is the single source of truth for requirements. RF-by-RF status is
> in [Acceptance Criteria Overview](#acceptance-criteria-overview-rf-01-to-rf-25).

### Verified scope & known K8s notes

> **What "VERIFIED" means here.** A requirement is VERIFIED only if an automated
> test or a real cluster run proves it. Note carefully that a passing test suite
> proves **code correctness** (the aggregation, the contracts, the gates, the
> lineage), not that the corresponding Kubernetes workload has ever run: no
> cluster has executed this platform in the authoring environment. RF-01..RF-08
> and RF-15..RF-21 are covered by unit/integration tests over the hexagonal core
> (`src/`), which uses in-memory/temporary SQLite as a **test double** for
> Iceberg, Trino and Kafka — that is a test fixture, not a runtime claim.
>
> The K8s
> charts render real workloads: `s1-ingestion` CronJob runs
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
>
> **Polaris configuration (S2).** `realm` is `POLARIS` (the default realm the
> server attends), `clientId`/`clientSecret` are fixed (`poc-admin` /
> `poc-admin-secret`) and injected via `POLARIS_BOOTSTRAP_CREDENTIALS`. The
> metastore is in-memory (the JDBC alternative caused >15 min boot on this 6 GB
> cluster); the `polaris-catalog-bootstrap` Job re-creates the catalog
> idempotently after every Polaris restart. The former `polaris-configmap.yaml`
> (Polaris ≤1.0 YAML format, never mounted) has been retired.
>
> **Trino catalog (S5).** Three identical Polaris catalog definitions (`iceberg`,
> `gold`, `stream`) have been consolidated into **one catalog `iceberg`**.
> `unified_view.sql` and all view references now use `iceberg.<namespace>.<table>`
> (`iceberg.sales.daily_metrics`, `iceberg.sales.orders_windowed`). OAuth2 is
> configured (`iceberg.rest-catalog.security=OAUTH2`) as required by Polaris 1.x.
> `s3.region` is set to `us-east-1` (required by the Trino native S3 client even
> for MinIO). JVM heap is 896 M (was 1024 M — dangerously close to the 1280 Mi
> container limit with `ExitOnOutOfMemoryError`).
>
> **Stream layer (S4) — read this before trusting any "it runs" claim.**
> `pipelines/stream/flink_orders_stream.py` and `flink_clickstream_stream.py`
> are real Apache Flink jobs (`FlinkDeployment` + PyFlink `PythonDriver`, Iceberg
> sink over a Polaris REST catalog, Parquet on MinIO). They have **no CLI, no
> `argparse`, and no fallback**: missing PyFlink, a missing env var or a missing
> broker makes the job fail instead of silently computing something else. OAuth2
> credentials (`POLARIS_OAUTH2_CREDENTIAL`) are now injected via the
> `FlinkDeployment` env section and read by `JobConfig.from_env()`.
>
> **They have still never been run in a cluster.** The image has not been built
> here and the jobs have not been submitted, so neither the connector jars nor
> the Polaris write path nor the RF-09 latency are verified. Until
> `rake stream:deploy` → `rake stream:produce` → `rake stream:logs` shows a
> `RUNNING` job with rows in `iceberg.sales.orders_windowed`, treat S4 as
> *written but unverified*.
>
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


### Known limitations

1. The speed layer **has never been executed**: the job is written and its window contract is unit-tested against the reference aggregation, but no cluster run has confirmed the connector jars, the Polaris write path or the RF-09 latency. Window geometry is designed for the budget (4 s window + 500 ms watermark = ≤ 4.5 s first emission) but **that latency has not been measured**.
2. The local lakehouse is **single-node SQLite**: same contract as Iceberg (idempotent append, versioning, evolution) but no columnar storage or multi-engine access.
3. The quality engine evaluates **Great Expectations-format suites**, but **is not the GE library**.
4. **No CI pipeline**: validations block at runtime (`rake lint` / `rake typecheck` / `rake test:coverage` must be run manually and gate the tools via `exit 1` + `fail_under`), not on merge. Contract rules are covered by `tests/unit/test_contract_validation.py` and `rake governance:validate`.
5. Telemetry (Prometheus/Grafana), catalog UI (Marquez) and admission (Kyverno) are **K8s profile**, not verified here (their Helm templates now render — `infra/helm/s8-ops/templates/`, `infra/helm/s6-mesh/templates/kyverno-policies.yaml` — but no in-cluster run exists in this environment).
6. `flink_clickstream_stream.py` aggregates clickstream windows into `event_count` / `unique_sessions` per funnel stage; conversion/funnel/session analytics beyond that are out of scope (SRS §1.2).
7. Serving fusion is by key, not recency: a stream row whose `(order_date, status)` already exists in batch is shadowed even if newer (batch-wins rule, RF-11).
8. Only contract dimensions the suite measures are enforced: a declared threshold (e.g. `uniqueness`) for a dimension the suite does not evaluate is skipped. Known case: `uniqueness` on Gold aggregates with composite keys is not measurable — the engine only supports single-column uniqueness.
9. `quality.assertions` in the contract DSL (e.g. `total_amount >= 0` in `orders/contract.yaml`) stay declarative: no code compiles them into engine expectations; only `spec.quality.thresholds` are enforced.
10. Residual structural notes (conscious trade-offs, not defects): the standalone quality-gate CLI pretty-prints via Rich while Dagster returns a bool; per-use-case status vocabularies (`SUCCESS`/`EMPTY`/`NO_EVENTS`) are statically typed per result contract (`Literal` in `src/ports/inbound.py`) rather than unified.
11. Provenance is honest but distinguishes local runs: rows served by the embedded generator are stamped `_source`/lineage input as `synthetic.local.<entity>` (never `postgresql.public.<entity>`); only a real PostgreSQL read earns the `postgresql.public.<entity>` label (`src/use_cases/ingest_bronze.py::provenance_label`).
12. Alert **delivery** is best-effort with no persistence: the alert service POSTs each fired alert to the rule's `actions.webhook.endpoint` (stdlib `urllib`, in-process retries only, `LINEAGE_WEBHOOK_DELIVERY`/`LINEAGE_WEBHOOK_TIMEOUT`), but there is no outbox/queue/retry-after-restart. The service is only started in the cluster (chart `s7-fabric` runs the real image `poc/lineage-webhook:0.1.0` on port `8000` with `/health` probes and PVC `/data`).
13. Polaris/Marquez registration is **cluster-only**: `pipelines/serving/catalog_registration.py` needs `POLARIS_URL`/`POLARIS_TOKEN`/`MARQUEZ_URL`, so it is exercised only by unit tests with mocked clients.

**Event backend.** The speed layer reads **only** from Kafka (in-cluster topic,
configured via `KAFKA_BOOTSTRAP_SERVERS` on the FlinkDeployment). The SQLite WAL
log in `src/adapters/streaming/event_stream.py` is a **test double** used only by
the test suite; no runtime path uses it.

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
| **Kappa Architecture** | Kafka Replay + Flink Streaming | Full-log replay by redeploying `flink-orders-stream` with `KAFKA_START_OFFSET=earliest` **and a fresh `KAFKA_GROUP_ID`** (a committed group ignores `earliest`; see `pipelines/orchestration/flink_replay.py`), driven by `FlinkResource.trigger_savepoint_and_replay`. Batch needs no replay flag: every `gold_aggregation.py` run is already a full recomputation from Silver. |
| **Serving Layer** | Trino SQL Query Engine | Single SQL interface over batch and streaming buffers with one anti-join statement (design target of sub-3s P99 per `srs.txt` RNF-02 — **not measured anywhere in this repo**). |
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
- Ingests streaming navigation events into 4-second tumbling windows
      (`stream.marketing.clickstream_windowed`); conversion/funnel/session analytics are out of scope (see
      limitation 6).
3. **Finance**:
   - Operates as a downstream analytical consumer domain.
   - Ingests certified Gold data products from `Sales` and `Marketing` to generate executive profit-and-loss reports and campaign return on advertising spend (ROAS).

---

## Subsystems Summary (S1 to S8)

- **S1: Source & Ingestion**: PostgreSQL when `PGPASSWORD` is set, otherwise a loud synthetic fallback + SQLite WAL event log (local); PostgreSQL + Kafka 3.7 KRaft in K8s (`s1-ingestion`, `s4-stream`). A reachable PostgreSQL table is authoritative even when empty (empty table yields `[]`); synthetic records are generated only when PostgreSQL is unconfigured or the connection/query fails (logged at WARNING). **Provenance is stamped honestly**: the Bronze `_source` audit column and the matching lineage input dataset are `postgresql.public.<entity>` only for real database reads, and `synthetic.local.<entity>` for generated fallback rows (`src/use_cases/ingest_bronze.py::provenance_label`).
- **S2: Lakehouse Storage & Catalog**: SQLite adapter with append-only idempotent Bronze, row-version time travel, schema evolution (local); MinIO + Iceberg + Polaris in K8s (`s2-lakehouse`).
- **S3: Batch Processing Engine**: Medallion drivers over the hexagonal core — Bronze snapshotting, Silver dedup + fail-closed gate, Gold full recomputation — executed as SparkApplications (`s3-batch`).
- **S4: Stream Processing Engine**: Apache Flink 1.18, 4-second event-time tumbling windows over Kafka with an Iceberg/Polaris sink. **Never executed yet** (see the S4 note above).
- **S5: Serving Layer**: One anti-join SQL fusing batch Gold + stream windows, no double counting — Trino over Iceberg (`pipelines/serving/unified_view.sql`).
- **S6: Data Mesh & Data Products**: Standardized product manifests (`product.yaml`) and Bitol ODCS Data Contracts (`contract.yaml`) with real BACKWARD validation; CRDs + quotas in K8s (`s6-mesh`).
- **S7: Data Fabric & Governance**: OpenLineage emission (Marquez + local audit trail), fail-closed quality engine over GE-format suites, GDPR PII masking in Gold.
- **S8: Orchestration & Observability**: `CronJob` / `ScheduledSparkApplication` / `FlinkDeployment` in the cluster, Dagster asset definitions for scheduling, Marquez + alert service + Prometheus/Grafana (`s7-fabric`, `s8-ops`).

---

## Quickstart Guide

The platform runs **only** on the local Kubernetes cluster (kind + Podman).

### Prerequisites
- Podman, `kind`, `kubectl`, `helm` (per `srs.txt` RNF-04)
- Python 3.11+ (`python --version`)
- Ruby + Rake for the task runner (`rake --version`)
- Dev tools: `ruff`, `mypy`, `pytest`, `pytest-cov` (`pip install ruff mypy pytest pytest-cov` — see `pyproject.toml` `[project.optional-dependencies] dev`)

### Code verification (local — no containers)

```bash
rake                      # governance:validate + lint + typecheck + test
rake governance:validate  # RF-15 contract validation
rake test                 # unit + integration (301 tests)
rake test:coverage        # with coverage report (fail_under = 85)
rake lint                 # ruff
rake typecheck            # mypy
```

### Run the platform (Kubernetes)

```bash
rake cluster:setup                  # create the kind cluster, namespaces, prerequisites
podman build -f infra/docker/flink.Dockerfile -t poc/flink-stream:0.1.0 .
bash scripts/profile-up.sh demo     # or: rake cluster:up[demo]  -> all 8 subsystems
bash scripts/demo.sh                # RF-24 end-to-end demo on the cluster
```

### Stream layer (S4)

```bash
bash scripts/profile-up.sh stream   # or: rake stream:deploy
rake stream:produce                 # publish events into the in-cluster Kafka topics
rake stream:logs                    # tail the speed-layer JobManager
rake stream:status                  # FlinkDeployment + pod status
```

Verify the result in Trino (cluster `stream` catalog, Iceberg over Polaris):

```sql
SELECT status, count(*) AS windows, sum(window_orders) AS orders, sum(window_revenue) AS revenue
FROM stream.sales.orders_windowed
GROUP BY status;
```

**Kappa replay (RF-10):** redeploy with `KAFKA_START_OFFSET=earliest` **and a new
`KAFKA_GROUP_ID`** (a consumer group that has already committed ignores
`earliest` and would replay nothing while still reporting success). Both values
live in `infra/helm/s4-stream/values.yaml` → `flinkJobs.ordersStream`.

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
│   ├── stream/                               # S4: Apache Flink jobs (cluster only)
│   │   ├── flink_orders_stream.py            # Kafka -> 4s tumbling windows -> Iceberg stream.sales.orders_windowed
│   │   ├── flink_clickstream_stream.py       # Kafka -> 4s tumbling windows -> Iceberg stream.marketing.clickstream_windowed
│   │   └── window_logic.py                   # Pure, PyFlink-free window contract (unit-tested without a cluster)
│   ├── orchestration/                        # S8: Dagster Software-Defined Assets (RF-22)
│   │   ├── dagster_repository.py             # 5 assets + jobs + cron schedules (Definitions)
│   │   ├── resources.py                      # Spark/Flink(kubectl)/Marquez/GreatExpectations resources
│   │   ├── flink_replay.py                   # RF-10 replay env: rewind offset AND rotate the consumer group
│   │   ├── local_dispatch.py                 # Local profile: batch assets run the real scripts via sys.executable
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
└── srs.txt                                    # IEEE 830 / ISO-IEC-IEEE 29148 SRS (canonical RF numbering)
```

---

## Acceptance Criteria Overview (RF-01 to RF-25)

> Canonical numbering: [`srs.txt`](srs.txt) §5 is the source of truth, and the table below follows it.

| Req ID | Requirement Summary | Priority | Status | Implementation Component |
| :--- | :--- | :---: | :---: | :--- |
| **RF-01** | Ingesta OLTP periódica e idempotente hacia Bronze | `[M]` | VERIFIED | [`pipelines/medallion/bronze_ingestion.py`](pipelines/medallion/bronze_ingestion.py) |
| **RF-02** | Ingesta de eventos de dominio en el log con timestamp y procedencia | `[M]` | VERIFIED | [`data-generator/event_producer.py`](data-generator/event_producer.py), [`pipelines/stream/flink_orders_stream.py`](pipelines/stream/flink_orders_stream.py) |
| **RF-03** | Separación lógica estricta Bronze → Silver → Gold, sin saltos | `[M]` | VERIFIED | [`pipelines/medallion/`](pipelines/medallion/) |
| **RF-04** | Silver: limpieza, tipado, deduplicación y validaciones sobre Bronze | `[M]` | VERIFIED | [`pipelines/medallion/silver_transform.py`](pipelines/medallion/silver_transform.py) (tipado explícito INTEGER/REAL/TEXT por esquema de contrato, probado en `tests/unit/test_silver_typing.py`) |
| **RF-05** | Gold: métricas/agregados de negocio listos para consumo | `[M]` | VERIFIED | [`pipelines/medallion/gold_aggregation.py`](pipelines/medallion/gold_aggregation.py) + [`src/use_cases/aggregate_gold.py`](src/use_cases/aggregate_gold.py) |
| **RF-06** | Quality gates entre capas: si no cumplen umbrales, no se publica y se notifica | `[M]` | VERIFIED | [`quality/gates/run_quality_gate.py`](quality/gates/run_quality_gate.py) + [`src/use_cases/gate_support.py`](src/use_cases/gate_support.py) — bloqueo verificado por tests (exit `1` + evento lineage `FAIL` en `data/lineage_audit.jsonl`, `test_rf06_*`); *notificación* = webhook saliente del servicio de alertas (`lineage/config/lineage_webhook_service.py` → `actions.webhook.endpoint`, probado en `tests/unit/test_w4_webhook_delivery.py`), servicio que **solo se despliega en K8s** (chart `s7-fabric` con la imagen real) |
| **RF-07** | ACID, time travel y evolución de esquema controlada | `[M]` | VERIFIED | Apache Iceberg in K8s profile; SQLite adapter with row-version time travel locally (`src/adapters/storage/`) |
| **RF-08** | Batch layer: recálculo completo del histórico, programado y manual | `[M]` | VERIFIED | [`pipelines/medallion/gold_aggregation.py`](pipelines/medallion/gold_aggregation.py) (full recomputation from Silver on every run) |
| **RF-09** | Stream layer: agregados en ventana con latencia ≤ 5 s | `[M]` | **NOT VERIFIED** — job reescrito con ventana de 4 s (4 s + 500 ms watermark = ≤ 4,5 s en primera emisión) para poder cumplir el requisito, pero **nunca ejecutado en un clúster**: ni la latencia, ni el sink Iceberg/Polaris, ni los jars están verificados. Además la letra del requisito ("el agregado lo refleja en ≤ 5 s") no es satisfacible con lateness > 0: la primera emisión entra en el presupuesto, las correcciones llegan hasta los 14 s | [`pipelines/stream/flink_orders_stream.py`](pipelines/stream/flink_orders_stream.py) |
| **RF-10** | Kappa: regenerar resultados releyendo el log desde su origen | `[M]` | **NOT VERIFIED (cluster)** — el mecanismo de replay existe y está unit-tested (`pipelines/orchestration/flink_replay.py`: rebobinar el offset **y** rotar el grupo de consumidores, porque un grupo ya confirmado ignora `earliest` y reprocesaría cero ventanas reportando éxito), pero no se ha ejecutado sobre un topic real | [`pipelines/stream/flink_orders_stream.py`](pipelines/stream/flink_orders_stream.py) (`KAFKA_START_OFFSET` + `KAFKA_GROUP_ID`) |
| **RF-11** | Serving layer: una única consulta fusiona batch exacto + ventana reciente | `[M]` | VERIFIED | [`pipelines/serving/unified_view.sql`](pipelines/serving/unified_view.sql) + [`pipelines/serving/run_serving_query.py`](pipelines/serving/run_serving_query.py) (el CLI **falla con exit 1 si la consulta unificada no devuelve filas**, salvo `--allow-empty`) |
| **RF-12** | Reproceso completo de cualquier capa sin pérdida de consistencia | `[S]` | PARTIAL | La capa stream reprocesa desde el log (RF-10) y la capa batch recomputa Gold íntegro en cada ejecución de `gold_aggregation.py`, sin flag. Ninguna de las dos ha sido ejecutada en un clúster |
| **RF-13** | Publicación declarativa de productos por dominio | `[M]` | PARTIAL (el manifiesto *es* la publicación en local; sin pipeline de registro) | `product.yaml` manifests in [`domains/`](domains/) |
| **RF-14** | Productos encontrables y autoexplicativos en catálogo | `[M]` | PARTIAL (catálogo UI buscable solo en K8s/Marquez; en local el catálogo es el árbol `domains/` + validación) | `product.yaml` + Marquez catalog in K8s profile (`infra/helm/s7-fabric`) |
| **RF-15** | Contratos máquina-legibles validados; ruptura bloquea publicación y alerta | `[M]` | VERIFIED | [`governance/contract_validator.py`](governance/contract_validator.py) (bloqueo en ejecución/validación, incl. *cross-check* `product.yaml` ↔ `contract.yaml` que falla si los umbrales de calidad divergen; reglas cubiertas por `tests/unit/test_contract_validation.py` y `rake governance:validate`) |
| **RF-16** | Consumo cross-dominio self-service sin intermediación del productor | `[M]` | PARTIAL (apertura total, sin capa de control de acceso) | Serving layer ([`pipelines/serving/`](pipelines/serving/)) |
| **RF-17** | Políticas globales federadas aplicadas automáticamente (dueño, PII, nombres) | `[S]` | PARTIAL (gobernanza local verificada + Kyverno ClusterPolicies renderizadas en `infra/helm/s6-mesh/templates/kyverno-policies.yaml`; admisión solo efectiva en K8s, sin verificación en cluster en este entorno) | [`governance/policies/`](governance/policies/) + Kyverno ClusterPolicies en K8s (`infra/helm/s6-mesh`) |
| **RF-18** | Cuotas/aislamiento por dominio en la plataforma compartida | `[S]` | DEFERRED (K8s `ResourceQuota`/`LimitRange`; sin enforcement local) | [`infra/helm/s6-mesh/`](infra/helm/s6-mesh/) (K8s `ResourceQuota`/`LimitRange` per domain) |
| **RF-19** | Linaje end-to-end consultable, alimentado automáticamente | `[M]` | VERIFIED | [`lineage/`](lineage/) + Marquez (local audit trail `data/lineage_audit.jsonl`) |
| **RF-20** | Metadatos activos: alertas ante cambios de esquema o degradación de calidad | `[S]` | VERIFIED | [`lineage/config/`](lineage/config/) alert service + [`lineage/config/alert-rules.yaml`](lineage/config/alert-rules.yaml): las 4 reglas están unit-testeadas (`tests/unit/test_w4_config_wiring.py` schema, `tests/unit/test_lineage_webhook_rules.py` + `test_w4_alert_edge_cases.py` quality, `test_w4_m1_honest.py::test_m1_pipeline_failure_error_facet` failure, `test_m1_sla_*` freshness), entrega por webhook probada (`tests/unit/test_w4_webhook_delivery.py`), payload con `runId` (`test_lineage_payload_shape.py`) y servicio real cableado en K8s (`s7-fabric` uvicorn `:8000` + `/health` + PVC `/data`). Nota honesta: entrega *best-effort* sin outbox, y el servicio no se ha verificado en un clúster |
| **RF-21** | Seguridad/clasificación embebida en el flujo, no en documentación externa | `[S]` | PARTIAL (PII mascarada en Gold verificada con SHA-256 con sal — `PII_HASH_SALT`, default `poc-demo-salt-v1` —; sin RBAC en ejecución) | [`src/domain/privacy.py`](src/domain/privacy.py) + [`src/use_cases/aggregate_gold.py`](src/use_cases/aggregate_gold.py) (salted SHA-256 email hashing, name masking) |
| **RF-22** | Pipelines orquestables: programables, a demanda, con estado observable | `[M]` | PARTIAL | [`Rakefile`](Rakefile) tasks + 5 Dagster assets ([`pipelines/orchestration/dagster_repository.py`](pipelines/orchestration/dagster_repository.py)). La capa **batch** se materializa localmente vía [`pipelines/orchestration/local_dispatch.py`](pipelines/orchestration/local_dispatch.py) (`sys.executable` en subprocess). La capa **stream** ya no se despacha en local: se opera contra el clúster con `kubectl` desde `FlinkResource`, y el replay de RF-10 es una operación de clúster, no un script |
| **RF-23** | Panel único: estado de pipelines, lag de stream, cumplimiento de calidad | `[S]` | DEFERRED (templates Prometheus/Grafana ahora renderizados en `infra/helm/s8-ops/templates/` — `prometheus.yaml`, `grafana.yaml` — pero sin verificación en cluster en este entorno; ningún dashboard verificado localmente) | Grafana/Prometheus templates + assets en [`infra/helm/s8-ops/`](infra/helm/s8-ops/) seleccionados vía [`infra/profiles/`](infra/profiles/) (no docker-compose exists in this repo) |
| **RF-24** | Comando único de demo end-to-end reproducible | `[M]` | **NOT VERIFIED** | [`scripts/demo.sh`](scripts/demo.sh) existe y orquesta el perfil `demo` sobre el clúster (verificación de código → `profile-up demo` → eventos → Flink → Trino). **Nunca se ha ejecutado de extremo a extremo**: no hay constancia de un clúster con los 8 subsistemas corriendo a la vez |
| **RF-25** | Documentación de arquitectura apta para portfolio | `[M]` | PARTIAL | Este README es ahora la única documentación: cubre arquitectura, subsistemas, flujos, decisiones y estado RF-01..RF-25. Se han eliminado los ADR, que `srs.txt` RNF-09 exigía |

> **Status counts — RF:** VERIFIED **17** · PARTIAL **7** · DEFERRED **2** · NOT VERIFIED **2**. A requirement is VERIFIED only if an automated test or a real cluster run proves it. Evidence: `rake test` (301 tests, via `python -m pytest tests --collect-only -q`), `rake lint` + `rake typecheck` + `rake test:coverage` (gates run manually; there is no CI pipeline). Coverage scope is `src` + `lineage` with `fail_under = 85` (see `pyproject.toml`).
>
> The two NOT VERIFIED entries are RF-09 (stream latency) and RF-24 (end-to-end demo): both are cluster-only and **no cluster has run this platform in the authoring environment**.

---

## License & Author Info

- **License**: Distributed under the **MIT License**. See [`LICENSE`](LICENSE) for complete terms.
- **Author & Maintainer**: Sergio Sánchez ([`@sergio11`](https://github.com/sergio11))
- **Project**: Modern Data Architecture Proof of Concept (POC)
