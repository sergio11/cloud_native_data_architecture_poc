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
> lineage), not that the corresponding Kubernetes workload has ever run: a
> test over the hexagonal core (`src/`) says nothing about the pods.
>
> **What has run on the cluster** (kind + Podman, 6 GiB / 4 CPU, the
> development box): the medallion on the Spark Operator - bronze through the
> `s1` CronJob, silver and gold as `ScheduledSparkApplication` on the
> operator - writing real rows into the Iceberg lakehouse (Polaris + MinIO),
> with the quality gates evaluating the contract thresholds; the speed layer
> on Flink (Kafka -> 4 s tumbling window -> Iceberg -> Parquet in MinIO),
> read back by Trino through Polaris; the serving layer (Trino) over the
> single `iceberg` catalog; the lineage from the core into Marquez; the
> observability tier (Prometheus + Grafana) and the admission control
> (Kyverno). The demo is a **sequence of phases**
> (`rake demo:phases`), not the eight subsystems at once, because the box
> does not hold them.
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
> via `rake cluster:up[<profile>]` (Helm `-f`).
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
> **Batch layer (S3) — the medallion on the Spark Operator.**
> `s3-batch` renders three `ScheduledSparkApplication` (bronze 02:00, silver
> 03:00, gold 04:00) that run `pipelines/medallion/*.py` inside a Spark driver
> pod. Five things about it are worth knowing before touching it:
>
> - **The medallion code is Spark-free.** `src/` has no `pyspark` import
>   anywhere: the drivers are plain Python that the core runs, and Spark is
>   the *deployment mechanism*, not a dependency. The executor pod therefore
>   has nothing to do, and the lineage that the platform exposes is published
>   by the core itself (`MarquezLineageAdapter`,
>   `src/adapters/lineage/marquez_lineage.py`), not by a Spark listener.
> - **The Spark image is `python:3.11-slim` + the pyspark wheel, not
>   `apache/spark`.** The official `apache/spark:3.5.1-...-python3-ubuntu`
>   image is Ubuntu 20.04 with **Python 3.8**, and the core needs 3.11:
>   `from enum import StrEnum` (`src/domain/contracts.py`) and
>   `from datetime import UTC` (`src/use_cases/ingest_bronze.py`) are 3.11
>   imports that fail at *import* time, so the driver died before running a
>   line of the pipeline. The image is now `python:3.11-slim-bookworm` (Java
>   17 for the JVM, `pyspark==3.5.1` placed at `/opt/spark`, which is where
>   the operator looks for `spark-submit`) and it is ~1 GB instead of 2.5 GB.
> - **`spark-defaults.conf` is NOT mounted, on purpose.** The ConfigMap
>   existed and nothing referenced it; mounting it as it was written would
>   have broken every job, because it asked for
>   `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` and
>   `io.openlineage.spark.agent.OpenLineageSparkListener`, two JARs that are
>   in neither the official image nor this one. The catalog is opened by
>   PyIceberg, so Spark never needs a catalog of its own. The template now
>   renders nothing and says why.
> - **The container limit is `memory + memoryOverhead`, not `memory`.** The
>   operator maps `memory` to `spark.driver.memory` (the JVM heap) and adds
>   the overhead to reach the pod limit; for a Kubernetes *non-JVM* (Python)
>   job Spark defaults that factor to **0.40**. Both are set explicitly now
>   (`driver: 512m + 384m`, `executor: 512m + 384m`), because with the heap
>   filling the whole container the first allocation gets the pod
>   OOM-killed.
> - **The image needs an entrypoint that dispatches on `driver`/`executor`.**
>   The operator starts the pod with an *empty* command and puts `driver` (or
>   `executor`) as the first argument, expecting the image to dispatch on it
>   the way the official `apache/spark` image does through
>   `/opt/entrypoint.sh`. An image without it dies with
>   `exec: "driver": executable file not found in $PATH`.
>   `infra/docker/spark-entrypoint.sh` is that entrypoint, and the `driver`
>   branch is not a plain `spark-submit`: it adds **`--deploy-mode client`**,
>   because the operator has already created the driver pod. Without the flag
>   the driver tries to create its own pod and dies with
>   `pods "spark-...-driver" already exists` (409).
> - **The environment goes in `envVars`, not `env`.** The v1beta2 operator
>   (1.6.2) does not translate the `env` LIST of the CRD into anything: the
>   API server accepts it, the operator ignores it, and the driver pod is
>   created without it. The medallion then ran with no `ICEBERG_*` variables,
>   fell back to the **SQLite** adapter and reported "no records found" while
>   the lakehouse had 105 rows. `driver.envVars` (a map) is what the operator
>   turns into `spark.kubernetes.driverEnv.<name>` options, and that is how
>   the variables reach the driver container.
> - **The variable names have to match the code.** The charts set
>   `ICEBERG_CATALOG_URL` while the code reads `ICEBERG_CATALOG_URI`: the
>   medallion died in the pod with "ICEBERG_CATALOG_URI is required for the
>   Iceberg lakehouse backend". The charts now use the canonical name and
>   the code accepts the old one as an alias
>   (`tests/unit/test_iceberg_catalog.py`).
> - **The operator needs RBAC on the `status` subresource.** `sparkapplications`
>   and `scheduledsparkapplications` are not enough: without
>   `sparkapplications/status` the operator submits the application but cannot
>   record what happened to it, and every `SparkApplication` keeps an empty
>   status (`cannot update resource "sparkapplications/status"`).
> - **A table created by the catalogue registration has to be evolved.** The
>   registration job creates the published tables in Polaris so the catalogue
>   lists them, and it creates them with the business schema only. The write
>   payload always carries the two internal columns (`_content_hash`,
>   `_row_seq`), so the Gold tier died with "PyArrow table contains more
>   columns" until the adapter learned to add them to a table it did not
>   create.
> - **The OLTP database is not seeded by the deployment.** The chart creates
>   the SCHEMA; the data comes from `data-generator/oltp_seed.py`, which
>   needs Faker and numpy, so it runs on the host through a port-forward:
>   `rake cluster:seed`. Without it the medallion reads an empty source and
>   reports EMPTY for every entity.
> - **One writer per table.** Bronze is ingested by the `s1` CronJob
>   (`poc/ingest:0.1.0`) and the Spark bronze job is **disabled**
>   (`jobs.bronzeIngestion.enabled=false`): with both enabled the same
>   Iceberg table had two writers, one every 15 minutes and one at 02:00.
>   Silver and gold run on Spark. `rake batch:run` runs the whole medallion
>   on demand: the CronJob is suspended and a one-shot Job is created from
>   it, and the silver/gold `ScheduledSparkApplication` spec is read back
>   and submitted as a plain `SparkApplication`, so the manual run and the
>   cron run exactly the same thing.
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

1. The speed layer **runs, but not everywhere at once**. The job itself has been executed on a cluster: Kafka → 4 s tumbling window → Iceberg → Parquet in MinIO, with checkpoints completing and Trino reading the result back through Polaris. What could not be done on a 6 GiB / 4-CPU host is running the whole platform simultaneously, so the end-to-end demo (`rake demo`, RF-24) remains unverified, and the **RF-09 latency has not been measured**: the window geometry (4 s + 500 ms watermark ⇒ ≤ 4.5 s for the first emission) is a design, not a measurement. `rake stream:probe` proves the logic in one pod, and `rake cluster:diagnose` is what makes a failure readable at all.
2. The local lakehouse is **single-node SQLite**: same contract as Iceberg (idempotent append, versioning, evolution) but no columnar storage or multi-engine access. Note that the same gap exists in the cluster: `container.storage` is the SQLite adapter, so the **batch** medallion layer writes to a local file and never reaches the lakehouse — the Spark jobs' `ICEBERG_*` environment variables are read by nobody. The speed layer writes to Iceberg because it goes through the Flink Table API, not through the hexagonal core.
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
rake cluster:image:build            # build the four poc/* images and load them into every node
rake cluster:up[batch]              # the batch phase: ingestion + lakehouse + Spark Operator
rake batch:run                      # run the medallion now (bronze -> silver -> gold)
rake lakehouse:rows                 # count the rows the medallion left in the Iceberg lakehouse
rake demo                           # RF-24 end-to-end demo, phase by phase
```

`rake cluster:image:build` is the single versioned procedure RNF-03 asks for:
it builds `poc/ingest`, `poc/spark-medallion`, `poc/flink-stream` and
`poc/lineage-webhook` with podman and copies them into every kind node
(building them by hand and forgetting to load them was the usual way to end
up with `failed to pull image: insufficient_scope`).

### The demo is a sequence of phases, not eight subsystems at once

RC-02 gives the box 8 GB and asks for **exclusive** startup profiles, and
AD-4 says "Demo por perfiles". The `demo` profile (all eight subsystems
simultaneously) needs ~8000 MiB of container limits and does not fit in 6 GiB:
the `kube-scheduler` ended up losing its lease 107 times. So the end-to-end
demo is a **sequence**, and each phase brings up what it needs and takes down
what the next one cannot afford:

```bash
rake demo:phases                    # platform -> batch -> stream -> serving -> fabric -> ops -> governance
rake demo:phases[batch,stream]      # only some phases
```

| Phase | Up | Taken down for the next phase |
| :--- | :--- | :--- |
| `platform` | postgres, minio, polaris, marquez, lineage webhook | - |
| `batch` | Spark Operator + the medallion | - |
| `stream` | kafka, flink operator, flink job | postgres, Spark Operator, marquez, webhook |
| `serving` | trino | flink job, flink operator |
| `fabric` | marquez + webhook back up | trino |
| `ops` | prometheus, grafana | flink operator |
| `governance` | kyverno controller + policies | kafka |

What crosses the phases is the **lakehouse** (MinIO + Iceberg), not a pod.
That is what makes it possible to run the batch tier in one phase and the
stream tier in another and still query them together at the end (RF-11).

`infra/profiles/platform.yaml`, `stream.yaml`, `batch.yaml`, `serving.yaml`
and `demo.yaml` describe the five profiles; `demo.yaml` is the complete
topology and documents what the whole platform looks like, but it is the one
profile that does not fit in the box.

### Stream layer (S4)

```bash
rake stream:deploy                  # Kafka + Flink Operator + the speed-layer jobs
rake stream:probe                   # prove window -> Iceberg -> Parquet in one pod (~3 min)
rake stream:produce                 # publish events into the in-cluster Kafka topics
rake stream:logs                    # tail the speed-layer JobManager
rake stream:status                  # FlinkDeployment + pod status
```

#### `rake stream:probe` - the fast check

`rake stream:probe` answers one question precisely: **does the speed layer turn
events into Parquet?** It runs the real operators from
`pipelines/stream/flink_orders_stream.py` inside a single pod with a local Flink
cluster, and writes through the real Iceberg sink to the real MinIO.

It exists because the `FlinkDeployment` cannot be trusted to report failures on a
small machine: the JobManager can take minutes to bind its REST port, the
Operator restarts it, and the terminated container's log is gone - which is how
several rounds of guesses ended up committed with no evidence. The probe needs
neither Kafka, nor the JobManager, nor the TaskManager, nor the Operator, so it
runs in ~1 GB and finishes in under a minute of cluster time.

It **proves**: `_OrdersWindowFunction.process_element` runs and returns rows,
the aggregate → window → Table API → Iceberg → Parquet chain works, and the
checkpoint storage `s3a://` really writes to MinIO. It does **not** cover the
Kafka source, the FlinkDeployment, the checkpointing of a running job, or the
serving layer.

#### `rake batch:run` and `rake lakehouse:rows` - the medallion, on demand and counted

`rake lakehouse:probe` proves the *adapter*; these two prove the *medallion*.

`rake batch:run` runs the three tiers in order, right now, without waiting
for the 02:00/03:00/04:00 crons:

* **bronze** — the `s1` CronJob is suspended and a one-shot Job is created
  from it (`kubectl create job --from=cronjob/`), so what runs is exactly
  what the cron would have run;
* **silver** and **gold** — the spec of the deployed
  `ScheduledSparkApplication` is read back and applied as a plain
  `SparkApplication`, so the chart stays the only source of truth for what a
  tier runs and the manual run cannot drift from the scheduled one.

`rake lakehouse:rows` then reads the catalogue and the tables and reports how
many rows each tier really has, from two independent angles: what PyIceberg
sees in Polaris, and what the storage adapter reads back. If the two disagree,
the medallion is writing somewhere the catalogue does not know about, which
is exactly the failure the probe exists to catch. It is the acceptance check
for the batch phase ("bronze, silver and gold have rows in Iceberg"), and it
reads the lakehouse itself rather than trusting a log line.

#### `rake lakehouse:probe` - the batch layer against the real lakehouse

The medallion layer had a second implementation of the lakehouse port waiting
for it: `IcebergLakehouseStorageAdapter`, which writes Iceberg tables into
Polaris with the data files in MinIO, instead of a local SQLite file. The
container picks it with `LAKEHOUSE_BACKEND=iceberg`.

`rake lakehouse:probe` proves it **by writing**: it creates a table in the
catalogue, appends rows, checks that a second identical write appends nothing
(RF-01 content-hash idempotency), reads the rows back and lists the snapshots
(RF-07 time travel). It runs the repository's own composition root, so what it
exercises is the wiring the medallion drivers use.

Two things it had to discover, both of which fail silently otherwise:

* the Iceberg client sends `X-Iceberg-Access-Delegation: vended-credentials`
  on every request, and Polaris reads that as "this client wants to write
  directly", which it enforces with a privilege the `catalog_admin` role does not
  carry:
  `ForbiddenException: ... is not authorized for op CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION`.
  The identical request without the header is accepted.
* the OAuth2 scope is not optional. The Iceberg client's own default is
  `catalog`, and Polaris rejects it with `invalid_scope: The scope is invalid`.
  It wants `PRINCIPAL_ROLE:ALL`, the same scope the Flink job asks for.

The script travels in a ConfigMap (`infra/diagnostics/window_write_probe.py`),
so it can be corrected without paying the image rebuild plus per-node load
cycle.

Two findings it encodes, both measured on a cluster:

* Iceberg writes data files with `S3FileIO` (AWS SDK v2), whose credential chain
  ignores the catalogue's `s3.access-key` / `s3.secret-key` and reads
  `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` instead. Without them the table
  **metadata** is created (that goes through the Polaris REST API) and every data
  file write dies with `SdkClientException`, leaving an empty warehouse.
* `DataStream.process()` takes a `ProcessFunction`; PyFlink 1.18 calls
  `process_element(value, ctx)`, returns the rows to emit, and passes no
  Collector. `ProcessWindowFunction.process(key, ctx, elements)` and a
  three-argument `process_element(value, ctx, out)` are never invoked.
* Checkpointing to MinIO needs **unshaded Guava** in the image.
  `flink-shaded-hadoop-2-uber` relocates its Guava to
  `org.apache.flink.shaded.guava31`, and `hadoop-aws 2.8.3` asks for the
  original `com.google.common.util.concurrent.ListeningExecutorService`, so
  `S3AFileSystem` cannot even be loaded without it. The alternative `s3://` is
  worse than broken: the legacy jets3t `S3FileSystem` ignores `fs.s3.endpoint`
  and talks to real AWS.

Verify the result in Trino (single `iceberg` catalog, Iceberg over Polaris):

```sql
SELECT status, count(*) AS windows, sum(window_orders) AS orders, sum(window_revenue) AS revenue
FROM iceberg.sales.orders_windowed
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
├── infra/                                     # K8s deployment target: helm charts per subsystem + kind + profiles + docker
│   ├── helm/                                 # One chart per subsystem (s1-ingestion … s8-ops, with real templates)
│   ├── docker/                               # Container images (poc/*:0.1.0: ingest, spark, flink)
│   ├── diagnostics/                           # Executable probes: the single-pod speed-layer check
│   ├── kind/                                 # Local kind cluster config
│   └── profiles/                             # Resource-constrained startup profiles (platform/stream/batch/serving/demo)
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
│   ├── kind/                                 # Local kind cluster config
│   └── profiles/                             # Resource-constrained startup profiles (platform/stream/batch/serving/demo)
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
| **RF-09** | Stream layer: agregados en ventana con latencia <= 5 s | `[M]` | **PARTIAL** - la agregacion y la persistencia estan verificadas en el clúster de punta a punta (Kafka -> ventana de 4 s -> Iceberg -> Parquet en MinIO -> Trino), y la **latencia ya esta MEDIDA** con `rake stream:latency` en el incremento `ramp:stream`: 934 eventos reales, ultimo evento -> commit final del sink **33,8 s** (commits individuales de 0,8 a 17,3 s). El desglose es explicito: ventana + watermark <= 4,5 s (config), espera al siguiente checkpoint <= 30 s (`enable_checkpointing(30_000)`) y la duracion del commit (I/O del rootfs WSL2). Conclusion honesta: **el agregado se CALCULA dentro de los 5 s, pero la FILA no es visible en el lakehouse hasta el siguiente checkpoint**, asi que la letra del requisito no se cumple para la visibilidad. A eso se suma que el requisito tampoco es satisfacible con `lateness > 0` (las correcciones llegan hasta los 14 s). El source de Kafka si queda cubierto por la medicion | [`pipelines/stream/flink_orders_stream.py`](pipelines/stream/flink_orders_stream.py) - medicion `rake stream:latency`, sonda [`infra/diagnostics/window_write_probe.py`](infra/diagnostics/window_write_probe.py) |
| **RF-10** | Kappa: regenerar resultados releyendo el log desde su origen | `[M]` | **PARTIAL** - el replay **se ejecutó sobre el topic real** con `rake stream:replay`: el `FlinkDeployment` se parchea rotando `KAFKA_GROUP_ID` y poniendo `KAFKA_START_OFFSET=earliest` (rebobinar el offset **sin** rotar el grupo no replaya nada y aun asi reporta exito), el job se reinicia, relee los 934 eventos del log desde el offset 0 y vuelve a commitear sobre `polaris.sales.orders_windowed`. **Lo que falta es la prueba de idempotencia logica**: `total-records` en la metadata no sirve como oraculo porque el sink es upsert/merge-on-read (cada commit anade los registros nuevos **y** los equality-deletes de los anteriores: 110 -> 165 sin que cambie el resultado). La comparacion correcta (huella `md5` del contenido logico antes/despues, con el job suspendido y Trino arriba) esta implementada en `rake stream:replay:verify` pero quedo **sin completar por inestabilidad del apiserver** (I/O del entorno) | [`pipelines/stream/flink_orders_stream.py`](pipelines/stream/flink_orders_stream.py) (`KAFKA_START_OFFSET` + `KAFKA_GROUP_ID`), [`pipelines/orchestration/flink_replay.py`](pipelines/orchestration/flink_replay.py) |
| **RF-11** | Serving layer: una única consulta fusiona batch exacto + ventana reciente | `[M]` | VERIFIED | [`pipelines/serving/unified_view.sql`](pipelines/serving/unified_view.sql) + [`pipelines/serving/run_serving_query.py`](pipelines/serving/run_serving_query.py) (el CLI **falla con exit 1 si la consulta unificada no devuelve filas**, salvo `--allow-empty`). Además probado **en el clúster** con `rake serving:verify`: `iceberg.serving.sales_unified_revenue` devuelve `batch_historical` 76 filas + `stream_speed` 4 filas en la misma consulta, con **0** claves `(order_date, status)` duplicadas |
| **RF-12** | Reproceso completo de cualquier capa sin pérdida de consistencia | `[S]` | PARTIAL | La mitad **stream** ya se ejecuta en clúster: el replay rerelee el log completo y vuelve a commitear (RF-10). La mitad **batch** no puede ejecutarse en clúster porque el core hexagonal no tiene adaptador Iceberg (`container.storage` es `LakehouseStorageAdapter` y los drivers del medallón nunca tocan el lakehouse); solo recomputa Gold íntegro sobre SQLite en local |
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
| **RF-24** | Comando unico de demo end-to-end reproducible | `[M]` | **NOT VERIFIED** | [`rake demo`](Rakefile) orquesta el perfil `demo` sobre el cluster (verificacion de codigo -> `cluster:up demo` -> eventos -> Flink -> Trino). **No se ha ejecutado de extremo a extremo**: el perfil `demo` levanta los 8 subsistemas a la vez y la maquina de desarrollo (6 GiB de VM, 4 CPU, 7,9 GB de RAM en el host) no los sostiene - el `kube-scheduler` llega a 107 reinicios por perder su lease. Las fases si se han probado por separado: la speed layer escribe Parquet y Trino lo lee |
| **RF-25** | Documentación de arquitectura apta para portfolio | `[M]` | PARTIAL | Este README es ahora la única documentación: cubre arquitectura, subsistemas, flujos, decisiones y estado RF-01..RF-25. Se han eliminado los ADR, que `srs.txt` RNF-09 exigía |

> **Status counts — RF:** VERIFIED **12** · PARTIAL **10** · DEFERRED **2** · NOT VERIFIED **1**. A requirement is VERIFIED only if an automated test or a real cluster run proves it. Evidence: `rake test` (301 tests, via `python -m pytest tests --collect-only -q`), `rake lint` + `rake typecheck` + `rake test:coverage` (gates run manually; there is no CI pipeline). Coverage scope is `src` + `lineage` with `fail_under = 85` (see `pyproject.toml`).
>
> The speed layer is the part that was proven on a real cluster, end to end: Kafka → tumbling window → Iceberg → Parquet in MinIO, read back through Polaris by Trino. The batch layer is proven over the SQLite adapter, because the hexagonal core has no Iceberg adapter: `container.storage` is `LakehouseStorageAdapter`, so the medallion drivers never reach the lakehouse, and the Spark jobs' `ICEBERG_*` environment variables are read by nobody. RF-24 (all eight subsystems at once) still needs a host with more RAM than this one has, and is why the demo profile cannot be run here.
>
> Two things make cluster debugging possible at all on a small machine, both added after a run where neither existed: `rake cluster:diagnose` (collects `describe`, events and **the previous container's log**, which a crash-loop otherwise destroys) and `rake stream:probe` (proves the speed layer in one pod, without Kafka, JobManager or TaskManager).

### Known Limitations

Everything below is a deliberate, measured limit of this POC. It is listed so nobody has to discover it in a review.

**Platform and environment**
- The development box has a **5.9 GiB VM with three kind nodes that each advertise 6.0 GiB allocatable** (17.4 GiB of schedulable capacity on 5.9 GiB of memory). Deployments work by **overcommit**. The real bottleneck is not only memory but **I/O**: the container rootfs lives on the Windows filesystem, `etcd`/`kube-apiserver` fall into `D` state, and the API starts answering `EOF` / `TLS handshake timeout` under load. That is why the demo runs as **increments** (`rake ramp:lakehouse`, `ramp:kafka`, `ramp:stream`, `ramp:serving`) that tear down what the next one does not need, and why `RF-24` (all eight subsystems at once) cannot be run here.
- **`Polaris` keeps its catalogue in memory** (`persistence.type: in-memory`). Every restart of the pod empties it, so the tables have to be re-registered with `rake lakehouse:restore` (idempotent, reads the live `metadata.json` from MinIO). Losing the catalogue is therefore cheap but **not automatic**.
- **MinIO runs a single replica on a local-path PVC**: no replication, no backup. It is a single point of failure for the whole lakehouse.
- **No TLS and no authentication** anywhere: Trino, Polaris, MinIO and Kafka all accept anonymous/plaintext traffic inside the cluster. Acceptable for a local POC, not for anything shared.
- Images must be **pre-loaded into the compute node** (`rake ramp:load`) because the nodes run with `imagePullPolicy: IfNotPresent` and the cluster is not meant to reach the registry during a run.
- Stopping a kind node leaves its pods **`Terminating` forever** (no kubelet to confirm the kill). A stuck hook `Job` then blocks the next hook of the same name, which is why `ramp:node:stop` force-deletes `kafka-topic-creator` before stopping the node, and why `ramp_kafka_reset` also deletes the `ControllerRevision`s (a leftover revision makes `currentRevision`/`updateRevision` point at a hash that is not the pod's, so a `StatefulSet` rolling update never fires).

**Measured deviations from the requirements**
- **RF-09**: the aggregate is computed within the 4.5 s window+watermark budget, but it becomes **visible in the lakehouse only at the next checkpoint**: 33.8 s measured end to end (0.8–17.3 s of that is the sink commit itself, on this I/O). The "≤ 5 s" in the requirement holds for computation, not for lakehouse visibility.
- **RF-10**: the replay re-reads the whole log on the cluster, but the **logical idempotency proof was not completed** (`total-records` cannot serve as the oracle on a merge-on-read upsert table). `rake stream:replay:verify` implements the correct check; it needs a stable API for ~10 minutes.
- **RF-12**: only the **stream** half can be reprocessed on the cluster. The batch half cannot, because the hexagonal core has no Iceberg adapter.

**Not delivered (scope decisions)**
- `RF-13`, `RF-14`, `RF-16`, `RF-17`, `RF-18`, `RF-21`, `RF-23`, `RF-25`: product publication pipeline, searchable catalogue UI, access control, admission policies, per-domain quotas, PII enforcement at runtime, observability dashboards and architecture documentation beyond this README. Their manifests exist in the charts; their **enforcement was not verified**.

---

## License & Author Info

- **License**: Distributed under the **MIT License**. See [`LICENSE`](LICENSE) for complete terms.
- **Author & Maintainer**: Sergio Sánchez ([`@sergio11`](https://github.com/sergio11))
- **Project**: Modern Data Architecture Proof of Concept (POC)
