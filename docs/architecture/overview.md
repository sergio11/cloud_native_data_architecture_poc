# Modern Data Architecture POC - Architecture Overview & System Design

---

## Executive Summary

The Modern Data Architecture Proof of Concept (POC) is a **POC-grade, 100% open-source data platform that implements production patterns** (hexagonal architecture, fail-closed quality gates, idempotent ingestion, machine-readable contracts, full traceability). It materialises five paradigms of modern data engineering into one verifiable repository:

1. **Data Mesh**: Decentralized, domain-oriented ownership across sales, marketing, and finance (`domains/**`).
2. **Data Fabric**: Active metadata (OpenLineage events, local audit trail, optional alert service) with automated lineage emission from every use case.
3. **Data Products & Declarative Contracts**: Analytical assets packaged as versioned products bounded by Bitol ODCS-style `contract.yaml` files, validated for BACKWARD compatibility.
4. **Medallion Lakehouse**: Multi-tier curation (Bronze raw, Silver conformed, Gold aggregated). The local profile implements the lakehouse contract over embedded SQLite (append-only + content-hash idempotency + row-level time travel); the K8s profile targets Apache Iceberg on MinIO governed by Apache Polaris.
5. **Hybrid Lambda / Kappa Processing**: Batch layer = full recomputation of Gold from Silver on every run; stream layer = persistent event log with real 5-minute tumbling windows and full log replay; both fused at query time by a single anti-join SQL.

### TWO EXECUTION PROFILES

The platform has two explicit execution profiles. **They share the same ports and use cases (`src/`); only the outbound adapters differ** — the adapters are the swap point.

| | **1. LOCAL PROFILE (default — what runs today)** | **2. K8s PROFILE (`infra/helm/**` + `infra/profiles/**`)** |
| :--- | :--- | :--- |
| **Runtime** | Windows + Python 3.11+ (no containers required) | kind/Kubernetes local cluster (Podman), Helm charts per subsystem |
| **Execution** | `rake demo` (RF-24), `rake test`, and the thin CLI drivers in `pipelines/**` | `scripts/setup.sh` → `scripts/profile-up.sh <profile>` (Helm) |
| **Storage adapter** | SQLite (`data/lakehouse.db`): Bronze **append-only** + content-hash idempotency (RF-01); Silver/Gold/speed layers rewrite via supersede mode (old rows are marked, never deleted); row-version time travel (RF-07); controlled schema evolution | Postgres + MinIO (object storage) + Apache Iceberg tables + Apache Polaris REST catalog (charts `s1-ingestion`, `s2-lakehouse`) |
| **Event log adapter** | SQLite WAL (`data/event_log.db`) with persistent per-consumer cursors and full replay (RF-10); Kafka directly when `KAFKA_BOOTSTRAP_SERVERS` is set — canonical backend with fail-fast, no silent fallback (`src/adapters/streaming/event_stream.py`) | Apache Kafka 3.7 KRaft (chart `s4-stream`) |
| **Quality adapter** | Built-in fail-closed evaluator over **Great-Expectations-format JSON suites** (the GE *library* is not used) | same suites, run inside Dagster/Spark jobs (job definitions in `pipelines/orchestration/**`) |
| **Serving adapter** | ONE SQL with `NOT EXISTS` anti-join over embedded SQL; **Trino when `TRINO_HOST` is set, SQLite otherwise** | Trino over Iceberg/Polaris (chart `s5-serving`, views from `pipelines/serving/unified_view.sql`) |
| **Lineage adapter** | OpenLineage JSON → local audit `data/lineage_audit.jsonl` (source of truth) + best-effort POST to Marquez if reachable | Marquez + lineage alert service (chart `s7-fabric`) |
| **Status** | **This is where all RFs are VERIFIED** — `rake test` executes the automated suite (`tests/unit` + `tests/integration`) | **Deployment target for the same ports backed by full open-source engines** — see "What exists vs. what is wired" below |

**Verification lives in the local profile.** `rake demo` is the single documented end-to-end command (RF-24) and `rake test` executes the automated suite that backs the traceability report in [`docs/traceability-status.md`](../traceability-status.md).

### What exists vs. what is wired in the K8s profile

Verified by inspecting `infra/helm/**` before writing this section:

| Chart | Renders real workloads? | Contents |
| :--- | :---: | :--- |
| `s1-ingestion` | Yes | PostgreSQL (Deployment/Service/PVC) + ingestion `CronJob` |
| `s2-lakehouse` | Yes | MinIO (Deployment/PVC/Service), Apache Polaris (Deployment/Service), bucket-init Job |
| `s3-batch` | Yes | Spark Operator (Deployment/RBAC) + `ScheduledSparkApplication` medallion jobs |
| `s4-stream` | Yes | Kafka KRaft (StatefulSet) + Kafka UI, Flink Operator, `FlinkDeployment` CRs running the same `pipelines/stream/*.py` scripts |
| `s5-serving` | Yes | Trino (Deployment/Service/ConfigMap) + `trino-views-init-job` applying `unified_view.sql` |
| `s6-mesh` | Yes (partially) | `DataProduct`/`DataContract` CRDs + per-domain `ResourceQuota`/`LimitRange` + `NetworkPolicy`. **Kyverno appears only in `values.yaml` — there is no Kyverno template, so admission control is not rendered yet** |
| `s7-fabric` | Yes | Marquez (Deployment/Service) + `lineage-alert-service` (the RF-20 webhook service) |
| `s8-ops` | **No templates** | `values.yaml` (Grafana/Prometheus/Metabase), `dashboards/platform-overview.json`, `prometheus/alerts.yaml` — **assets present, no Kubernetes resources rendered yet** |
| Dagster | **Not in any chart** | `pipelines/orchestration/dagster_repository.py` + `dagster-values.yaml` exist as definitions/values for the upstream chart; nothing in-repo deploys it |

**Rule of thumb used in this document:** anything described as *"chart present, not wired"* above is a **deployment target**, not current behaviour. Current behaviour = the local profile.

---

## 1. The Three Architectural Planes

```
+========================================================================================+
|                                1. ORGANIZATION PLANE                                   |
|  (identical in both profiles: data products are plain files in domains/**)             |
|   +-----------------------+     +-----------------------+    +-----------------------+ |
|   | Domain: Sales         |     | Domain: Marketing     |    | Domain: Finance       | |
|   | - Product: customers  |     | - Product: campaigns  |    | - Product: budgets    | |
|   | - Product: orders     |     | - Product: leads      |    | - Product: closings   | |
|   +-----------------------+     +-----------------------+    +-----------------------+ |
|               \                             |                            /             |
|                +----------------------------+---------------------------+              |
|   +----------------------------------------------------------------------------------+ |
|   | Federated Governance & Contracts: ODCS-style YAML, JSON Schema, BACKWARD rules    | |
|   +----------------------------------------------------------------------------------+ |
+========================================================================================+
|                                2. PROCESSING PLANE                                     |
|                                                                                        |
|   LOCAL PROFILE (default, executes today — Python + SQLite adapters):                  |
|     [OLTP source (PostgreSQL if PGPASSWORD, else synthetic)] --F1--> [Bronze, append+  |
|        content-hash idempotent] --> [Silver: dedup + fail-closed gate] --> [Gold:      |
|        full recomputation + GDPR masking]                                              |
|     [SQLite WAL event log] --F2/F6--> [5-min tumbling windows] --> [speed buffer]      |
|     Serving: ONE SQL with NOT EXISTS anti-join (Trino if TRINO_HOST, else SQLite)      |
|                                                                                        |
|   K8s PROFILE (deployment target — charts in infra/helm, same ports):                  |
|     [PostgreSQL] --Spark jobs--> [MinIO + Iceberg via Polaris]                         |
|     [Kafka KRaft] --Flink jobs--> [Iceberg stream tables]                              |
|     Serving: Trino over Iceberg/Polaris (unified_view.sql)                             |
+========================================================================================+
|                                3. OPERATIONS PLANE                                     |
|   LOCAL: rake tasks (demo/test/...), fail-closed quality engine, lineage ->            |
|          data/lineage_audit.jsonl (+ Marquez if reachable), optional alert service     |
|   K8s:   Marquez + alert service (s7-fabric), CronJobs / ScheduledSparkApplications,   |
|          Prometheus/Grafana assets (s8-ops values only — not templated yet)            |
+========================================================================================+
```

### 1.1 Organization Plane (Data Mesh + Data Fabric)
- **Domain Boundaries**: three autonomous business units — `Sales`, `Marketing`, `Finance` — each owning `domains/<domain>/data-products/<product>/{product.yaml, contract.yaml, schema.json}`.
- **Data Products**: manifests carry owner, description, SLA (freshness/availability), classification, quality thresholds, assets, consumers, and declared lineage steps.
- **Declarative Data Contracts**: `contract.yaml` declares schema fields, compatibility mode, and protected fields. `src/use_cases/validate_contract.py` performs **real BACKWARD compatibility evaluation** of `contract.yaml` vs the sibling `schema.json` snapshot plus optional `jsonschema` structural validation; `governance/contract_validator.py` is a thin CLI delegating to that use case (`rake governance:validate`).
- **Federated Governance**: policies live in `governance/policies/*.yaml` (owner required, PII classification, naming convention) and are enforced by the contract validation use case at repository level. Per-domain compute quotas are enforced ONLY by the `s6-mesh` Helm chart (`infra/helm/s6-mesh/values.yaml` + `templates/domain-quotas.yaml`, the single source of truth — no duplicate quota file under `governance/`). Deployment-time admission (Kyverno) is a K8s-profile concern (chart values only today, RF-17 DEFERRED).

### 1.2 Processing Plane (Local behaviour vs. K8s target)
- **Local profile (executes today)**:
  - **Batch (Lambda)**: `bronze_ingestion.py` → `silver_transform.py` → `gold_aggregation.py` are **pure-Python CLI drivers** over the hexagonal core — they do not run on Spark. Gold is a **full recomputation from Silver on every run** (that *is* the Lambda batch replay; the old `--replay` flag was removed as dead weight).
  - **Stream (Kappa)**: `flink_orders_stream.py` (also pure Python locally) consumes the SQLite WAL log through the `EventBrokerPort`, computes **real 5-minute tumbling windows** (`window_start`/`window_end`), and appends only new events thanks to **persistent consumer cursors**. Full replay from offset 0 regenerates all windows deterministically (RF-10).
  - **Storage**: SQLite adapter implementing the lakehouse contract — Bronze writes are **append-only with content-hash deduplication** (a re-run of identical content writes 0 rows); Silver/Gold/speed layers rewrite their derived tables in **supersede mode** (prior rows are marked `_row_superseded`, never physically deleted, so historical reads stay possible); snapshot bookkeeping (`_lakehouse_snapshots`) with `read_table(version=N)` / `list_versions()` time travel (RF-07).
  - **Serving**: `QueryServingUseCase` builds a single SQL statement that unions batch Gold with stream windows using a `NOT EXISTS` anti-join — **batch wins on `(order_date, status)` collision, stream rows only fill absent keys, no double counting (RF-11)**. The backend is Trino when `TRINO_HOST` is set, embedded SQLite otherwise, and the adapter logs which backend served the query.
- **K8s profile (deployment target, charts present)**: the same ports backed by heavyweight open-source engines — Spark Operator for batch (`s3-batch`), Flink Operator for streaming (`s4-stream`), Trino for serving (`s5-serving`), MinIO + Iceberg + Polaris for storage (`s2-lakehouse`), Postgres as the OLTP source (`s1-ingestion`). **These charts are not wired to the local core**: they run the same script paths as container images, while the local profile runs them directly with Python.

### 1.3 Operations Plane (Active Metadata, Quality & Orchestration)
- **Active Metadata & Lineage**: every use case emits an OpenLineage `COMPLETE`/`FAIL` event. `MarquezLineageAdapter` always appends to `data/lineage_audit.jsonl` (source of truth) and best-effort POSTs to Marquez (`MARQUEZ_URL`, 2s timeout) — failures are logged, never silent.
- **Quality Gates**: `quality/gates/run_quality_gate.py` is a CLI wrapper around `RunQualityGateUseCase`; the fail-closed rules live in the core (`src/adapters/quality/expectation_engine.py`): missing suite → `SuiteNotFoundError`, empty data / zero assertions / unknown expectation type → **BLOCKED**. Suites are **Great-Expectations-format JSON** (`quality/expectations/silver_orders_suite.json`, `silver_customers_suite.json`) evaluated by the built-in engine — the Great Expectations *library* itself is not installed or invoked.
- **Orchestration**: locally, `rake` tasks are the observable, on-demand orchestrator (`rake medallion:all`, `rake stream:orders`, `rake serving:query`, `rake demo`). Schedules exist as Dagster `ScheduleDefinition`s (`pipelines/orchestration/dagster_repository.py`) and K8s `CronJob`/`ScheduledSparkApplication` templates — chart/definition level, not exercised by the local profile.
- **Alerting (RF-20)**: `lineage/config/lineage_webhook_service.py` is a standalone FastAPI service evaluating four active-metadata rules (schema change, quality breach, pipeline failure, SLA freshness) with `lineage/config/alert-rules.yaml`; it is deployable via `s7-fabric` but **not part of the `rake demo` flow**.
- **Telemetry**: Prometheus/Grafana/Metabase values and a dashboard JSON exist under `infra/helm/s8-ops` — **assets only, the chart has no templates yet**, so no dashboard is deployed today.

---

## 2. Subsystem Breakdown (S1 through S8)

| Subsystem | Responsibility | Local profile (runs today) | K8s profile target (charts) |
| :--- | :--- | :--- | :--- |
| **S1: Ingestion & Sources** | Operational snapshot + domain events | `src/adapters/source/oltp_source.py` (PostgreSQL when `PGPASSWORD` is set, otherwise a loud synthetic fallback) + `data-generator/` scripts | `s1-ingestion`: PostgreSQL + ingestion `CronJob` |
| **S2: Lakehouse & Catalog** | ACID storage, versioning, schema evolution | `src/adapters/storage/lakehouse_storage.py` (SQLite, content-hash idempotency, row-version time travel) | `s2-lakehouse`: MinIO + Apache Polaris (+ Iceberg tables written by the jobs) |
| **S3: Batch compute (Lambda)** | Bronze → Silver → Gold | Pure-Python drivers `pipelines/medallion/*.py` over the core use cases; Gold = full recomputation | `s3-batch`: Spark Operator + `ScheduledSparkApplication` running the same scripts |
| **S4: Stream compute (Kappa)** | Windows over the event log | `pipelines/stream/flink_orders_stream.py` over `EventStreamAdapter` (SQLite WAL log, cursors, replay) | `s4-stream`: Kafka KRaft + Flink Operator + `FlinkDeployment` CRs |
| **S5: Serving layer** | One query, no double counting | `pipelines/serving/run_serving_query.py` → single anti-join SQL; Trino if `TRINO_HOST`, else SQLite | `s5-serving`: Trino + `trino-views-init-job` (`unified_view.sql`) |
| **S6: Data Mesh & Contracts** | Declarative products & contracts | `domains/**` manifests + `ValidateContractUseCase` (`rake governance:validate`) | `s6-mesh`: `DataProduct`/`DataContract` CRDs, per-domain quotas & network policies (Kyverno: values only) |
| **S7: Data Fabric & Governance** | Lineage, quality, policy | OpenLineage JSON → `data/lineage_audit.jsonl` (+ Marquez if up); fail-closed quality engine; GDPR masking in Gold | `s7-fabric`: Marquez + `lineage-alert-service` (RF-20 rules) |
| **S8: Orchestration & Observability** | Schedules, state, dashboards | `rake` tasks (observable, on-demand); Dagster definitions in `pipelines/orchestration/**` (not running locally) | `s8-ops`: Grafana/Prometheus/Metabase values + dashboard/alert assets — **no templates yet** |

### Detailed Notes per Subsystem (local reality)

#### S1: Ingestion & Sources
- **OLTP source**: reads `orders`, `customers`, `campaigns`. If `PGPASSWORD` is unset (RNF-07: no embedded credentials) PostgreSQL is treated as unavailable and a **synthetic generator logs loudly** that it is serving demo records. Table names are validated against a SQL allowlist.
- **Event log**: `EventStreamAdapter` persists `{offset, topic, key, payload, published_at}` in SQLite WAL, keeps a `consumer_offsets` cursor per consumer group, and **never fabricates events** when the log is empty. `pipelines/stream/flink_orders_stream.py` auto-seeds a deterministic 14-event demo burst **only when the log is empty, with a loud warning log line** (kept because `rake demo`/`scripts/demo.sh` expect a populated speed buffer).

#### S2: Lakehouse Storage & Catalog
- Single SQLite database (`data/lakehouse.db`, WAL) hosting all logical 3-part names (`bronze.*`, `silver.*`, `gold.*`, `stream.*`) as normalized physical tables.
- Versioning: every successful write records a snapshot in `_lakehouse_snapshots` and stamps rows with `_row_version`; superseded rows keep `_row_superseded` so historical reads stay possible — the RF-07 equivalent of Iceberg snapshots.
- The Iceberg/MinIO/Polaris combination is the **K8s profile target** (`s2-lakehouse` chart).

#### S3: Batch Compute (Lambda)
- `bronze_ingestion.py`, `silver_transform.py`, `gold_aggregation.py` are **thin argparse drivers** that call `container.<use_case>.execute(...)`. They run with plain `python` — **no Spark is involved in the local profile**.
- Silver applies deduplication by primary key and then the fail-closed quality gate; a breach raises `QualityGateBreachError`, emits a lineage `FAIL` event, and exits non-zero (`rake medallion:silver`).
- Gold recomputes `gold.sales.daily_metrics` and `gold.sales.customers` from Silver on every run and applies GDPR pseudonymization (SHA-256 email hash, name masking) — these two tables are the **only** Gold outputs; no marketing or finance Gold tables are produced by the local core. Each gate is enforced by the required suites `quality/expectations/gold_daily_metrics_suite.json` / `gold_customers_suite.json` (a missing suite blocks the run).

#### S4: Stream Compute (Kappa)
- 5-minute tumbling windows keyed by event time with `window_start`/`window_end` boundaries (exclusive end), aggregating orders, revenue and average order value per `(window, status)`.
- Incremental mode consumes only offsets above the committed cursor and appends (no duplicate windows on re-run); `--start-offset earliest` / `--replay` triggers the **full Kappa replay**: recompute every window from offset 0 and overwrite the speed table deterministically (`rake stream:replay`).

#### S5: Serving Layer
- One SQL statement unions `gold.sales.daily_metrics` (batch) with `stream.sales.orders_windowed` (stream) using `WHERE NOT EXISTS (... b.order_date = s.order_date AND b.status = s.status)` — batch wins on collision.
- `pipelines/serving/unified_view.sql` expresses the identical semantics for Trino (`iceberg.serving.sales_unified_revenue`) when the K8s profile is up.

#### S6: Data Mesh & Data Products
- Six data products across three domains (orders, customers, campaigns, leads, financial_closings, budgets), each with `product.yaml` + `contract.yaml` + `schema.json`.
- Validation aggregates **all** errors (never early-returns) and reports them deterministically: structure, governance JSON schema, declared compatibility mode, and BACKWARD evolution against `schema.json`.

#### S7: Data Fabric & Governance
- Lineage: `emit_job_event` is called by every use case (Bronze/Silver/Gold/stream/quality-gate), including `FAIL` on gate breach.
- Quality: GE-format suites evaluated by the built-in engine; `rake quality:gate[suite,table,threshold]` runs a standalone gate with exit codes 0/1/2 (pass / blocked / execution error).

#### S8: Orchestration & Observability
- Local: `rake -T` lists every runnable task; `rake demo` chains preflight → contracts → bronze → silver → gold → stream → serving → tests.
- Dagster: asset definitions and cron schedules exist as code (`daily_batch_schedule = 0 2 * * *`, `hourly_ingestion_schedule = 0 * * * *`); the Dagster quality resource **delegates to the core use case** (`container.run_quality_gate.execute`), verified by `tests/unit/test_catalog_manifest.py::test_quality_resource_delegates`.

---

## 3. End-to-End Data Flows (F1 to F6)

```
  +------------------+         +-------------------+         +--------------------+
  | OLTP source      | --F1--> | bronze_ingestion  | --F1--> | Bronze (append-only |
  | (PG or synthetic)|         | (pure Python)     |         |  + content hash)    |
  +------------------+         +-------------------+         +--------------------+
                                         |                             |
                                        F3                            F3
                                         v                             v
  +------------------+         +-------------------+         +--------------------+
  | SQLite WAL event | --F2--> | flink_orders_     | --F2--> | stream.*.          |
  | log (cursors)    |         | stream (Python)   |         | orders_windowed    |
  +------------------+         +-------------------+         +--------------------+
           |                            |                             |
          F6                     F2/F6  |                             F3
           v                            v                             v
  +------------------+         +-------------------+         +--------------------+
  | Full log replay  |         | 5-min tumbling    |         | Gold (full         |
  | (earliest offset)|         | windows           |         |  recomputation +   |
  +------------------+         +-------------------+         |  GDPR masking)     |
                                          \                           /
                                           \                         /
                                            +-----------+-----------+
                                                        |
                                                       F4
                                                        v
                                         +-----------------------------+
                                         | ONE SQL: anti-join          |
                                         | (batch wins, no double      |
                                         |  counting) — Trino if       |
                                         |  TRINO_HOST else SQLite     |
                                         +-----------------------------+
                                                        ^
                                                        |
                                          (F5: OpenLineage & quality)
                                                        |
                                         +-----------------------------+
                                         | data/lineage_audit.jsonl    |
                                         | (+ Marquez if reachable)    |
                                         +-----------------------------+
```

### Flow F1: Batch Ingestion (OLTP -> Bronze) — append, never overwrite
1. **Trigger**: `rake medallion:bronze` (or `python pipelines/medallion/bronze_ingestion.py --entity all`).
2. **Read**: `OltpSourceAdapter.extract_table` — PostgreSQL over JDBC when `PGPASSWORD` is provided, otherwise the embedded synthetic generator (logged loudly).
3. **Metadata Enrichment**: audit columns `_ingested_at` (UTC), `_source` (e.g. `oltp.public.orders`), `_batch_id` (UUID per run).
4. **Write**: `write_table(..., mode="append")` inserts only rows whose SHA-256 content hash is not already present. Re-running the same snapshot writes **0 rows** (`idempotent_hit=True`) — Bronze is append-only (RF-01), prior batches stay readable.
5. **Lineage**: an OpenLineage `COMPLETE` event is appended to `data/lineage_audit.jsonl` (and POSTed to Marquez if reachable).

### Flow F2: Real-Time Stream Ingestion (event log -> speed buffer)
1. **Produce**: events land in the SQLite WAL log (`publish_event`); `flink_orders_stream.py` **auto-seeds 14 deterministic demo events only when the log is empty**, logging a warning that says so.
2. **Consume**: `EventStreamAdapter.consume_events` returns only offsets above the `speed_layer` consumer cursor and advances it atomically — repeated runs never duplicate windows.
3. **Windowing**: 5-minute tumbling windows over event time (`window_start`/`window_end`, exclusive end) aggregating order counts, revenue and AOV per `(window, status)`.
4. **Sink**: windows are appended to `stream.sales.orders_windowed`.

### Flow F3: Medallion Progression (Bronze -> Silver -> Gold) — fail-closed
1. **Silver Transformation**: `silver_transform.py` reads Bronze, deduplicates by primary key (`id`), strips internal audit columns, and **runs the quality gate before promoting**.
2. **Fail-closed Quality Gate**: the suite path defaults to `quality/expectations/silver_<entity>_suite.json` (i.e. `silver_orders_suite.json` / `silver_customers_suite.json`). Threshold precedence: per-dimension contract thresholds (`spec.quality.thresholds`) govern the verdict via `evaluate_and_enforce`; the explicit `--threshold` (or its `0.95` default) is only a fallback when the contract is silent, and contract dimensions the suite does not measure are skipped. A **missing suite (`SuiteNotFoundError`), an unknown expectation type, an empty table, or a breached threshold ⇒ promotion is BLOCKED**: `QualityGateBreachError` is raised, a lineage `FAIL` event is emitted, and the CLI exits `1`. Nothing is written to Silver.
3. **Gold Aggregation**: `gold_aggregation.py` performs a **full recomputation from Silver on every run** — this *is* the Lambda batch replay (there is no `--replay` flag on Gold; it was removed as dead weight).
4. **GDPR PII Masking**: emails are irreversibly hashed with SHA-256 (`email_hash`) and names are masked to first name + initial (`"Alice S."`) in `gold.sales.customers`; the plaintext `name`/`email` columns are dropped (`src/domain/privacy.py`).
5. **Commit**: results land in `gold.sales.daily_metrics` and `gold.sales.customers`, each write versioned for time travel.

### Flow F4: Unified Serving Layer (ONE SQL, anti-join)
1. **Client request**: `rake serving:query` (or `python pipelines/serving/run_serving_query.py --limit 10`).
2. **Single statement**: `QueryServingUseCase._build_unified_sql` unions batch Gold with stream windows and adds `WHERE NOT EXISTS (SELECT 1 FROM gold.sales.daily_metrics b WHERE b.order_date = s.order_date AND b.status = s.status)`.
3. **Rule**: for the same `(order_date, status)` key the **exact batch value wins**; stream rows only fill keys absent from batch, so **no key is ever counted twice (RF-11)**.
4. **Backend**: Trino when `TRINO_HOST` is set, embedded SQLite otherwise; the adapter logs `Serving query executed by backend=...` and SQL errors raise (never swallowed).

### Flow F5: Lineage Harvesting & Quality Enforcement
1. **Event Emission**: every use case emits OpenLineage JSON (job, inputs, outputs, status) through `LineagePublisherPort`.
2. **Local audit first**: `data/lineage_audit.jsonl` is the source of truth; the Marquez HTTP POST is best-effort (2s timeout, failures logged as warnings).
3. **Breach Handling**: a failed gate emits an OpenLineage `FAIL` event for `silver_clean_<entity>` before raising, and `run_quality_gate.py` exits `1` (blocked) or `2` (execution error, e.g. missing suite).

### Flow F6: Kappa Reprocessing & Historical Replay
1. **Replay trigger**: `rake stream:replay` (equivalently `--start-offset earliest` or `--replay` on `flink_orders_stream.py`) rewinds to offset 0.
2. **Deterministic recompute**: the full log is re-read, every tumbling window is recomputed, and the speed table is overwritten idempotently — repeated replays produce identical results with no row growth.
3. **Cursor safety**: replay does **not** advance consumer cursors, so incremental consumption resumes exactly where it left off.
4. **Batch side**: Gold has no replay flag because **every Gold run is already a full recomputation** of the historical aggregate from Silver.

---

## 4. Component Interaction Matrix (local profile)

| Source Component | Target Component | Protocol / Medium | Purpose / Payload |
| :--- | :--- | :--- | :--- |
| `rake` (Rakefile) | `pipelines/**` CLI drivers | Process invocation | RF-24 demo, per-layer tasks, tests |
| `bronze_ingestion.py` | PostgreSQL / synthetic source | psycopg2 (env `PGPASSWORD`) / in-process | Relational snapshot extraction |
| `bronze_ingestion.py` | `data/lakehouse.db` | SQLite (WAL) | Append-only Bronze + audit columns |
| `flink_orders_stream.py` | `data/event_log.db` | SQLite (WAL) | Consume events, advance `speed_layer` cursor |
| `silver_transform.py` / `gold_aggregation.py` | `data/lakehouse.db` | SQLite (WAL) | Promote Silver (gate), recompute Gold |
| Any use case | `data/lineage_audit.jsonl` | Local JSONL append | OpenLineage audit trail (source of truth) |
| Any use case | Marquez (optional) | HTTP `POST /api/v1/lineage` (`MARQUEZ_URL`) | Best-effort lineage graph ingestion |
| `run_quality_gate.py` | quality suites | JSON files in `quality/expectations/` | GE-format suites evaluated by built-in engine |
| `run_serving_query.py` | Trino or embedded SQLite | `TRINO_HOST` env / local file | Single anti-join serving query (RF-11) |
| `governance/contract_validator.py` | `domains/**` manifests | Filesystem scan | BACKWARD contract validation (RF-15) |

**K8s profile (target, not wired to the local core):** Spark/Flink jobs → Polaris REST `8181` and MinIO S3 `9000`; Trino → Polaris + MinIO; all engines → Marquez `5000`; `s8-ops` Prometheus scrapes (assets only, chart has no templates).

---

## 5. Memory and Resource Budget (8 GB RAM Constraint)

To satisfy **Constraint RC-02** (strict 8 GB RAM limit), the K8s profile activates subsystems exclusively through four profile files — **`infra/profiles/{platform,stream,batch,demo}.yaml`**, applied by `scripts/profile-up.sh <profile>` (Helm `-f`) and reverted by `scripts/profile-down.sh`:

| Profile | File | Target RAM | Subsystems enabled |
| :--- | :--- | :---: | :--- |
| **platform** | `infra/profiles/platform.yaml` | 3,072 MB | Charts `s1-ingestion`, `s2-lakehouse`, `s7-fabric` via `scripts/profile-up.sh platform` (Spark/Flink/Trino/Dagster disabled; `s6-mesh`/`s8-ops` charts deploy only under `demo` — DEFERRED for this branch) |
| **stream** | `infra/profiles/stream.yaml` | 6,144 MB | platform + Flink operator & jobs (orders + clickstream), Kafka, Prometheus/Grafana |
| **batch** | `infra/profiles/batch.yaml` | 6,656 MB | platform + Spark operator + Trino + Dagster definitions |
| **demo** | `infra/profiles/demo.yaml` | 7,424 MB | all subsystems S1–S8 in single-replica lightweight mode under a 7.5 GB ceiling |

Each profile sets per-container `requests`/`limits` (CPU and memory) for every subsystem, so batch and stream engines are never fully resident at once. **The local profile needs none of this** — it runs the hexagonal core directly with Python and SQLite (single process, tens of MB), which is the default way to verify the RFs on Windows today.

> Note: these numbers are the *declared targets* in the profile files (real, verified in this repository). Measuring actual consumption requires the kind cluster (`scripts/setup.sh`), which is a K8s-profile activity — see [`docs/traceability-status.md`](../traceability-status.md) for what has actually been verified.
