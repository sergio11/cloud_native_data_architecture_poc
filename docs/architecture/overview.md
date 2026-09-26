# Modern Data Architecture POC - Architecture Overview & System Design

---

## Executive Summary

The Modern Data Architecture Proof of Concept (POC) is a production-grade, 100% open-source data engineering reference platform. It unifies five transformative paradigms of modern data engineering into an operational reference implementation:
1. **Data Mesh**: Decentralized, domain-oriented ownership and data architecture across sales, marketing, and finance.
2. **Data Fabric**: Active metadata management, automated end-to-end lineage harvesting, and dynamic policy-driven quality gates.
3. **Data Products & Declarative Contracts**: Treating analytical data assets as versioned products bounded by formal contracts adhering to the Bitol Open Data Contract Standard (ODCS).
4. **Medallion Lakehouse**: Multi-tier curation (Bronze raw, Silver conformed, Gold aggregated) utilizing open columnar storage (Apache Iceberg) on S3-compatible object storage (MinIO) governed by an open REST catalog (Apache Polaris).
5. **Hybrid Lambda / Kappa Processing**: Combining high-throughput distributed batch processing (Apache Spark) for historical depth with event-driven stream processing (Apache Flink) for real-time responsiveness, unified seamlessly at query time via Trino.

The entire architecture is designed to execute locally on standard developer hardware constrained to an **8 GB RAM memory budget** through orchestrated execution profiles.

---

## 1. The Three Architectural Planes

Modern enterprise data architectures suffer from severe fragmentation when data governance, compute engines, and operations are decoupled without cohesive structure. This platform establishes clear separation of concerns across three orthogonal planes:

```
+========================================================================================+
|                                1. ORGANIZATION PLANE                                   |
|                                                                                        |
|   +-----------------------+     +-----------------------+    +-----------------------+ |
|   | Domain: Sales         |     | Domain: Marketing     |    | Domain: Finance       | |
|   | - Product: customers  |     | - Product: campaigns  |    | - Product: revenue    | |
|   | - Product: orders     |     | - Product: leads      |    | - Product: c360_gold  | |
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

### 1.1 Organization Plane (Data Mesh + Data Fabric)
The Organization Plane addresses the socio-technical challenges of data management:
- **Domain Boundaries**: Data ownership is decentralised into three autonomous business units: `Sales`, `Marketing`, and `Finance`. Each domain owns its operational data models, transformation logic, and release lifecycles.
- **Data Products**: Analytical outputs are packaged as formal products containing source code, metadata descriptors (`product.yaml`), schemas (`schema.json`), and SLA definitions.
- **Declarative Data Contracts**: Products publish explicit contractual interfaces (`contract.yaml`) adhering to the Bitol Open Data Contract Standard. Contracts specify field types, nullability, uniqueness, allowed ranges, and privacy classifications. Schema evolution rules strictly enforce backward compatibility.
- **Federated Governance**: Governance rules (e.g., GDPR PII protection, access rights) are defined globally by data stewards but implemented locally by domain teams within their transformation code.

### 1.2 Processing Plane (Dual-Engine Compute & Lakehouse Tiers)
The Processing Plane executes compute and storage operations:
- **Decoupled Architecture**: Stateless compute engines (Spark, Flink, Trino) operate independently from the shared storage tier (MinIO + Apache Iceberg).
- **Batch Processing**: Apache Spark acts as the heavy batch engine, reading relational snapshots from PostgreSQL, structuring raw data into the Bronze tier, transforming and validating records into Silver, and producing aggregated dimensional models in Gold.
- **Stream Processing**: Apache Flink acts as the low-latency stream engine, consuming real-time events from Kafka, performing stateful tumbling-window aggregations (5-minute windows), and writing streaming buffers directly to Iceberg.
- **Lakehouse Storage**: Apache Iceberg v2 table format provides ACID transactional guarantees, atomic snapshot commits, partition evolution, and time-travel querying.
- **Serving Layer**: Trino presents an interactive ANSI SQL interface over the Lakehouse via the Apache Polaris REST catalog. Trino evaluates unified views that combine historical batch data with real-time stream data without duplication.

### 1.3 Operations Plane (Active Metadata, Quality & Orchestration)
The Operations Plane ensures system integrity, observability, and automated execution:
- **Active Metadata & Automated Lineage**: Spark and Flink runtime listeners intercept job lifecycle events and transmit OpenLineage payloads to Marquez over HTTP, generating a real-time, interactive dataset lineage graph.
- **Automated Quality Gates**: Data transformations invoke Great Expectations validation suites (`run_quality_gate.py`). Validation checks verify completeness, uniqueness, enum validity, and numeric ranges. If a validation score fails to meet the contractual threshold (e.g., 95%), the pipeline halts immediately, preventing bad data from contaminating downstream tiers.
- **Asset-Based Orchestration**: Dagster orchestrates pipeline dependencies using Software-Defined Assets (SDAs), tracking asset materialization history and execution lineage.
- **System Telemetry**: Prometheus scrapes resource consumption and JVM metrics; Grafana visualizes pipeline latencies, record counts, and quality metrics.

---

## 2. Subsystem Breakdown (S1 through S8)

```
+--------------------------------------------------------------------------------------------------+
|                                    SUBSYSTEM COMPONENT MAP                                       |
+----------------------+-----------------------------+---------------------------------------------+
| Subsystem ID & Name  | Primary Technologies        | Architectural Responsibility                |
+----------------------+-----------------------------+---------------------------------------------+
| S1: Ingestion &      | PostgreSQL 16-alpine        | Synthetic e-commerce transactional database |
|     Sources          | Apache Kafka 3.7 (KRaft)    | High-throughput distributed event bus       |
|                      | Python (Faker, Click)       | Seed data generation & clickstream producer |
+----------------------+-----------------------------+---------------------------------------------+
| S2: Lakehouse &      | MinIO Object Storage        | S3-compatible persistent storage layer      |
|     Catalog          | Apache Iceberg v2 Spec      | Open ACID table format with time travel     |
|                      | Apache Polaris REST Catalog | Unified multi-engine catalog service        |
+----------------------+-----------------------------+---------------------------------------------+
| S3: Batch Compute    | Apache Spark 3.5.1 (PySpark)| Distributed ETL, cleansing & dimensional agg|
+----------------------+-----------------------------+---------------------------------------------+
| S4: Stream Compute   | Apache Flink 1.18 (PyFlink) | Stateful event-time windowing & streaming   |
+----------------------+-----------------------------+---------------------------------------------+
| S5: Serving Layer    | Trino SQL Query Engine 442+ | Interactive federated SQL & unified views   |
+----------------------+-----------------------------+---------------------------------------------+
| S6: Data Mesh &      | Bitol ODCS v2 (YAML)        | Declarative contracts & product manifests   |
|     Contracts        | JSON Schema Draft-07        | Structural field-level validation schemas   |
+----------------------+-----------------------------+---------------------------------------------+
| S7: Data Fabric &    | OpenLineage Specification   | Active metadata standard                    |
|     Governance       | Marquez 0.42 Service        | Interactive lineage DAG & dataset tracking  |
|                      | Great Expectations 0.18     | Declarative data quality testing gates      |
|                      | Kyverno / OPA Policies      | Policy-as-code admission & security rules   |
+----------------------+-----------------------------+---------------------------------------------+
| S8: Orchestration &  | Dagster 1.6+                | Asset-based DAG execution & scheduling      |
|     Observability    | Prometheus 2.50 & Grafana   | Operational metrics & telemetry dashboards  |
+----------------------+-----------------------------+---------------------------------------------+
```

### Detailed Subsystem Rationale

#### S1: Ingestion & Sources Subsystem
- **PostgreSQL 16**: Chosen as the transactional OLTP engine because it mirrors enterprise relational database standards (ACID transactions, relational constraints, foreign keys). `oltp_seed.py` creates realistic Spanish e-commerce entities (`clientes`, `pedidos`, `lineas_pedido`, `campanas`) using deterministic seed data.
- **Apache Kafka 3.7**: Operating in KRaft mode eliminates Apache ZooKeeper, conserving ~300 MB of system RAM. `event_producer.py` streams synthetic clickstream (`ventas.navegacion.raw`) and order status events (`ventas.pedidos.events`) using token-bucket rate limiting to prevent consumer starvation.

#### S2: Lakehouse Storage & Catalog Subsystem
- **MinIO**: High-performance, S3A-compatible local object storage providing persistent multi-tenant bucket infrastructure (`lakehouse`). Eliminates dependency on proprietary cloud buckets (Amazon S3, Google Cloud Storage).
- **Apache Iceberg**: Solves the critical limitations of standard file-based data lakes (such as directory renames and eventual consistency bugs) by maintaining atomic metadata trees. Iceberg provides snapshot isolation, schema evolution without rewriting data, partition evolution, and instant time travel.
- **Apache Polaris**: An incubating Apache project providing an open-source, vendor-neutral implementation of the Iceberg REST Catalog specification. Allows Spark, Flink, and Trino to discover and coordinate table updates through an open REST protocol over port `8181`.

#### S3: Batch Compute Engine Subsystem
- **Apache Spark 3.5.1**: The industry benchmark for distributed large-scale batch processing. Spark reads relational tables via JDBC, executes Medallion transformations (`bronze_ingestion.py`, `silver_transform.py`, `gold_aggregation.py`), applies window-based deduplication, computes multidimensional aggregates, and writes Parquet files directly to Iceberg.

#### S4: Stream Compute Engine Subsystem
- **Apache Flink 1.18.1**: Selected over micro-batching solutions for its native pipelined execution engine. Flink processes clickstream records individually with sub-second latency, handles late-arriving events through watermarks, maintains state in RocksDB, and commits 5-minute window aggregations into Iceberg stream tables.

#### S5: Serving Layer Subsystem
- **Trino 442+**: A distributed MPP query engine specifically optimized for interactive SQL queries. Trino connects to the Polaris REST catalog and executes federated queries across batch and streaming Iceberg tables. View `serving.ventas.revenue_unified` joins historical Gold batch partitions with active streaming buffers via `UNION ALL`.

#### S6: Data Mesh & Data Products Subsystem
- **Bitol Open Data Contract Standard (ODCS)**: Eliminates schema drift and undocumented upstream changes by formalizing producer-consumer agreements. Contracts defined in `domains/*/data-products/*/contract.yaml` specify schemas, SLAs, data quality rules, and consumer registrations.

#### S7: Data Fabric & Governance Subsystem
- **OpenLineage & Marquez**: Automated metadata collection standard that instruments Spark and Flink jobs. When a pipeline executes, OpenLineage emits runtime metadata (inputs, outputs, run state, schema) to Marquez, eliminating manual documentation of pipeline dependencies.
- **Great Expectations**: Declarative quality test framework. `run_quality_gate.py` runs suites (`silver_pedidos_suite.json`, `silver_clientes_suite.json`) before committing records, blocking invalid data from reaching Gold.

#### S8: Orchestration & Observability Subsystem
- **Dagster**: Asset-based orchestration engine. Unlike task-based schedulers (Airflow), Dagster models data engineering workflows as Software-Defined Assets (SDAs), aligning directly with the Data Products paradigm.
- **Prometheus & Grafana**: Collects container metrics and engine JMX telemetry, providing real-time dashboards for system health and memory usage.

---

## 3. End-to-End Data Flows (F1 to F6)

```
  +------------------+         +-------------------+         +--------------------+
  | PostgreSQL OLTP  | --F1--> | Apache Spark      | --F1--> | Iceberg Bronze     |
  +------------------+         +-------------------+         +--------------------+
                                         |                             |
                                        F3                            F3
                                         v                             v
  +------------------+         +-------------------+         +--------------------+
  | Apache Kafka Bus | --F2--> | Apache Flink      | --F2--> | Iceberg Silver     |
  +------------------+         +-------------------+         +--------------------+
           |                             |                             |
          F6                            F2                            F3
           v                             v                             v
  +------------------+         +-------------------+         +--------------------+
  | Historical Replay|         | Stream Buffer     |         | Iceberg Gold (PII) |
  +------------------+         +-------------------+         +--------------------+
                                         \                             /
                                          \                           /
                                           +------------+------------+
                                                        |
                                                       F4
                                                        v
                                         +-----------------------------+
                                         | Trino Serving Layer (Views) |
                                         +-----------------------------+
                                                        ^
                                                        |
                                          (F5: OpenLineage & Quality)
                                                        |
                                         +-----------------------------+
                                         | Marquez Lineage & GE Gates  |
                                         +-----------------------------+
```

### Flow F1: Batch Ingestion (PostgreSQL -> Iceberg Bronze)
1. **Trigger**: Scheduled Dagster asset run or manual execution of `python pipelines/medallion/bronze_ingestion.py`.
2. **Read**: PySpark initializes an Iceberg-enabled session and connects to PostgreSQL over JDBC (`postgresql://postgres:5432/ecommerce`).
3. **Metadata Enrichment**: Spark reads source tables (`orders`, `customers`, `campaigns`), appends technical lineage attributes:
   - `_ingested_at`: UTC timestamp of the ingestion run.
   - `_source`: Origin identifier (e.g., `postgresql.public.orders`).
   - `_batch_id`: Unique UUID generated for the execution.
4. **Write**: Records are written into Iceberg tables (`bronze.sales.orders`, `bronze.sales.customers`) on MinIO using append mode.
5. **Lineage**: Spark emits an OpenLineage `COMPLETE` event to Marquez (`http://marquez:5000`).

### Flow F2: Real-Time Stream Ingestion (Kafka -> Flink -> Stream Buffer)
1. **Produce**: `event_producer.py` continuously publishes JSON messages to Kafka topics `sales.orders.events` and `marketing.clickstream.raw`.
2. **Consume**: `flink_orders_stream.py` consumes the stream using event-time timestamps and a 30-second bounded out-of-orderness watermark.
3. **Windowing**: Flink assigns events to 5-minute tumbling windows and calculates order counts and gross merchandise value (GMV).
4. **Sink**: Windowed aggregates are committed into Iceberg streaming tables (`stream.sales.orders_windowed`) on MinIO.

### Flow F3: Medallion Progression (Bronze -> Silver -> Gold)
1. **Silver Transformation**: `silver_transform.py` reads new records from Bronze. It trims string whitespace, casts columns to contractual datatypes, removes duplicate records via window ranking over primary keys, and validates foreign key integrity between `orders.customer_id` and `customers.id`.
2. **Quality Gate Verification**: Before committing to Silver, `run_quality_gate.py` evaluates Great Expectations test suites (`silver_orders_suite.json`). If the validation score is < 0.95, the pipeline aborts.
3. **Gold Aggregation**: Upon passing quality gates, `gold_aggregation.py` reads Silver. It computes business summaries (daily revenue by status, campaign ROI).
4. **GDPR PII Masking**: In compliance with RC-05, customer PII is pseudonymized: emails are hashed with SHA-256 (`hashlib.sha256(email.encode()).hexdigest()`), customer names are truncated to initials ("John D."), and registration dates are converted to year-only.
5. **Commit**: Aggregations are written to Iceberg Gold tables (`gold.sales.daily_metrics`).

### Flow F4: Unified Serving Layer (Trino SQL)
1. **Client Request**: An analytical tool or business user submits a SQL query:
   ```sql
   SELECT * FROM serving.sales.unified_revenue WHERE order_date >= CURRENT_DATE - INTERVAL '7' DAY;
   ```
2. **View Evaluation**: Trino evaluates the view definition in `pipelines/serving/unified_view.sql`.
3. **Partition Pruning & Union**:
   - Historical records older than 2 days are fetched from `gold.sales.daily_metrics` (batch tier).
   - Recent records from the last 2 days are fetched from `stream.sales.orders_windowed` (streaming tier).
4. **Result**: A unified, seamless time series is returned to the user with zero duplication and sub-second query latency.

### Flow F5: Lineage Harvesting & Quality Enforcement
1. **Event Emission**: At job initiation and completion, Spark and Flink runtime listeners transmit OpenLineage JSON payloads to Marquez.
2. **Graph Construction**: Marquez constructs the end-to-end directed acyclic graph mapping inputs, transformations, and output datasets.
3. **Breach Handling**: If `run_quality_gate.py` detects a critical failure, it logs an error, emits a failure event to Marquez, fires an alert webhook, and exits with code `1`, halting downstream execution.

### Flow F6: Kappa Reprocessing & Historical Replay
1. **Replay Trigger**: When business logic changes or historical backfill is required, the operator runs `gold_aggregation.py` with the `--replay` flag, or resets Kafka consumer group offsets to `earliest`.
2. **Deterministic Processing**: The pipeline reprocesses the historical data stream through the exact same logic.
3. **Idempotent Overwrite**: Iceberg snapshot isolation ensures that downstream tables are updated atomically and idempotently without corrupting operational state.

---

## 4. Component Interaction Matrix

The following matrix documents the communication protocols, source/target relationships, and operational dependencies across all components:

| Source Component | Target Component | Protocol / Port | Purpose / Payload |
| :--- | :--- | :--- | :--- |
| `oltp_seed.py` | PostgreSQL | TCP `5432` | Synthetic relational seed data (psycopg2) |
| `event_producer.py` | Apache Kafka | TCP `9092` | Synthetic order and navigation events (JSON) |
| Spark Batch (`bronze_ingestion`) | PostgreSQL | JDBC `5432` | Relational table snapshot extraction |
| Spark Batch (`bronze_ingestion`) | MinIO S3 | S3A `9000` | Write raw data snapshots to Iceberg Bronze |
| Spark Batch / Flink | Apache Polaris | REST `8181` | Fetch and commit Iceberg table metadata |
| Flink Stream (`flink_orders`) | Apache Kafka | TCP `9092` | Consume real-time event topics |
| Flink Stream (`flink_orders`) | MinIO S3 | S3A `9000` | Write windowed aggregates to Iceberg Stream |
| Spark / Flink Jobs | Marquez | HTTP `5000` | Emit OpenLineage runtime lifecycle events |
| `run_quality_gate.py` | MinIO / Trino | JDBC / S3A | Execute Great Expectations validation suites |
| Trino Query Engine | Apache Polaris | REST `8181` | Discover schemas, namespaces, and table metadata |
| Trino Query Engine | MinIO S3 | S3A `9000` | Read Parquet data files across Bronze, Silver, Gold |
| Dagster Orchestrator | Spark / Flink / Scripts| Process / API | Trigger Software-Defined Asset materializations |
| Prometheus | All Services | HTTP Scraping | Collect system, container, and JVM metrics |
| Grafana | Prometheus | HTTP `9090` | Query telemetry metrics for dashboard visualization |

---

## 5. Memory and Resource Budget Breakdown (8 GB RAM Constraint)

To satisfy **Constraint RC-02** (strict 8 GB RAM memory limit on developer workstations), the platform enforces hard container memory limits (`mem_limit`) and divides workloads into four operational startup profiles:

```
+-----------------------------------------------------------------------------------------+
|                                8 GB RAM ALLOCATION BUDGET                               |
+--------------------------+--------------------+-------------------+---------------------+
| Service / Container      | Baseline Platform  | Stream Profile    | Batch Profile       |
+--------------------------+--------------------+-------------------+---------------------+
| PostgreSQL 16            | 256 MB             | 256 MB            | 256 MB              |
| Apache Kafka (KRaft)     | 512 MB             | 512 MB            | 512 MB              |
| MinIO S3 Storage         | 256 MB             | 256 MB            | 256 MB              |
| Apache Polaris Catalog   | 512 MB             | 512 MB            | 512 MB              |
| Marquez & OpenLineage    | 512 MB             | 512 MB            | 512 MB              |
| Trino SQL Query Engine   | 1,536 MB           | 1,536 MB          | 1,536 MB            |
| Dagster Web & Daemon     | 512 MB             | 512 MB            | 512 MB              |
| Prometheus & Grafana     | 384 MB             | 384 MB            | 384 MB              |
+--------------------------+--------------------+-------------------+---------------------+
| SUB-TOTAL (Shared Base)  | 4,480 MB (~4.4 GB) | 4,480 MB (~4.4 GB)| 4,480 MB (~4.4 GB)  |
+--------------------------+--------------------+-------------------+---------------------+
| Flink JobManager         | - (idle)           | 512 MB            | - (stopped)         |
| Flink TaskManager        | - (idle)           | 1,024 MB          | - (stopped)         |
| Event Producer           | - (idle)           | 128 MB            | - (stopped)         |
| Spark Batch Driver       | - (idle)           | - (stopped)       | 1,536 MB            |
| Spark Batch Executor     | - (idle)           | - (stopped)       | 1,536 MB            |
+--------------------------+--------------------+-------------------+---------------------+
| TOTAL ACTIVE CONSUMPTION | 4,480 MB (4.4 GB)  | 6,144 MB (6.0 GB) | 7,552 MB (7.4 GB)   |
| REMAINING HEADROOM       | 3,712 MB (3.6 GB)  | 2,048 MB (2.0 GB) | 640 MB (0.6 GB)     |
+--------------------------+--------------------+-------------------+---------------------+
```

### Operational Startup Profiles
- **Profile 1: `platform` (Core Infrastructure)**: Starts foundational services only: PostgreSQL, Kafka, MinIO, Polaris, Marquez, Trino, Dagster, Prometheus, and Grafana. Consumes ~4.4 GB RAM. Ideal for catalog inspection, dashboard reviews, and contract authoring.
- **Profile 2: `stream` (Real-Time Pipeline)**: Keeps core infrastructure active and boots Apache Flink JobManager/TaskManager and the Kafka event producer. Consumes ~6.0 GB RAM. Dedicated to real-time event streaming and window aggregation verification.
- **Profile 3: `batch` (Medallion ETL)**: Keeps core infrastructure active, stops Flink, and launches Spark batch jobs (`bronze_ingestion`, `silver_transform`, `gold_aggregation`). Consumes ~7.4 GB RAM. Dedicated to snapshot ingestion, cleansing, and dimensional modeling.
- **Profile 4: `demo` (Automated Sequential Runner)**: Orchestrates execution sequentially via `./scripts/demo.sh`. It seeds PostgreSQL, runs Spark batch processing to Gold, starts Kafka streaming, executes Flink windowing, runs quality gates, and executes verification queries in Trino without running heavy batch and stream engines simultaneously.
