# Software Requirements Specification (SRS)
## Modern Data Architecture Proof of Concept (POC)
### Standard: IEEE Std 830-1998 / ISO/IEC/IEEE 29148:2018 Compliant

> **NOTICE — legacy numbering.** This English SRS uses a legacy RF numbering that
> differs from the canonical requirements in [`../srs.txt`](../srs.txt) (Spanish, §5:
> authoritative RF-01…RF-25 definitions, priorities `[M]`/`[S]`, and §9 acceptance
> scenarios — the tests follow that numbering, e.g. `test_rf06_*` = quality gate,
> `test_rf07_*` = time travel, `test_rf19_*` = lineage). Roughly, the legacy RF-06…RF-21
> block in this file covers the same ground as the srs.txt RF-04…RF-17 region (Silver/Gold,
> gates, serving, mesh, contracts, lineage), but there is **no exact row-by-row equivalence**:
> do not treat the two numberings as interchangeable. For any conflict, `srs.txt` wins.
> Repository paths in this file have been normalised to English
> (`domains/sales`, `domains/finance`, `flink_orders_stream.py`, `sales.*` table names);
> the K8s deployment target lives in `infra/helm` + `infra/profiles` (there is no
> `docker-compose.yml` in this repo).

---

## Document Metadata

| Attribute | Value |
| :--- | :--- |
| **Document Title** | Software Requirements Specification for Modern Data Architecture POC |
| **Document Version** | 1.0.0-PROD |
| **Status** | Approved / Baseline |
| **System Classification** | Enterprise Data Platform & Analytics Proof of Concept |
| **Core Paradigms** | Data Mesh, Data Fabric, Data Products, Medallion Lakehouse, Hybrid Lambda/Kappa |
| **Target Runtime** | Single-Node Kubernetes (kind) via Helm profiles (8 GB RAM constraint; no Docker Compose file in this repo) |
| **License** | MIT (100% Open Source Software) |

---

## Table of Contents

1. [Section 1: Introduction](#1-introduction)
   - 1.1 Purpose
   - 1.2 Scope
   - 1.3 Definitions, Acronyms, and Abbreviations
   - 1.4 References
2. [Section 2: Overall Description](#2-overall-description)
   - 2.1 Product Perspective
   - 2.2 User Classes and Characteristics
   - 2.3 Operating Environment
   - 2.4 Design and Implementation Constraints (RC-01 to RC-07)
   - 2.5 Assumptions and Dependencies
3. [Section 3: Detailed Architecture](#3-detailed-architecture)
   - 3.1 Architecture Overview & 3-Plane Model
   - 3.2 Subsystem Breakdown (S1 to S8)
   - 3.3 End-to-End Data Flows (F1 to F6)
   - 3.4 Core Architectural Decisions (AD-1 to AD-4)
4. [Section 4: Technology Stack](#4-technology-stack)
   - 4.1 100% Open-Source Technology Stack Matrix
   - 4.2 Trade-off Analysis & Technical Justifications
5. [Section 5: Functional Requirements](#5-functional-requirements)
   - 5.1 Ingestion & Synthetic Generation (RF-01 to RF-04)
   - 5.2 Medallion Architecture & Lakehouse Storage (RF-05 to RF-09)
   - 5.3 Processing & Serving Layer (RF-10 to RF-12)
   - 5.4 Data Mesh & Data Products (RF-13 to RF-16)
   - 5.5 Data Fabric & Automated Governance (RF-17 to RF-20)
   - 5.6 Operations, Observability & Validation (RF-21 to RF-25)
6. [Section 6: Non-Functional Requirements](#6-non-functional-requirements)
   - 6.1 Performance (RNF-01)
   - 6.2 Scalability (RNF-02)
   - 6.3 Reliability & Fault Tolerance (RNF-03)
   - 6.4 Security & Compliance (RNF-04)
   - 6.5 Maintainability (RNF-05)
   - 6.6 Portability (RNF-06)
   - 6.7 Resource Footprint (RNF-07)
   - 6.8 Observability & Traceability (RNF-08)
   - 6.9 Data Freshness & Latency (RNF-09)
   - 6.10 Idempotency & Consistency (RNF-10)
   - 6.11 Usability & Developer Experience (RNF-11)
7. [Section 7: External Interfaces](#7-external-interfaces)
   - 7.1 User Interfaces
   - 7.2 Software Interfaces & Protocols
   - 7.3 Data Interfaces & Schema Specifications
8. [Section 8: Data Constraints & Sizing](#8-data-constraints--sizing)
   - 8.1 Data Volume and Generation Bounds
   - 8.2 Retention, Compaction & Purge Policies
9. [Section 9: Acceptance Criteria & Verification](#9-acceptance-criteria--verification)
   - 9.1 Verification Scenarios (Given / When / Then)
10. [Section 10: Traceability Matrix](#10-traceability-matrix)
11. [Section 11: Risks and Mitigations](#11-risks-and-mitigations)
12. [Section 12: Operator Decisions & Future Roadmap](#12-operator-decisions--future-roadmap)

---

## 1. Introduction

### 1.1 Purpose
This Software Requirements Specification (SRS) establishes the complete functional, structural, and behavioral baseline for the **Modern Data Architecture Proof of Concept (POC)**. This platform synthesizes modern enterprise data engineering paradigms—**Data Mesh**, **Data Fabric**, **Data Products**, **Medallion Lakehouse**, and **Hybrid Lambda/Kappa Architecture**—into an operational, reproducible, and verifiable reference implementation.

The document serves two primary objectives:
1. **Academic and Pedagogical Reference**: Codify how theoretical constructs from *Modern Data Engineering* translate directly into concrete, production-grade open-source implementations.
2. **Technical Architecture Portfolio**: Provide rigorous architectural validation, operational runbooks, declarative contracts, and automated verification suites demonstrating engineering excellence.

### 1.2 Scope
The POC platform models an omnichannel e-commerce enterprise comprising three autonomous business domains:
- **Sales**: Customer entities, order transactions, line-item purchases, and fulfillment states.
- **Marketing**: Marketing campaigns, multi-channel outreach, web clickstream/navigation events, and conversion funnels.
- **Finance**: Revenue recognition, gross margin aggregation, and financial reporting metrics derived from cross-domain consumption.

The system encompasses:
- Synthetic transaction and streaming event generation (OLTP PostgreSQL database and Apache Kafka broker).
- Raw data preservation, transformation, and dimensional aggregation across Bronze, Silver, and Gold Lakehouse tiers stored as Apache Iceberg tables on S3-compatible MinIO storage.
- Real-time stream processing with tumbling and sliding windows via Apache Flink.
- Unified federated serving via Trino merging historical batch datasets with streaming buffers.
- Decentralized Data Product manifests and declarative Data Contracts with programmatic backward-compatibility validation.
- Automated Data Fabric governance utilizing OpenLineage for active metadata harvesting, Marquez for visual lineage graphs, and fail-closed quality gates evaluating Great-Expectations-format suites with the built-in evaluation engine.
- Resource-optimized deployment supporting an aggressive **8 GB total system RAM footprint** through orchestrated execution profiles.

### 1.3 Definitions, Acronyms, and Abbreviations

| Acronym / Term | Definition |
| :--- | :--- |
| **ACID** | Atomicity, Consistency, Isolation, Durability |
| **ADR** | Architecture Decision Record |
| **CDC** | Change Data Capture |
| **Data Contract** | Formal agreement between data producer and consumer specifying schema, SLAs, quality rules, and governance terms. |
| **Data Fabric** | Architecture that integrates disparate data sources through active metadata harvesting, automated lineage, and dynamic quality enforcement. |
| **Data Mesh** | Decentralized socio-technical architectural paradigm treating data as a product owned by domain-aligned teams. |
| **Data Product** | Autonomous, discoverable, understandable, secure, and interoperable data delivery node comprising data assets, metadata, contracts, and output ports. |
| **DLQ** | Dead Letter Queue |
| **GDPR** | General Data Protection Regulation (EU 2016/679) |
| **Iceberg** | Apache Iceberg high-performance open table format for massive analytic datasets. |
| **Kappa Architecture** | Data processing pattern where all processing (real-time and historical replay) flows through a unified streaming log engine. |
| **Lambda Architecture** | Data processing pattern utilizing separate batch (historical accuracy) and speed (low latency) layers unified at query time. |
| **Medallion Architecture** | Data curation pattern structured into Bronze (raw/audit), Silver (cleaned/conformed), and Gold (aggregated/modeled) tiers. |
| **OLTP** | Online Transaction Processing |
| **PII** | Personally Identifiable Information |
| **Polaris** | Apache Polaris (incubating), an open-source REST catalog for Apache Iceberg. |
| **SLA** | Service Level Agreement |
| **Trino** | Fast distributed SQL query engine for big data analytics. |

### 1.4 References
1. IEEE Std 830-1998: *IEEE Recommended Practice for Software Requirements Specifications*.
2. ISO/IEC/IEEE 29148:2018: *Systems and software engineering — Life cycle processes — Requirements engineering*.
3. Dehghani, Zhamak. *Data Mesh: Delivering Data-Driven Value at Scale*. O'Reilly Media, 2022.
4. Apache Iceberg Table Specification v2: https://iceberg.apache.org/spec/
5. OpenLineage Specification v1: https://openlineage.io/spec/1-0-5/
6. Linux Foundation / Bitol Open Data Contract Standard (ODCS): https://datacontract.com/

---

## 2. Overall Description

### 2.1 Product Perspective
The Modern Data Architecture POC operates as a self-contained, microservices-based data ecosystem. It operates across three distinct architectural planes:
1. **Organization Plane**: Decentralized domain ownership, Data Product manifests, and versioned Data Contracts.
2. **Processing Plane**: Dual-engine compute infrastructure (Apache Spark for distributed batch ETL; Apache Flink for real-time stream processing; Trino for interactive federated serving).
3. **Operations Plane**: Active metadata fabric (OpenLineage, Marquez), automated data quality validation (built-in evaluator over GE-format suites), workflow orchestration (Dagster), and infrastructure telemetry (Prometheus, Grafana).

```
+-----------------------------------------------------------------------------------+
|                              ORGANIZATION PLANE                                   |
|   Domain: Sales                Domain: Marketing           Domain: Finance         |
|   - Data Products:             - Data Products:            - Derived Products:    |
|     * customers                  * campaigns                 * cross_domain_360   |
|     * orders                     * leads / funnel            * revenue_unified    |
|   - Declarative Contracts      - Declarative Contracts     - Federated Policies   |
+-----------------------------------------------------------------------------------+
|                              PROCESSING PLANE                                     |
|   Sources (OLTP / Kafka) -> Ingestion -> Bronze -> Silver -> Gold (Iceberg/MinIO) |
|   Streaming Clickstream  -> Flink Job  -> Stream Iceberg Buffers (T+5m latency)   |
|   Trino Serving Layer    -> Unified Batch + Stream Queries (Zero Duplication)    |
+-----------------------------------------------------------------------------------+
|                              OPERATIONS PLANE                                     |
|   Active Fabric: OpenLineage Event Bus -> Marquez Lineage Graph                   |
|   Quality Gates: GE-format Suite Evaluation (built-in fail-closed engine)         |
|   Orchestration: Dagster Asset Graph & Execution Profiles                         |
|   Telemetry: Prometheus Exporters + Grafana Dashboards                            |
+-----------------------------------------------------------------------------------+
```

### 2.2 User Classes and Characteristics
- **Data Engineer**: Authors batch ETL and streaming pipelines, configures Iceberg partition strategies, tunes compute engines, and maintains CI/CD verification workflows.
- **Data Product Owner / Domain Lead**: Defines product boundaries, specifies schemas and business semantic definitions, authors Data Contracts (`contract.yaml`), and sets SLA commitments.
- **Data Steward / Governance Officer**: Reviews privacy controls, enforces PII masking policies, audits pipeline lineage in Marquez, and inspects quality scorecards.
- **Business / Analytics Consumer**: Executes ad-hoc analytical queries in Trino, connects BI dashboards, and consumes certified Gold data products without requiring knowledge of storage physical layouts.

### 2.3 Operating Environment
- **Host Hardware**: Standard developer workstation or laptop (x86_64 or ARM64) with at least 4 physical CPU cores and 16 GB physical RAM (with **<= 8 GB RAM allocated to Docker/Kubernetes**).
- **Virtualization / Container Platform**: Docker Engine 24.x+ or Docker Desktop with kind (Kubernetes in Docker) v0.20+, or standalone Docker Compose v2.20+.
- **Base Container Images**: Alpine Linux, Debian-slim, and official OpenJDK 17 headless base images.
- **Client Tooling**: Python 3.10+, PowerShell 7+, Bash, `curl`, and standard Trino CLI.

### 2.4 Design and Implementation Constraints (RC-01 to RC-07)

- **RC-01 (100% Open Source Software)**: Every single framework, catalog, database, orchestrator, and dashboard in the architecture must be licensed under an OSI-approved open-source license (Apache 2.0, MIT, BSD, or MPL). Commercial SaaS or proprietary cloud offerings (e.g., Snowflake, BigQuery, Databricks Unity Catalog, Confluent Cloud) are strictly prohibited.
- **RC-02 (8 GB RAM Memory Budget)**: The total memory consumption across all concurrently active containers in any operational mode must not exceed 8,192 MB (8 GB). Memory allocations per container must be constrained via cgroups/Docker resource limits.
- **RC-03 (Single-Node Kubernetes & Containerized Portability)**: The entire topology must be deployable on a single-node local Kubernetes cluster (`kind` or `minikube`) or modular Docker Compose without external network dependencies.
- **RC-04 (Strict Schema Evolution & Compatibility)**: Data Contracts must enforce strict backward compatibility (e.g., fields cannot be deleted or renamed; types cannot be narrowed). Contract breaking changes must be detected and blocked automatically at pre-commit or CI time.
- **RC-05 (GDPR & Privacy Compliance)**: Personally Identifiable Information (PII) such as customer names, email addresses, phone numbers, and physical addresses must never be exposed in unmasked plaintext within Gold tables or Serving views. Pseudonymization and cryptographic hashing (SHA-256) must be applied in Silver-to-Gold transformations.
- **RC-06 (Deterministic Local Reproducibility)**: The platform must support repeatable synthetic data seeding and pipeline re-execution from scratch with zero manual state manipulation.
- **RC-07 (End-to-End Operational Observability)**: Every pipeline execution must publish metadata via OpenLineage to Marquez, record data quality validation metrics, and expose health probes.

### 2.5 Assumptions and Dependencies
- The local host machine possesses sufficient disk storage (minimum 25 GB free disk space) to accommodate container images and MinIO persistent volumes.
- Network port collisions are avoided by adhering to the predefined platform port assignment schema (e.g., MinIO API `9000`, MinIO Console `9001`, Kafka `9092`, Trino `8080`, Marquez `5000`, Dagster `3000`).
- When operating under the strict 8 GB RAM limit, background jobs that are not needed simultaneously (e.g., batch processing vs. stream processing) are scheduled sequentially using operational startup profiles.

---

## 3. Detailed Architecture

### 3.1 Architecture Overview & 3-Plane Model
The system architecture separates enterprise data concerns across three orthogonal planes:

```
+===================================================================================+
| 1. ORGANIZATION PLANE (Decentralized Governance & Ownership)                      |
|                                                                                   |
|  +-----------------------+  +-----------------------+  +-----------------------+  |
|  | Domain: Sales           |  | Domain: Marketing     |  | Domain: Finance       |  |
|  | * Product: customers  |  | * Product: campaigns  |  | * Product: rev_daily  |  |
|  | * Product: orders     |  | * Product: leads      |  | * Product: c360_gold  |  |
|  | * Contract: v1.2.0    |  | * Contract: v1.0.0    |  | * Contract: v1.0.0    |  |
|  +-----------------------+  +-----------------------+  +-----------------------+  |
|               \                         |                         /               |
|                +------------------------+------------------------+                |
|                                         |                                         |
|  +-----------------------------------------------------------------------------+  |
|  | Federated Governance: Declarative Contracts (Bitol ODCS) + Schema Registry   |  |
|  +-----------------------------------------------------------------------------+  |
+===================================================================================+
| 2. PROCESSING PLANE (Dual Compute Engines & Medallion Storage)                    |
|                                                                                   |
|  Sources:                                                                         |
|    [PostgreSQL OLTP] --(JDBC Snapshot)--> [Apache Spark Engine]                   |
|    [Apache Kafka Bus] --(Real-Time)-----> [Apache Flink Engine]                   |
|                                                                                   |
|  Medallion Lakehouse Tiers (Apache Iceberg tables on MinIO S3 Object Store):      |
|    +--------------------+    +---------------------+    +----------------------+  |
|    | BRONZE LAYER       | -> | SILVER LAYER        | -> | GOLD LAYER           |  |
|    | - Raw snapshot     |    | - Cleaned & typed   |    | - Aggregated KPIs    |  |
|    | - Audit metadata   |    | - Deduplicated      |    | - PII masked (GDPR)  |  |
|    | - Append-only      |    | - Conformed dims    |    | - Star/Snowflake     |  |
|    +--------------------+    +---------------------+    +----------------------+  |
|                                                                                   |
|  Serving Layer:                                                                   |
|    +---------------------------------------------------------------------------+  |
|    | Trino SQL Query Engine (Federated Catalogs via Apache Polaris REST)       |  |
|    | - View: serving.sales.unified_revenue (Batch Gold UNION ALL Stream 5m)   |  |
|    | - View: serving.cross_domain.customer_360                                 |  |
|    +---------------------------------------------------------------------------+  |
+===================================================================================+
| 3. OPERATIONS PLANE (Active Fabric, Orchestration & Observability)                |
|                                                                                   |
|  Active Metadata Fabric:                                                          |
|    [Spark/Flink Run] --(OpenLineage Events)--> [Marquez Lineage & Catalog Service]|
|                                                                                   |
|  Quality Gates:                                                                   |
|    [GE-format Suite] --(Built-in Engine: Validation & Threshold Check)----------->|
|    (If Pass -> Commit Iceberg Snapshot; If Fail -> Alert & Block Pipeline)        |
|                                                                                   |
|  Orchestration & Telemetry:                                                       |
|    [Dagster Asset Graph] -> Schedules, Triggers, Run Profiles                      |
|    [Prometheus & Grafana] -> Latency, Memory, Query Throughput, Contract Metrics  |
+===================================================================================+
```

### 3.2 Subsystem Breakdown (S1 to S8)

#### S1: Source & Ingestion Subsystem
- **Purpose**: Generates and ingests synthetic operational transactions and web clickstream data representing a realistic Spanish e-commerce environment.
- **Components**:
  - `oltp_seed.py`: Connects via `psycopg2` to PostgreSQL, creating idempotent schemas and seeding 1,000 customers, 50,000 orders, and 200,000 order line items.
  - `event_producer.py`: Multithreaded Kafka producer with token-bucket rate limiting (10 to 100 events/sec) producing realistic checkout and navigation funnels to topics `sales.orders.events` and `marketing.clickstream.raw`.
  - Ingestion Connectors: PySpark JDBC connector for batch snapshotting; Flink Kafka Source for low-latency streaming ingest.

#### S2: Lakehouse Storage & Catalog Subsystem
- **Purpose**: Provides reliable, ACID-compliant, open-standard columnar storage with snapshot isolation and schema evolution.
- **Components**:
  - **Storage Engine**: MinIO S3-compatible object storage configured with bucket `lakehouse` partitioned into `bronze/`, `silver/`, and `gold/`.
  - **Table Format**: Apache Iceberg v2 table format specifying partition evolution, hidden partitioning, file-level min/max statistics, and time-travel capability.
  - **Metadata Catalog**: Apache Polaris (incubating) REST catalog offering centralized multi-engine namespace discovery and table metadata management.

#### S3: Batch Processing Engine Subsystem
- **Purpose**: Executes high-throughput transformations, deduplication, cleansing, and multi-dimensional aggregations across Medallion layers.
- **Components**:
  - `bronze_ingestion.py`: Reads PostgreSQL tables via JDBC, attaches system lineage attributes (`_ingested_at`, `_source`, `_batch_id`), and appends into Iceberg Bronze tables.
  - `silver_transform.py`: Reads Bronze tables, cleanses whitespace, casts strict types, enforces business validation rules, eliminates duplicate records by primary key, and commits into Iceberg Silver tables.
  - `gold_aggregation.py`: Computes business KPIs (daily sales by status, customer segment distribution, campaign ROAS), applies SHA-256 PII masking, and publishes to Iceberg Gold tables.

#### S4: Stream Processing Engine Subsystem
- **Purpose**: Ingests, enriches, and aggregates high-frequency event streams with sub-minute latency.
- **Components**:
  - `flink_orders_stream.py`: PyFlink streaming pipeline reading `sales.orders.events`, tumbling over 5-minute event-time windows with 30-second watermarks, calculating total volume and GMV, and writing to Iceberg stream buffers.
  - `flink_clickstream_stream.py`: Clickstream sessionizer tracking visitor drop-off and funnel stages, streaming real-time metrics to `stream.marketing.clickstream_windowed`.
  - Replay Mechanism: Configurable stream offsets supporting complete reprocessing (Kappa paradigm).

#### S5: Serving Layer Subsystem
- **Purpose**: Presents a unified, zero-copy SQL access interface across diverse storage tiers and catalogs.
- **Components**:
  - **Engine**: Trino distributed SQL query engine with Iceberg connector hooked to Apache Polaris REST catalog.
  - `unified_view.sql`: Exposes views (e.g., `serving.sales.unified_revenue`) that execute a `UNION ALL` between historical Gold batch tables and active streaming Iceberg buffers, using date filtering to prevent double counting.
  - Cross-Domain Analytics: Supports federated joins combining `sales` transactions, `marketing` spend, and `finance` allocations.

#### S6: Data Mesh & Data Products Layer Subsystem
- **Purpose**: Enforces decentralized domain boundary ownership, explicit data asset interfaces, and contractual accountability.
- **Components**:
  - **Domain Repositories**: Independent workspaces for `domains/sales`, `domains/marketing`, and `domains/finance`.
  - **Manifests (`product.yaml`)**: Machine-readable descriptors specifying product ownership, metadata, SLA guarantees, data classifications, and output ports.
  - **Data Contracts (`contract.yaml`)**: Bitol Open Data Contract Standard (ODCS) declarative contracts defining schemas, validation rules, compatibility policies, and consumer registrations.

#### S7: Data Fabric & Automated Governance Subsystem
- **Purpose**: Provides automated metadata harvesting, cross-pipeline lineage visualization, and declarative data quality enforcement.
- **Components**:
  - **Active Lineage**: OpenLineage Spark listener and Flink OpenLineage reporter publishing dataset dependencies and run states to Marquez on port `5000`.
  - **Quality Gates (`run_quality_gate.py`)**: Evaluates Great-Expectations-format suites (`silver_orders_suite.json`, `silver_customers_suite.json`) with the built-in fail-closed engine before committing data to subsequent tiers. Halts pipeline progression upon quality breaches.
  - **Policy Enforcement**: Declarative policy rules (Kyverno / OPA) verifying container security contexts and schema contract compliance.

#### S8: Orchestration & Observability Subsystem
- **Purpose**: Coordinates inter-job execution dependencies, manages resource-constrained startup profiles, and visualizes platform telemetry.
- **Components**:
  - **Orchestration**: Dagster asset-based orchestrator modeling pipeline steps as a Software-Defined Asset (SDA) DAG.
  - **Telemetry**: Prometheus server scraping container metrics and engine JMX metrics; pre-configured Grafana dashboards displaying pipeline throughput, latency, quality gate pass rates, and system memory.

---

### 3.3 End-to-End Data Flows (F1 to F6)

```
 [PostgreSQL OLTP] ---(F1: Batch JDBC)---> [Spark Bronze] ---> [Iceberg Bronze]
                                                                     |
                                                               (F3: Spark Silver)
                                                                     v
 [Kafka Clickstream] --(F2: Flink Stream)-> [Stream Buffer]    [Iceberg Silver]
           |                                       |                 |
     (F6: Replay)                                  |           (F3: Spark Gold)
           |                                       v                 v
           +---------------------------------> [Trino Unified Serving (F4)]
                                                       ^
                                                       |
 [Pipelines S3/S4] ---(F5: OpenLineage)-----> [Marquez Lineage & Quality Gates]
```

- **Flow F1 (OLTP to Bronze Ingestion)**: Scheduled or manual trigger initiates `bronze_ingestion.py`. Spark connects to PostgreSQL via JDBC, captures an immutable snapshot of `orders`, `customers`, and `campaigns`, appends ingestion timestamps (`_ingested_at`), and appends to Iceberg Bronze tables on MinIO. Emits OpenLineage START/COMPLETE events to Marquez.
- **Flow F2 (Streaming Ingestion & Aggregation)**: `event_producer.py` publishes JSON messages to Kafka. `flink_orders_stream.py` consumes records using event-time timestamps, extracts windowed totals (5-minute tumbling windows), and sinks results to Iceberg `stream.sales.orders_windowed`.
- **Flow F3 (Medallion Progression)**: `silver_transform.py` runs (thin driver over the core use case), reading Bronze, filtering corrupt rows, deduplicating by primary key, and writing to Silver. `run_quality_gate.py` evaluates the GE-format suite with the built-in fail-closed engine. Upon passage (score >= 0.95), `gold_aggregation.py` reads Silver, computes dimensional KPIs, hashes PII columns using SHA-256, and writes to Gold tables (a **full recomputation from Silver on every run**).
- **Flow F4 (Unified Serving Query)**: A business analyst issues `SELECT * FROM serving.sales.unified_revenue` to Trino. Trino evaluates the view definition: historical data prior to `CURRENT_DATE - INTERVAL '2' DAY` is read from `gold.sales.daily_metrics`, while recent data is read from `stream.sales.orders_windowed`. The query returns unified results without manual reconciliation.
- **Flow F5 (Lineage & Quality Enforcement)**: During every pipeline step, dataset names, input/output schemas, row counts, and execution run UUIDs are transmitted over HTTP to Marquez. If `run_quality_gate.py` detects a critical check breach (e.g., null primary keys), it exits with code `1`, halting the DAG, emitting an OpenLineage FAILED event, and firing a notification webhook.
- **Flow F6 (Kappa Reprocessing / Historical Replay)**: Upon algorithm revision or backfill request, the operator triggers a **full stream log replay** (`flink_orders_stream.py --start-offset earliest` / `--replay`, or `rake stream:replay`) or resets consumer offsets to `earliest`. The batch side needs no flag: every `gold_aggregation.py` run is already a **full recomputation** of the historical aggregates from Silver. Both paths re-evaluate historical data idempotently without mutating raw Bronze history.

---

### 3.4 Core Architectural Decisions (AD-1 to AD-4)

- **AD-1 (Hybrid Lambda + Kappa Architecture)**: Combine the reliability and deep relational transformation power of Spark batch processing for master historical data (Lambda batch tier) with Flink low-latency windowing for real-time telemetry (Kappa streaming tier), joined seamlessly at the Trino serving layer.
- **AD-2 (Apache Iceberg + Apache Polaris as Lakehouse Foundation)**: Select Apache Iceberg over Delta Lake or Apache Hudi for native multi-engine interoperability (Spark, Flink, and Trino share identical metadata) and Apache Polaris as the vendor-neutral REST catalog.
- **AD-3 (Multi-Domain Data Mesh with Declarative Contracts)**: Organize data architecture around autonomous business domains (`sales`, `marketing`, `finance`) rather than centralized technical layers. Enforce domain boundaries via Bitol-compliant YAML data contracts (`contract.yaml`).
- **AD-4 (Resource-Constrained Execution Profiling for 8 GB RAM)**: Mitigate memory contention on developer hardware by establishing 4 distinct operational startup profiles (`Platform`, `Stream`, `Batch`, `Demo`), executing heavy compute workloads sequentially rather than simultaneously.

---

## 4. Technology Stack

### 4.1 100% Open-Source Technology Stack Matrix

| Subsystem | Component | Open Source Project | Version | License | Host / Container Port |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **S1** | OLTP Source Database | PostgreSQL | 16-alpine | PostgreSQL (MIT-style) | `5432` |
| **S1** | Event Streaming Broker | Apache Kafka | 3.7 (KRaft mode) | Apache 2.0 | `9092` |
| **S1** | Data Generators | Python (Faker, Click) | 3.11-slim | Python Software Foundation | Internal CLI |
| **S2** | Object Storage | MinIO | RELEASE.2024+ | GNU AGPLv3 (Community) | `9000` (API), `9001` (UI) |
| **S2** | Table Format | Apache Iceberg | 1.5.0 | Apache 2.0 | Embedded Library |
| **S2** | Lakehouse Catalog | Apache Polaris (incubating) | 0.1.0+ | Apache 2.0 | `8181` |
| **S3** | Batch Compute Engine | Apache Spark (PySpark) | 3.5.1 | Apache 2.0 | `4040` (UI) |
| **S4** | Stream Compute Engine | Apache Flink (PyFlink) | 1.18.1 | Apache 2.0 | `8081` (UI) |
| **S5** | Distributed Query Engine | Trino | 442+ | Apache 2.0 | `8080` (UI/API) |
| **S6** | Data Contract Spec | Bitol / Open Data Contract | ODCS v2 | Apache 2.0 / CC-BY-4.0 | File-based YAML |
| **S7** | Active Metadata & Lineage| OpenLineage & Marquez | OL 1.9 / Mqz 0.42 | Apache 2.0 | `5000` (API), `3001` (UI) |
| **S7** | Quality Assurance Gate | Built-in fail-closed evaluator over GE-format suites (Great Expectations *library* not required) | n/a (in-repo) | MIT (this repo) | Embedded CLI |
| **S7** | Kubernetes Policy Engine | Kyverno | 1.11+ | Apache 2.0 | K8s Admission Webhook |
| **S8** | Workflow Orchestration | Dagster | 1.6+ | Apache 2.0 | `3000` (Web UI) |
| **S8** | Platform Telemetry | Prometheus & Grafana | Prom 2.50 / Graf 10.3| Apache 2.0 / AGPLv3 | `9090` (Prom), `3000` (Graf) |

### 4.2 Trade-off Analysis & Technical Justifications

| Selected Component | Alternative Evaluated | Why Selected | Why Alternative Rejected |
| :--- | :--- | :--- | :--- |
| **Apache Iceberg** | Delta Lake, Apache Hudi | Industry-standard open specification, superior multi-engine support across Spark, Flink, and Trino, clean REST catalog specification. | Delta Lake's full multi-engine feature set is tightly tied to the Databricks ecosystem; Hudi has complex tuning parameters and heavier memory footprint. |
| **Apache Polaris** | Project Nessie, Hive Metastore | Vendor-neutral, open-source REST catalog standard supported natively by Snowflake, Trino, and Spark with fine-grained access control. | Hive Metastore requires heavy legacy Hadoop dependencies; Nessie focuses primarily on Git-like branching rather than open REST standard. |
| **Apache Flink** | Spark Streaming | True native event-driven stream processing with sub-second latency, superior stateful tumbling/sliding window primitives, and robust out-of-order event handling. | Spark Structured Streaming relies on micro-batching which introduces artificial latency (1-2s minimum) and higher memory overhead under constrained resources. |
| **Trino** | Apache Drill, PrestoDB | Exceptional query execution speed, native high-performance Iceberg connector, widespread enterprise adoption, and rich SQL syntax. | Apache Drill lacks modern Iceberg REST catalog support; PrestoDB has slower development cadence compared to Trino community. |
| **Marquez** | Apache Atlas, DataHub | Lightweight reference implementation of OpenLineage; runs efficiently under 512 MB RAM; immediate visual DAG representation of dataset lineage. | Apache Atlas and DataHub require multiple backing distributed datastores (Elasticsearch, Kafka, Neo4j) which violate the 8 GB RAM constraint (RC-02). |
| **Dagster** | Apache Airflow | Native software-defined asset (SDA) paradigm matches Data Products concept; lightweight Python daemon; superior local testability. | Apache Airflow is task-oriented rather than asset-oriented, requiring heavier background scheduler processes and worker pools. |

---

## 5. Functional Requirements

### 5.1 Ingestion & Synthetic Generation

- **RF-01 [M] Synthetic OLTP Data Generation**: The system shall generate a synthetic relational e-commerce dataset via `oltp_seed.py` comprising `customers` (1,000 rows with realistic Spanish demographic attributes and PII), `orders` (50,000 order records distributed across the last 90 days), `order_items` (200,000 item details), and `campaigns` (100 marketing records). Generation must be idempotent using `INSERT ON CONFLICT DO NOTHING`.
- **RF-02 [M] Real-Time Event Generation**: The system shall continuously stream synthetic JSON events to Apache Kafka topics (`sales.orders.events` and `marketing.clickstream.raw`) via `event_producer.py`. The producer must feature token-bucket rate limiting (10-100 events/sec), realistic conversion funnel probabilities, and session correlation IDs.
- **RF-03 [M] Batch Ingestion to Bronze Lakehouse**: The system shall read relational tables from PostgreSQL via Spark JDBC, append audit metadata columns (`_ingested_at`, `_source`, `_batch_id`), and persist the records into raw Iceberg tables within `bronze.*` on MinIO without schema alteration.
- **RF-04 [M] Real-Time Stream Ingestion**: The system shall ingest records from Kafka topics via Apache Flink, parse JSON payloads against predefined schemas, handle deserialization failures via an internal dead-letter queue, and commit windowed records into Iceberg streaming tables.

### 5.2 Medallion Architecture & Lakehouse Storage

- **RF-05 [M] Bronze Raw Preservation**: The system shall maintain an immutable, append-only history of raw ingested data in Bronze Iceberg tables. No deduplication, business filtering, or data mutation shall be performed in this tier.
- **RF-06 [M] Silver Data Cleansing & Deduplication**: The system shall process Bronze records into Silver tables (`silver.sales.orders`, `silver.sales.customers`) by performing whitespace trimming, casting to contractual datatypes, eliminating exact and primary-key duplicates via window deduplication, and validating foreign key relationships.
- **RF-07 [M] Gold Aggregation & Dimensional Modeling**: The system shall aggregate Silver data into high-value business KPI tables (`gold.sales.daily_metrics`, `gold.sales.customers`). Aggregations shall include daily GMV, order volume, cancellation rates, and customer lifetime value. *(Implementation note: sales Gold is delivered and verified; marketing/finance Gold tables are PLANNED, not produced in this POC — see `docs/traceability-status.md`.)*
- **RF-08 [M] ACID Operations & Time Travel**: The system shall support snapshot isolation, atomic commits, schema evolution, and time-travel queries across all Iceberg tables, verifiable via Spark and Trino metadata tables (e.g., `silver.sales.orders$snapshots`).
- **RF-09 [M] Centralized REST Catalog Registration**: All Iceberg namespaces and tables must be registered in Apache Polaris REST catalog, ensuring a single source of metadata truth across Spark, Flink, and Trino.

### 5.3 Processing & Serving Layer

- **RF-10 [S] Kappa-Style Historical Reprocessing**: The stream processing and batch aggregation pipelines shall provide an idempotent replay capability (stream: `--start-offset earliest` / `--replay` CLI flags or broker offset resets; batch: full recomputation of derived tables on every run) allowing complete recomputation of derived tables without mutating raw Bronze history.
- **RF-11 [M] Unified Batch + Stream Serving**: The system shall provide unified analytical views in Trino (`serving.sales.unified_revenue`, `serving.marketing.unified_campaign_performance`) that seamlessly execute a `UNION ALL` between historical Gold batch tables and active streaming buffers, applying partition filters to ensure zero data duplication.
- **RF-12 [S] Interactive Cross-Domain SQL Querying**: Trino shall execute ad-hoc analytical queries joining assets across disparate domains (e.g., joining `sales.orders` with `marketing.campaigns` and `finance` budgets) with sub-3-second P99 query latency.

### 5.4 Data Mesh & Data Products

- **RF-13 [M] Domain Autonomy & Decentralization**: The system codebase, schemas, and pipelines shall be segregated into distinct domain directories (`domains/sales`, `domains/marketing`, `domains/finance`) reflecting domain boundaries and autonomous lifecycle management.
- **RF-14 [M] Formal Data Product Manifests**: Each data product shall contain a standardized `product.yaml` descriptor documenting product ownership, metadata, business SLA commitments, input ports, and output ports.
- **RF-15 [M] Declarative Data Contracts**: Every product shall maintain an authoritative `contract.yaml` adhering to the Bitol Open Data Contract Standard, defining formal field types, validation constraints, privacy classifications, and registered consumers.
- **RF-16 [M] Contract Backward Compatibility Verification**: The system shall validate that schema changes maintain strict backward compatibility (preventing field removal or datatype narrowing). Pre-commit or CI checks must reject breaking changes.

### 5.5 Data Fabric & Automated Governance

- **RF-17 [M] Automated Metadata & Lineage Harvesting**: Every pipeline job (Spark, Flink, Trino scripts) must emit OpenLineage-compliant lifecycle events to the Marquez API, tracking dataset inputs, outputs, schemas, and execution run states.
- **RF-18 [M] Automated Quality Gates**: Data pipeline execution must evaluate GE-format validation suites (`run_quality_gate.py`, built-in fail-closed engine — the Great Expectations library is not invoked) against Silver and Gold datasets. If the validation success ratio falls below the contractual threshold (e.g., 95%), the script must exit with a non-zero code, blocking downstream table commits.
- **RF-19 [M] Automated PII Masking (GDPR Compliance)**: Customer PII fields (email, full name, registration date) must be pseudonymized or masked in Silver-to-Gold transformations. Emails must be hashed with SHA-256; names must be truncated to initials; registration dates must be truncated to year-only.
- **RF-20 [S] Policy-as-Code Governance**: Kubernetes manifests and container deployments must comply with declarative security policies (Kyverno / OPA) preventing privilege escalation, enforcing read-only root filesystems where applicable, and disallowing root user execution.

### 5.6 Operations, Observability & Validation

- **RF-21 [M] Asset-Based Workflow Orchestration**: Pipeline workflows shall be defined in Dagster as a directed acyclic graph of Software-Defined Assets (SDAs), tracking asset materialization status and upstream/downstream dependencies.
- **RF-22 [M] Platform Telemetry & Dashboards**: Prometheus shall collect container, JVM, and pipeline metrics. Grafana dashboards shall display pipeline execution latency, data quality pass rates, active memory consumption, and record throughput.
- **RF-23 [M] Startup Execution Profiles**: The platform shall provide modular execution profiles (`Platform`, `Stream`, `Batch`, `Demo`) in `infra/profiles/*.yaml` and the `infra/helm` charts to prevent exceeding the 8 GB RAM memory budget constraint (RC-02). (There is no `docker-compose.yml` in this repo; Helm profiles are the deployment target.)
- **RF-24 [M] Automated End-to-End Demo Script**: The system shall provide an executable shell script (`scripts/demo.sh`) that orchestrates synthetic data generation, batch ingestion, streaming pipelines, quality gate execution, Trino verification queries, and lineage assertion with colored status output.
- **RF-25 [M] Production-Grade Technical Documentation**: The repository shall contain complete architectural specifications, Architecture Decision Records (ADRs), concept-to-component mappings, and execution guides written in professional technical English.

---

## 6. Non-Functional Requirements

### 6.1 Performance (RNF-01)
- Gold aggregation queries executed in Trino must return within **< 3.0 seconds** at the 99th percentile (P99) over a dataset of 50,000 orders and 200,000 line items.
- Batch ingestion of 50,000 relational records from PostgreSQL to Iceberg Bronze must complete within **< 60 seconds**.
- The event producer must reliably maintain an output rate of **10 to 100 events/second** without dropped packets or socket starvation.

### 6.2 Scalability (RNF-02)
- Compute and storage must be completely decoupled: compute engines (Spark, Flink, Trino) must be stateless and horizontally scalable, while all persistent state resides in MinIO object storage.
- Adding a new data domain (e.g., `logistica`) must require only adding a new domain folder with its corresponding manifests and contracts, without refactoring shared core infrastructure.

### 6.3 Reliability & Fault Tolerance (RNF-03)
- Flink streaming jobs must enable RocksDB state backend checkpointing at 30-second intervals to allow seamless recovery from container restarts without data loss.
- Bronze ingestion must be append-only with content-hash idempotency (retrying a failed execution never duplicates records and never rewrites Bronze history); derived layers (Silver/Gold) may use idempotent supersede-mode rewrites (`overwrite` mode, prior rows marked not deleted) so retries are equally duplicate-free.

### 6.4 Security & Compliance (RNF-04)
- **GDPR Article 25 & 32 Compliance**: Customer PII must be protected via pseudonymization and one-way SHA-256 cryptographic hashing prior to storage in Gold analytical tables.
- Network communication between internal services must adhere to network boundary controls. Containers must execute under unprivileged user accounts (non-root UID).

### 6.5 Maintainability (RNF-05)
- All configuration parameters (endpoints, credentials, catalog URIs, memory allocations) must be externalized into environment variables and `.env` files.
- Codebases must adhere to strict type hinting (Python `mypy` compliance), PEP 8 formatting, and comprehensive docstring annotations.

### 6.6 Portability (RNF-06)
- The entire platform must operate identically across Linux (Ubuntu 22.04+), macOS (Apple Silicon / Intel), and Windows 11 (via WSL2 or native PowerShell/Docker Desktop).
- Zero proprietary cloud vendor APIs (AWS SDK, GCP Client Libraries, Azure SDK) may be hardcoded; standard S3A and REST protocols must be used exclusively.

### 6.7 Resource Footprint (RNF-07)
- The aggregate memory consumption of all running containers in any active profile must strictly remain **<= 8,192 MB (8 GB)**.
- Container definitions must enforce hard memory limits (`mem_limit` in Docker Compose; `resources.limits.memory` in Kubernetes).

### 6.8 Observability & Traceability (RNF-08)
- 100% of pipeline runs must generate OpenLineage event payloads transmitted to Marquez, establishing full dataset lineage from raw source to final serving views.
- Quality gate validation reports must be published as JSON artifacts and logged to standard output with structured severity levels.

### 6.9 Data Freshness & Latency (RNF-09)
- Historical Gold batch tables must guarantee data freshness within **<= 1 hour** of scheduled pipeline execution.
- Real-time streaming views in Trino must reflect incoming Kafka transactions with an end-to-end latency of **<= 5 minutes**.

### 6.10 Idempotency & Consistency (RNF-10)
- Multiple consecutive runs of batch ingestion and transformation pipelines over the same input snapshot must yield bit-for-bit identical Iceberg table data.
- Read operations on Iceberg tables must provide ACID snapshot isolation, ensuring queries never observe partial or uncommitted writes.

### 6.11 Usability & Developer Experience (RNF-11)
- Developers must be able to boot the entire platform, run the end-to-end data pipeline, and verify results via a single terminal command: `./scripts/demo.sh`.
- All documentation, schemas, and code comments must be written in clear, unambiguous, professional English.

---

## 7. External Interfaces

### 7.1 User Interfaces
- **Dagster UI (`http://localhost:3000`)**: Interactive visual DAG asset catalog, real-time job execution monitoring, and run timeline inspection.
- **Marquez UI (`http://localhost:3001` or `:5000`)**: Interactive directed graph visualizing end-to-end dataset lineage, job run durations, and input/output schema evolution.
- **MinIO Console (`http://localhost:9001`)**: Web-based object storage browser for inspecting S3 bucket hierarchies, Iceberg metadata files (`.metadata.json`), and Parquet data files.
- **Grafana Dashboard (`http://localhost:3002` or `:3000`)**: Operational dashboards visualizing container CPU/memory telemetry, Flink processing lag, and quality gate scorecards.
- **Trino CLI / Web UI (`http://localhost:8080`)**: SQL console for executing interactive federated queries and inspecting distributed query execution plans.

### 7.2 Software Interfaces & Protocols
- **Apache Polaris REST Catalog API**: HTTP/REST interface over port `8181` complying with the Iceberg REST OpenAPI specification.
- **MinIO S3 API**: S3A-compatible REST protocol over port `9000` supporting multipart uploads and path-style addressing.
- **OpenLineage HTTP API**: POST endpoints on Marquez (`http://marquez:5000/api/v1/lineage`) receiving JSON event schemas.
- **Kafka Protocol**: TCP binary protocol over port `9092` for high-throughput message publishing and consumption.

### 7.3 Data Interfaces & Schema Specifications

#### Entity: `cliente` (PostgreSQL Source & Bronze/Silver Lakehouse)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Cliente",
  "type": "object",
  "required": ["id", "nombre", "email", "segmento", "fecha_alta"],
  "properties": {
    "id": { "type": "integer", "minimum": 1, "description": "Primary key identifier" },
    "nombre": { "type": "string", "maxLength": 100, "description": "Full customer name (PII)" },
    "email": { "type": "string", "format": "email", "description": "Customer email address (PII)" },
    "telefono": { "type": "string", "description": "Contact phone number (PII)" },
    "direccion": { "type": "string", "description": "Physical shipping address (PII)" },
    "segmento": { "type": "string", "enum": ["premium", "standard", "basico"] },
    "fecha_alta": { "type": "string", "format": "date", "description": "Registration date (YYYY-MM-DD)" },
    "_ingested_at": { "type": "string", "format": "date-time", "description": "Audit timestamp" },
    "_source": { "type": "string", "description": "Source system origin" },
    "_batch_id": { "type": "string", "description": "Ingestion batch UUID" }
  }
}
```

#### Entity: `pedido` (PostgreSQL Source & Bronze/Silver Lakehouse)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Pedido",
  "type": "object",
  "required": ["id", "id_cliente", "fecha", "estado", "importe"],
  "properties": {
    "id": { "type": "integer", "minimum": 1, "description": "Order primary key" },
    "id_cliente": { "type": "integer", "minimum": 1, "description": "Foreign key to Cliente" },
    "fecha": { "type": "string", "format": "date", "description": "Order date (YYYY-MM-DD)" },
    "estado": { "type": "string", "enum": ["confirmado", "pendiente", "enviado", "cancelado"] },
    "importe": { "type": "number", "minimum": 0.01, "maximum": 99999.99, "description": "Monetary total in EUR" },
    "metodo_pago": { "type": "string", "enum": ["tarjeta", "paypal", "transferencia", "bizum"] },
    "canal": { "type": "string", "enum": ["web", "app", "tienda"] },
    "_ingested_at": { "type": "string", "format": "date-time" },
    "_source": { "type": "string" },
    "_batch_id": { "type": "string" }
  }
}
```

#### Entity: `linea_pedido` (PostgreSQL Source & Bronze/Silver Lakehouse)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "LineaPedido",
  "type": "object",
  "required": ["id", "id_pedido", "id_producto", "cantidad", "precio_unitario", "subtotal"],
  "properties": {
    "id": { "type": "integer", "minimum": 1 },
    "id_pedido": { "type": "integer", "minimum": 1 },
    "id_producto": { "type": "integer", "minimum": 1 },
    "cantidad": { "type": "integer", "minimum": 1 },
    "precio_unitario": { "type": "number", "minimum": 0.01 },
    "subtotal": { "type": "number", "minimum": 0.01 }
  }
}
```

#### Entity: `evento_navegacion` (Kafka Stream & Stream Iceberg Buffer)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "EventoNavegacion",
  "type": "object",
  "required": ["event_id", "session_id", "timestamp", "event_type", "url"],
  "properties": {
    "event_id": { "type": "string", "format": "uuid" },
    "session_id": { "type": "string" },
    "user_id": { "type": ["integer", "null"] },
    "timestamp": { "type": "string", "format": "date-time" },
    "event_type": { "type": "string", "enum": ["page_view", "product_view", "add_to_cart", "checkout_start", "purchase"] },
    "url": { "type": "string" },
    "referrer": { "type": "string" },
    "product_id": { "type": ["integer", "null"] },
    "ip_address": { "type": "string" },
    "user_agent": { "type": "string" }
  }
}
```

#### Entity: `campana` (PostgreSQL Source & Marketing Lakehouse)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Campana",
  "type": "object",
  "required": ["id", "nombre", "canal", "presupuesto", "fecha_inicio", "fecha_fin"],
  "properties": {
    "id": { "type": "integer", "minimum": 1 },
    "nombre": { "type": "string", "maxLength": 100 },
    "canal": { "type": "string", "enum": ["email", "sms", "display", "social", "search"] },
    "presupuesto": { "type": "number", "minimum": 0 },
    "fecha_inicio": { "type": "string", "format": "date" },
    "fecha_fin": { "type": "string", "format": "date" },
    "estado": { "type": "string", "enum": ["activa", "finalizada", "pausada", "borrador"] },
    "objetivo_leads": { "type": "integer", "minimum": 0 }
  }
}
```

---

## 8. Data Constraints & Sizing

### 8.1 Data Volume and Generation Bounds
- **OLTP PostgreSQL Database**:
  - `customers`: exactly 1,000 records (~150 KB).
  - `orders`: exactly 50,000 records (~5.5 MB).
  - `order_items`: exactly 200,000 records (~18 MB).
  - Total relational disk footprint: **<= 50 MB**.
- **Kafka Streaming Buffer**:
  - Event generation rate: 10 to 50 events/sec.
  - Active retention: 24 hours (or 100,000 events max).
  - Kafka log segment footprint: **<= 100 MB**.
- **MinIO Lakehouse Parquet Storage**:
  - Bronze Tier: ~30 MB (uncompressed JSON/Parquet snapshots).
  - Silver Tier: ~25 MB (Snappy-compressed Parquet with dictionary encoding).
  - Gold Tier: ~5 MB (aggregated multi-dimensional summaries).
  - Total S3 bucket storage: **<= 200 MB**.

### 8.2 Retention, Compaction & Purge Policies
- **Snapshot Retention**: Iceberg snapshot expiration configured for 7 days retention (`history.expire.max-snapshot-age-ms = 604800000`).
- **Small File Compaction**: Spark jobs schedule bin-pack compaction rewrite actions when table partitions accumulate more than 10 data files smaller than 16 MB.
- **GDPR Deletion**: Hard deletion requests for customer records in Silver are honored by issuing Iceberg equality delete files or copy-on-write row rewrites.

---

## 9. Acceptance Criteria & Verification

### 9.1 Verification Scenarios (Given / When / Then)

#### Scenario AC-01: Synthetic OLTP Generation & Idempotency (RF-01)
- **Given** an initialized and running PostgreSQL container,
- **When** the operator runs `python data-generator/oltp_seed.py` multiple consecutive times,
- **Then** the script must complete with exit code `0`, and SQL verification queries must confirm exactly 1,000 customers, 50,000 orders, and 200,000 order lines without record duplication.

#### Scenario AC-02: Quality Gate Enforces Business SLA & Contract (RF-18, RF-15)
- **Given** a populated `silver.sales.orders` Iceberg table,
- **When** `run_quality_gate.py` executes against `silver_orders_suite.json`,
- **Then** the validation suite must verify that 100% of `id` values are unique, completeness across required fields meets or exceeds 0.98, all order amounts exceed 0.01, and the script exits with code `0`.
- **When** synthetic corrupt rows (e.g., negative order amounts or null primary keys) are injected into the dataset,
- **Then** `run_quality_gate.py` must halt pipeline progression, output a failure report, and return exit code `1`.

#### Scenario AC-03: Unified Batch and Stream Query Execution (RF-11)
- **Given** historical order records populated in `gold.sales.daily_metrics` and real-time streaming records ingested by Flink into `stream.sales.orders_windowed`,
- **When** a user queries `SELECT * FROM serving.sales.unified_revenue` in Trino,
- **Then** Trino must return a consolidated time series displaying both historical and real-time records without double counting or query syntax errors.

#### Scenario AC-04: GDPR PII Masking Verification (RF-19)
- **Given** customer records in Bronze containing plaintext names and email addresses,
- **When** the transformation progresses through Silver to Gold (`gold_aggregation.py`),
- **Then** the Gold table and serving views must contain only SHA-256 hashed emails (`^[a-f0-9]{64}$`), truncated names ("Juan P."), and truncated registration years, ensuring zero plaintext PII leaks.

#### Scenario AC-05: Memory Ceiling Compliance (RNF-07, RC-02)
- **Given** the deployment of any startup profile (`Platform`, `Stream`, `Batch`, `Demo`),
- **When** total system memory consumption across all Docker containers is polled via `docker stats --no-stream`,
- **Then** the aggregate memory usage must remain strictly below 8,192 MB (8 GB) at all times.

---

## 10. Traceability Matrix

| Requirement ID | Subsystem | Implementation File / Component | Verification Method |
| :--- | :--- | :--- | :--- |
| **RF-01** | S1 | `data-generator/oltp_seed.py` | Automated CLI execution & SQL count |
| **RF-02** | S1 | `data-generator/event_producer.py` | Kafka consumer message inspection |
| **RF-03** | S1, S2, S3 | `pipelines/medallion/bronze_ingestion.py` | Iceberg Bronze metadata inspection |
| **RF-04** | S1, S2, S4 | `pipelines/stream/flink_orders_stream.py` | Flink job execution & table query |
| **RF-05** | S2, S3 | `pipelines/medallion/bronze_ingestion.py` | Bronze table schema & audit check |
| **RF-06** | S2, S3 | `pipelines/medallion/silver_transform.py` | Primary key deduplication check |
| **RF-07** | S2, S3 | `pipelines/medallion/gold_aggregation.py` | KPI query verification in Trino |
| **RF-08** | S2 | Apache Iceberg + MinIO | Snapshot inspection via `$snapshots` |
| **RF-09** | S2 | `pipelines/serving/catalog_registration.py` | Polaris REST API endpoint query |
| **RF-10** | S3, S4 | Full recomputation in `gold_aggregation.py` / `rake stream:replay` (log replay) | Historical re-run idempotency test |
| **RF-11** | S5 | `pipelines/serving/unified_view.sql` | Trino SQL `UNION ALL` query check |
| **RF-12** | S5 | Trino Engine | Cross-domain multi-table join test |
| **RF-13** | S6 | `domains/sales`, `domains/marketing` | Directory structure & autonomy audit |
| **RF-14** | S6 | `domains/*/data-products/*/product.yaml` | YAML schema validation |
| **RF-15** | S6 | `domains/*/data-products/*/contract.yaml` | Bitol ODCS compliance validation |
| **RF-16** | S6 | `quality/gates/run_quality_gate.py` | Backward compatibility test |
| **RF-17** | S7 | Spark/Flink OpenLineage -> Marquez | Marquez API GET lineage graph check |
| **RF-18** | S7 | `quality/gates/run_quality_gate.py` | Quality-gate exit code check (0 pass / 1 blocked / 2 error) |
| **RF-19** | S3, S7 | `pipelines/medallion/gold_aggregation.py` | Regex audit for SHA-256 in Gold |
| **RF-20** | S7 | Kyverno / Kubernetes manifests | Admission control test |
| **RF-21** | S8 | Dagster SDA Definitions | Dagster UI asset materialization |
| **RF-22** | S8 | Prometheus & Grafana | Dashboard HTTP 200 & metrics scrape |
| **RF-23** | S8 | `infra/profiles/*.yaml` + `infra/helm` charts | `docker stats` memory audit <= 8 GB |
| **RF-24** | All | `scripts/demo.sh` | End-to-end automated script pass |
| **RF-25** | All | `README.md`, `docs/*` | Formal documentation review |

---

## 11. Risks and Mitigations

| Risk ID | Risk Description | Severity | Probability | Mitigation Strategy |
| :--- | :--- | :--- | :--- | :--- |
| **RSK-01** | JVM compute engines (Spark, Flink, Trino) exceed 8 GB host RAM, triggering OOM-killer. | Critical | High | Implement container memory limits (`mem_limit`), tune JVM heap flags (`-Xmx512m` to `-Xmx1536m`), and enforce startup profiles executing batch and stream sequentially. |
| **RSK-02** | Kafka producer swamps Flink streaming job causing memory buffer overflow. | High | Medium | Implement token-bucket rate limiting in `event_producer.py` and configure Flink RocksDB state backend with bounded memory. |
| **RSK-03** | Schema changes deployed by producer domain break downstream consumer SQL queries. | High | Medium | Enforce declarative Data Contracts (`contract.yaml`) with automated CI compatibility validation before merging schema pull requests. |
| **RSK-04** | REST catalog connectivity loss between compute engines and Apache Polaris. | Medium | Low | Implement exponential backoff retry policies in `SparkSessionBuilder`, Flink sink connectors, and Trino catalog clients. |
| **RSK-05** | PII data leaks into unmasked Gold analytical views or dashboards. | High | Low | Enforce automated PII hashing during Silver-to-Gold transformations and run quality-suite assertions blocking unmasked data. |

---

## 12. Operator Decisions & Future Roadmap

1. **CDC Ingestion (Debezium Integration)**: Future iterations may replace the periodic JDBC snapshot ingestion in `bronze_ingestion.py` with continuous Debezium Change Data Capture (CDC) streaming PostgreSQL WAL logs directly into Kafka topics.
2. **Kubernetes Helm Production Packaging**: Transition local Docker Compose profiles into production-grade Helm charts with Horizontal Pod Autoscaling (HPA) and automated volume provisioning.
3. **Advanced Semantic Layer & Metric Stores**: Integrate Cube or Apache Pinot on top of the Trino serving layer to accelerate sub-second dashboard rendering for external client-facing analytics.
4. **Dynamic Data Contract Negotiation Portal**: Build an interactive web portal enabling consumers to subscribe to data products, negotiate SLAs, and request schema additions directly through GitOps workflows.
