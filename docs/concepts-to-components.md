# Theoretical Concepts to Concrete Implementation Mapping
## Modern Data Engineering (Volume II) Cross-Reference Guide

---

## Overview

This document provides a comprehensive, bidirectional mapping between the foundational theoretical paradigms of modern data engineering and their concrete, production-grade implementations in this repository. Every concept is accompanied by its architectural rationale, design pattern, and the exact files and lines of code realizing it.

---

## Master Conceptual Matrix

| Modern Data Concept | Key Theoretical Tenet | Repository Implementation | Primary Files & Artifacts |
| :--- | :--- | :--- | :--- |
| **Medallion Architecture** | Progressive raw-to-refined data curation across Bronze, Silver, and Gold tiers. | Multi-tier Iceberg tables stored on MinIO object storage. | [`pipelines/medallion/bronze_ingestion.py`](../pipelines/medallion/bronze_ingestion.py)<br>[`pipelines/medallion/silver_transform.py`](../pipelines/medallion/silver_transform.py)<br>[`pipelines/medallion/gold_aggregation.py`](../pipelines/medallion/gold_aggregation.py) |
| **Lakehouse Table Format** | ACID transactions, snapshot isolation, partition evolution, and time-travel on object storage. | Apache Iceberg v2 open specification with Parquet columnar storage. | Spark Iceberg Catalog config in [`pipelines/medallion/bronze_ingestion.py`](../pipelines/medallion/bronze_ingestion.py)<br>Catalog metadata sync in [`pipelines/serving/catalog_registration.py`](../pipelines/serving/catalog_registration.py) |
| **Lakehouse REST Catalog** | Open, engine-agnostic metadata management decoupling catalog service from storage. | Apache Polaris (incubating) REST catalog. | [`pipelines/serving/catalog_registration.py`](../pipelines/serving/catalog_registration.py)<br>Trino catalog configuration |
| **Lambda Architecture** | Separate batch (accuracy) and speed (latency) processing layers unified at query time. | Spark batch ETL + Flink streaming windowing joined via Trino views. | [`pipelines/serving/unified_view.sql`](../pipelines/serving/unified_view.sql)<br>[`docs/architecture/adr/ADR-001-lambda-kappa-hybrid.md`](architecture/adr/ADR-001-lambda-kappa-hybrid.md) |
| **Kappa Architecture** | Everything is an event stream; historical backfills executed via stream replay. | Consumer-cursor rewinds + full-log replay (`flink_orders_stream.py --replay`); batch side = full Gold recomputation on every run (no flag needed). | [`pipelines/stream/flink_orders_stream.py`](../pipelines/stream/flink_orders_stream.py)<br>[`pipelines/medallion/gold_aggregation.py`](../pipelines/medallion/gold_aggregation.py) |
| **Unified Serving Layer** | Single SQL query surface abstracting storage topology and data freshness latency. | One anti-join SQL (`UNION ALL` + `NOT EXISTS` on `(order_date, status)`; batch wins, no double counting) — Trino if `TRINO_HOST`, else embedded SQLite. | [`pipelines/serving/unified_view.sql`](../pipelines/serving/unified_view.sql) |
| **Data Mesh** | Decentralized domain ownership, data as a product, and self-serve platform. | Domain-segregated repositories (`sales`, `marketing`, `finance`). | [`domains/sales/`](../domains/sales/)<br>[`domains/marketing/`](../domains/marketing/)<br>[`docs/architecture/adr/ADR-003-multi-domain-mesh.md`](architecture/adr/ADR-003-multi-domain-mesh.md) |
| **Data Products** | Autonomous data delivery nodes with code, metadata, schemas, SLAs, and ports. | Standardized `product.yaml` descriptors and schema specifications. | [`domains/sales/data-products/orders/product.yaml`](../domains/sales/data-products/orders/product.yaml)<br>[`domains/marketing/data-products/campaigns/product.yaml`](../domains/marketing/data-products/campaigns/product.yaml) |
| **Data Contracts** | Declarative producer-consumer agreements defining schema, quality rules, and terms. | Bitol Open Data Contract Standard (ODCS) YAML specifications. | [`domains/sales/data-products/orders/contract.yaml`](../domains/sales/data-products/orders/contract.yaml)<br>[`domains/marketing/data-products/campaigns/contract.yaml`](../domains/marketing/data-products/campaigns/contract.yaml) |
| **Data Fabric & Lineage** | Active metadata harvesting generating dynamic, end-to-end lineage graphs. | OpenLineage Spark/Flink listeners emitting events to Marquez. | OpenLineage emitters in [`pipelines/medallion/bronze_ingestion.py`](../pipelines/medallion/bronze_ingestion.py)<br>Marquez client in [`pipelines/serving/catalog_registration.py`](../pipelines/serving/catalog_registration.py) |
| **Data Quality Gates** | Programmatic test suites blocking pipeline progression upon quality threshold breaches. | Fail-closed engine over Great-Expectations-format suites (missing suite / unknown type / empty data also block). Contract `spec.quality.thresholds` govern per dimension over the `0.95` fallback. | [`quality/gates/run_quality_gate.py`](../quality/gates/run_quality_gate.py)<br>[`quality/expectations/silver_orders_suite.json`](../quality/expectations/silver_orders_suite.json) |
| **Privacy & Governance** | GDPR Article 25 privacy-by-design, pseudonymization, and PII cryptographic hashing. | SHA-256 email hashing, name initial masking, and year-only truncation before Gold tier. | [`pipelines/medallion/gold_aggregation.py`](../pipelines/medallion/gold_aggregation.py)<br>PII tags in `contract.yaml` |
| **Modern Orchestration** | Software-Defined Assets (SDAs) tracking data lineage, materializations, and freshness. | Dagster asset graph coordinating pipelines and profiles. | Dagster asset definitions & execution profiles in [`docs/architecture/overview.md`](architecture/overview.md) |

---

## Detailed Paradigm Walkthroughs

### 1. Medallion Architecture (Bronze -> Silver -> Gold)
- **Theoretical Concept**: First popularized by Databricks, the Medallion Architecture organizes data into three refinement layers to guarantee both historical auditability and analytical performance:
  - **Bronze**: Raw, immutable, append-only ingestion tier storing the exact state of source systems, augmented with audit metadata.
  - **Silver**: Conformed, cleansed, deduplicated, and enriched tier adhering to standardized data types and business validation rules.
  - **Gold**: Consumption-ready, aggregated dimensional models and metrics optimized for business intelligence and reporting.
- **Repository Implementation**:
  - **Bronze Ingestion** ([`pipelines/medallion/bronze_ingestion.py`](../pipelines/medallion/bronze_ingestion.py)): Connects to PostgreSQL, extracts source tables without schema alteration, appends technical audit columns (`_ingested_at`, `_source`, `_batch_id`), and commits them to Iceberg tables located at `s3a://lakehouse/bronze/`.
  - **Silver Transformation** ([`pipelines/medallion/silver_transform.py`](../pipelines/medallion/silver_transform.py)): Reads Bronze records, strips leading/trailing whitespace, converts dates and decimals to contractual types, removes duplicate orders using window functions (`ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC)`), and checks referential integrity between orders and customers.
  - **Gold Aggregation** ([`pipelines/medallion/gold_aggregation.py`](../pipelines/medallion/gold_aggregation.py)): Reads Silver tables, performs business-level grouping (e.g., daily sales by order status, customer segment lifetime value), applies PII masking, and publishes to `gold.*`. Publishing is gated by the required suites `quality/expectations/gold_daily_metrics_suite.json` / `gold_customers_suite.json` (a missing suite blocks the run).

---

### 2. Lakehouse & Open Table Format (Apache Iceberg)
- **Theoretical Concept**: A Lakehouse combines the flexibility and cost efficiency of data lake object storage with the ACID transactions, data integrity, and performance of data warehouses. Apache Iceberg achieves this by introducing an immutable, file-based metadata tree that abstracts physical Parquet files into a structured SQL table.
- **Key Features Realized**:
  - **ACID Transactions & Snapshot Isolation**: Concurrent reads and writes are managed through optimistic concurrency control. Readers always see a consistent, committed snapshot.
  - **Time Travel**: Analytical queries can inspect historical table states using snapshot IDs or timestamps:
    ```sql
    SELECT * FROM silver.sales.orders FOR SYSTEM_TIME AS OF '2026-09-26 10:00:00';
    ```
  - **Hidden Partitioning**: Partition transforms (e.g., `days(order_date)`) are tracked within Iceberg metadata, eliminating the need for user queries to include redundant partition columns in WHERE clauses.
- **Repository Implementation**:
  - Configured in Spark via `SparkSessionBuilder` in [`pipelines/medallion/bronze_ingestion.py`](../pipelines/medallion/bronze_ingestion.py):
    ```python
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", iceberg_catalog_url)
    ```

---

### 3. Lakehouse REST Catalog (Apache Polaris)
- **Theoretical Concept**: Earlier data lakes relied on the legacy Hive Metastore (HMS), which suffered from slow Thrift RPCs, single-point-of-failure relational database backends, and tight coupling to the Apache Hadoop ecosystem. The Apache Iceberg REST Catalog specification defines an open, lightweight HTTP/REST interface for discovering and committing table metadata.
- **Repository Implementation**:
  - **Apache Polaris**: Deployed as a lightweight container on port `8181`.
  - **Catalog Registration Script** ([`pipelines/serving/catalog_registration.py`](../pipelines/serving/catalog_registration.py)): Uses `PolarisClient` to authenticate, register namespaces (`sales`, `marketing`, `finance`, `stream`), inspect table metadata, and publish table URIs to Marquez and Data Product manifests.

---

### 4. Hybrid Lambda / Kappa Architecture & Serving Layer
- **Theoretical Concept**:
  - **Lambda Architecture**: Processes data through a batch layer (high accuracy, high computational cost, T+1 day latency) and a speed layer (stream processing, lower computational depth, T+5 minute latency).
  - **Kappa Architecture**: Eliminates dual codebases by executing all transformations through stream processing and replaying historical logs for backfills.
  - **Hybrid Approach**: Employs Spark batch for complex, multi-year relational dimensional models and Flink streaming for low-latency operational metrics, while ensuring both engines support historical replay (Kappa principle).
- **Repository Implementation**:
  - **Batch Pipeline**: [`pipelines/medallion/gold_aggregation.py`](../pipelines/medallion/gold_aggregation.py) aggregates historical orders daily.
  - **Stream Pipeline**: [`pipelines/stream/flink_orders_stream.py`](../pipelines/stream/flink_orders_stream.py) consumes events, applies 5-minute tumbling event-time windows, and writes to `stream.sales.orders_windowed`. Event backend: SQLite WAL by default; Kafka is canonical only when `KAFKA_BOOTSTRAP_SERVERS` is set (fail-fast, `src/adapters/streaming/event_stream.py`).
  - **Unified Serving View** ([`pipelines/serving/unified_view.sql`](../pipelines/serving/unified_view.sql)):
    ```sql
    CREATE OR REPLACE VIEW serving.sales.unified_revenue AS
    -- Historical batch layer (T+1 latency, guaranteed accuracy)
    SELECT CAST(order_date AS DATE) AS order_date, status, total_amount, num_orders, 'batch' AS source_type
    FROM gold.sales.daily_metrics
    WHERE CAST(order_date AS DATE) < CURRENT_DATE - INTERVAL '2' DAY

    UNION ALL

    -- Near-real-time streaming buffer (T+5m latency, last 2 days)
    SELECT CAST(window_start AS DATE) AS order_date, status, SUM(total_amount) AS total_amount, COUNT(*) AS num_orders, 'stream' AS source_type
    FROM stream.sales.orders_windowed
    WHERE CAST(window_start AS DATE) >= CURRENT_DATE - INTERVAL '2' DAY
    GROUP BY CAST(window_start AS DATE), status;
    ```

---

### 5. Data Mesh & Autonomous Data Products
- **Theoretical Concept**: Formulated by Zhamak Dehghani, Data Mesh shifts data architecture from centralized data lakes to decentralized domain ownership:
  1. **Domain Ownership**: Business domains closest to the data own their data models and pipelines.
  2. **Data as a Product**: Analytical data is packaged, documented, tested, and shared with the same rigor as external software products.
  3. **Self-Serve Data Infrastructure**: Domains utilize shared compute and storage platforms without relying on centralized data teams.
  4. **Federated Computational Governance**: Security, privacy, and quality standards are codified into automated platform policies.
- **Repository Implementation**:
  - Domain isolation: [`domains/sales/`](../domains/sales/) and [`domains/marketing/`](../domains/marketing/).
  - Product Manifest ([`domains/sales/data-products/orders/product.yaml`](../domains/sales/data-products/orders/product.yaml)):
    Defines metadata, data steward contacts, business SLAs (freshness, latency), data classification, input sources, and output ports (Iceberg tables and Trino views).

---

### 6. Declarative Data Contracts (Bitol ODCS)
- **Theoretical Concept**: A Data Contract is a formal, machine-readable agreement between a data producer and downstream data consumers. It defines the authoritative schema, semantic constraints, SLAs, compatibility rules, and legal/security terms, preventing unexpected schema drift from breaking downstream analytics.
- **Repository Implementation**:
  - Authoritative Contract ([`domains/sales/data-products/orders/contract.yaml`](../domains/sales/data-products/orders/contract.yaml)):
    - **Schema Constraints**:
      - `id`: integer, required, unique, min 1.
      - `total_amount`: decimal(10,2), required, min 0.01, max 99999.99.
      - `status`: string enum (`confirmed`, `pending`, `cancelled`, `shipped`).
    - **Quality Rules**:
      - `QR-001`: Completeness ratio >= 0.98.
      - `QR-002`: Uniqueness of primary key `id` == 1.0.
      - `QR-006`: Referential integrity against `silver.sales.customers`.
    - **Compatibility Policy**: `BACKWARD` with protected fields (`id`, `customer_id`, `total_amount`, `status`, `order_date`).
    - **Consumer Registration**: Explicitly authorizes `finance-team` and `marketing-team`.

---

### 7. Data Fabric, Lineage & Quality Gates
- **Theoretical Concept**: A Data Fabric automates metadata collection and quality enforcement across diverse data pipelines, providing continuous observability into data health and lineage.
- **Repository Implementation**:
  - **OpenLineage & Marquez**:
    - Every pipeline use case emits OpenLineage run events (`COMPLETE`/`FAIL`) via `LineagePublisherPort` (`src/adapters/lineage/marquez_lineage.py`): events are POSTed to Marquez when reachable and **always** appended to the local audit trail `data/lineage_audit.jsonl`.
    - Marquez records dataset facets, input/output schemas, and parent-child run relationships.
  - **Quality Gates (Great-Expectations-format suites)** ([`quality/gates/run_quality_gate.py`](../quality/gates/run_quality_gate.py)):
    - Evaluates JSON test suites ([`quality/expectations/silver_orders_suite.json`](../quality/expectations/silver_orders_suite.json)) with the built-in fail-closed engine (`src/adapters/quality/expectation_engine.py`) — not the Great Expectations library.
    - Blocks pipeline progression with exit code `1` if the quality score is below threshold (missing suite, unknown expectation type, or empty data also block); optionally emits alert payloads to webhooks.
    - Threshold precedence: per-dimension contract thresholds (`spec.quality.thresholds`) govern the verdict via the shared `src/use_cases/gate_support.py::enforce_quality_gate` collaborator (pure verdict in `src/domain/quality_gate.py::assess_gate`); the global/`--threshold` value (default `0.95`) applies only when the contract is silent, and contract dimensions the suite does not measure are skipped.

---

### 8. GDPR Privacy Compliance & Cryptographic Masking
- **Theoretical Concept**: GDPR Article 25 ("Data protection by design and by default") and Article 32 ("Security of processing") mandate that personal data must not be exposed in analytical stores where it is not strictly required for business calculations.
- **Repository Implementation**:
  - Implemented in [`pipelines/medallion/gold_aggregation.py`](../pipelines/medallion/gold_aggregation.py):
    - **Customer Email**: Transformed via irreversible SHA-256 cryptographic hash (`SHA2(email, 256)`).
    - **Customer Name**: Masked to first name and last initial only (e.g., "Alice S.").
    - **Registration Date**: Truncated to registration year (`YEAR(registration_date)`).
    - Gold tables and unified serving views expose zero plaintext PII attributes.

