# ADR-002: Apache Iceberg and Apache Polaris as Lakehouse Table Format and Catalog

## Status
Accepted

## Date
2026-09-26

## Context
Traditional data lakes built on raw object storage (e.g., CSV, JSON, or unmanaged Parquet files on S3/HDFS) introduce severe operational failure modes:
1. **Lack of ACID Guarantees**: Concurrent writes cause partial updates, phantom reads, and data corruption.
2. **Directory-Based Table Inefficiencies**: Relying on object storage directory listings (`S3 listObjects`) for partition pruning introduces significant query latency when scanning millions of files.
3. **Rigid Schema Evolution**: Renaming, adding, or deleting columns often corrupts historical data files or requires expensive full-table rewrites.
4. **Catalog Vendor Lock-in**: Historically, catalogs like AWS Glue, Databricks Unity Catalog, or Hive Metastore have either introduced heavy cloud-vendor lock-in or required legacy Hadoop infrastructure.

Our system requires a 100% open-source, multi-engine table format and metadata catalog that enables concurrent reading and writing across **Apache Spark**, **Apache Flink**, and **Trino**, with ACID transactions, snapshot isolation, partition evolution, and zero vendor lock-in.

## Decision
We select **Apache Iceberg (v2 specification)** as the authoritative Lakehouse open table format, combined with **Apache Polaris (incubating)** as the vendor-neutral REST catalog.

### Technical Implementation Details
1. **Apache Iceberg v2 Format**:
   - Stores data as Snappy-compressed Parquet files backed by immutable metadata trees (metadata JSON files, manifest lists, and manifest files).
   - Guarantees **serializable ACID transactions** via optimistic concurrency control (OCC).
   - Provides **snapshot isolation**, allowing Trino and Spark to query historical snapshots (time travel) without lock contention during active Flink or Spark writes.
   - Supports **hidden partitioning** and **partition evolution** without altering existing physical data directories or rewriting historical data files.
   - Enforces column-level statistics (min/max bounds, null counts) within metadata files, enabling Trino to skip non-matching files without opening Parquet footers.
2. **Apache Polaris REST Catalog**:
   - An open-source, multi-engine catalog implementing the standard Iceberg REST OpenAPI specification.
    - Serves as the single source of truth for namespace hierarchies (`sales`, `marketing`, `finance`, `stream`) and table commit points.
   - Eliminates legacy Hive Metastore / Thrift dependencies, running as a lightweight container consuming ~512 MB RAM.
   - Allows Spark, Flink, and Trino to use an identical catalog interface (`type: rest`, URI `http://polaris:8181`).
3. **MinIO Object Storage**:
   - Provides S3A-compliant object storage with bucket `lakehouse` containing directories `bronze/`, `silver/`, `gold/`, and `stream/`.

```
     +--------------------------------------------------------------------+
     |                     Apache Polaris REST Catalog                    |
     |                       (http://polaris:8181)                        |
     +--------------------------------------------------------------------+
               ^                               ^                      ^
               | (Metadata Commits)            | (Read Snapshots)     | (Table Lookup)
               v                               v                      v
     +-------------------+           +-------------------+   +--------------------+
     | Apache Spark 3.5  |           | Apache Flink 1.18 |   | Trino Query Engine |
     | (Batch Writes)    |           | (Stream Writes)   |   | (Analytics Reads)  |
     +-------------------+           +-------------------+   +--------------------+
               \                               |                      /
                \                              |                     /
                 v                             v                    v
     +--------------------------------------------------------------------+
     |             MinIO Object Storage (S3A: s3a://lakehouse/)           |
     |  - Metadata Tree: .metadata.json, snap-*.avro, manifest.avro       |
     |  - Data Files: *.snappy.parquet                                    |
     +--------------------------------------------------------------------+
```

## Consequences

### Positive
- **Multi-Engine Consistency**: Spark, Flink, and Trino interact with identical Iceberg tables with zero format conversion or data duplication.
- **Snapshot Isolation & Time Travel**: Queries can reference specific snapshot IDs or historical timestamps (e.g., `FOR SYSTEM_TIME AS OF`), facilitating automated regression testing and auditing.
- **Efficient File Pruning**: Iceberg's manifest-level metrics minimize MinIO GET requests, ensuring Trino queries meet the < 3.0s P99 latency requirement (RNF-01).
- **100% Open Standards**: The entire Lakehouse tier adheres strictly to open specifications, complying with Constraint RC-01.

### Negative
- **Metadata Compaction Requirement**: High-frequency streaming writes from Flink generate numerous small snapshot metadata files, requiring periodic maintenance runs (expire snapshots, remove orphan files).
- **REST Catalog Immature Ecosystem**: Polaris is an incubating project; configuration requires strict adherence to OpenAPI payload formats.

## Alternatives Considered
- **Delta Lake**: Rejected due to multi-engine limitations. While Spark support is first-class, full multi-engine read/write support in Flink and Trino without proprietary Databricks extensions has historically lagged behind Iceberg.
- **Apache Hudi**: Rejected because its write path and metadata model are significantly more complex, requiring higher CPU/memory overhead that challenges our 8 GB RAM constraint (RC-02).
- **Hive Metastore (HMS)**: Rejected due to heavy legacy dependencies (Hadoop, Java Thrift, MySQL backend) and lack of native support for modern Iceberg REST catalog features.

## Compliance & Verification
- **Functional Requirements**: Satisfies RF-05, RF-06, RF-07, RF-08, and RF-09.
- **Verification**: Verified via `pipelines/serving/catalog_registration.py` and Trino snapshot queries (`SELECT * FROM "gold.sales.daily_metrics$snapshots"`).
