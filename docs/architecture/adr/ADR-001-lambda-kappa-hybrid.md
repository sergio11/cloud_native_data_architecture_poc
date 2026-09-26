# ADR-001: Hybrid Lambda + Kappa Architecture Selection

## Status
Accepted

## Date
2026-09-26

## Context
Enterprise data platforms face an enduring tension between two fundamental processing requirements:
1. **Historical Depth and Accuracy**: Massive batch datasets require comprehensive data cleaning, complex relational joins, deduplication over wide time windows, and multi-dimensional aggregations that are computationally expensive and best performed in batch mode.
2. **Real-Time Operational Responsiveness**: Business applications, fraud detection, and customer personalization demand sub-minute or near-real-time visibility into incoming event streams (e.g., website clickstream, current-day orders).

Traditional architectural patterns typically choose one extreme:
- **Pure Lambda Architecture**: Runs two separate processing pipelines—a batch layer (for comprehensive, accurate historical data) and a speed layer (for low-latency recent data)—joined at query time. Historically, this suffered from code duplication (e.g., writing the same business logic in MapReduce/Spark and Storm) and complex operational reconciliation.
- **Pure Kappa Architecture**: Eliminates the batch layer entirely by treating all data as a continuous stream of events. Historical reprocessing is achieved by rewinding stream offsets and replaying events through the stream processing engine. While elegant, pure Kappa is inefficient for complex relational operations, large dimensional joins, and retrospective updates across massive historical partitions.

Furthermore, our system operates under a strict **8 GB RAM memory constraint** (RC-02), making it impractical to run heavy, stateful streaming jobs alongside massive batch executors simultaneously.

## Decision
We select a **Hybrid Lambda + Kappa Architecture** that synthesizes the strengths of both paradigms while mitigating their historical pitfalls:

1. **Processing Plane Separation**:
   - **Batch Layer (Lambda)**: Powered by **Apache Spark 3.5**. Ingests relational snapshots from PostgreSQL, transforms raw data across Bronze and Silver Medallion tiers, and produces dimensional Gold models with high computational efficiency.
   - **Speed / Stream Layer (Kappa)**: Powered by **Apache Flink 1.18**. Ingests high-velocity JSON events from Apache Kafka, computes 5-minute tumbling window metrics, and writes streaming buffers directly to Apache Iceberg.
2. **Kappa-Style Historical Replay (RF-10)**:
   - Both the batch pipelines (`gold_aggregation.py` recomputes Gold fully from Silver on every run — that full recomputation *is* the Lambda replay; no `--replay` flag exists) and the streaming pipelines (cursor-free full-log replay via `flink_orders_stream.py --replay`) support idempotent historical recomputation. If business algorithms change, data can be reprocessed from Bronze or the event log without altering raw historical records.
3. **Unified Serving Layer (Trino)**:
   - Rather than maintaining complex dual-view reconciliation in application code, the serving layer executes a unified SQL query (`src/use_cases/query_serving.py`, mirrored in `pipelines/serving/unified_view.sql` for Trino). The query fuses both tiers with a `UNION ALL` plus an anti-join: historical data is served from the Gold batch tables, while stream rows only fill `(order_date, status)` keys **absent** from batch — the exact batch values win on collision, which guarantees zero record duplication (no date-boundary heuristic).

```
                    +------------------------------------+
                    |        Incoming Data Sources       |
                    +------------------------------------+
                       /                              \
                      /                                \
           (Batch Snapshot)                       (Real-Time Events)
                    v                                    v
     +-----------------------------+      +-----------------------------+
     |   Apache Spark Batch ETL    |      |  Apache Flink Stream Engine |
     |  (Bronze -> Silver -> Gold) |      | (5-min Tumbling Windows)    |
     +-----------------------------+      +-----------------------------+
                    |                                    |
                    v                                    v
     +-----------------------------+      +-----------------------------+
     | Iceberg Gold Batch Tables   |      | Iceberg Stream Buffer Tables|
     | (Historical T+1 Day)        |      | (Near-Real-Time T+5 Min)    |
     +-----------------------------+      +-----------------------------+
                    \                                    /
                     \                                  /
                      v                                v
     +------------------------------------------------------------------+
     |                 Trino Unified Serving Layer                      |
     |       (serving.ventas.revenue_unified: UNION ALL with Filter)    |
     +------------------------------------------------------------------+
```

## Consequences

### Positive
- **Optimal Engine Allocation**: Heavy relational transformations, deduplication, and PII masking are handled by Spark, which excels at memory-efficient batch processing. Event-driven windowing is handled by Flink, which excels at low-latency streaming.
- **Single Query Interface**: Consumers query a standard Trino view (`serving.ventas.revenue_unified`) without needing to understand the underlying physical storage or whether data originated from batch or stream.
- **Idempotency & Replayability**: Reprocessing historical data does not require manual pipeline recreation; the stream pipeline recomputes deterministically with `--replay`, and every Gold run is already a full recomputation.
- **Zero Double-Counting**: An anti-join on `(order_date, status)` in the serving SQL prevents overlapping records between Gold batch partitions and active stream buffers (batch wins on collision).

### Negative
- **Dual Engine Maintenance**: The engineering team must maintain codebase components for both PySpark and PyFlink.
- **Operational Sequencing**: Under the 8 GB RAM constraint, Spark batch jobs and Flink streaming jobs cannot run concurrently; they must be executed sequentially via operational profiles.

## Alternatives Considered
- **Pure Kappa Architecture (Flink Only)**: Rejected because running Flink continuously for historical multi-year dimensional modeling and complex window joins over 200,000 order lines exceeds our 8 GB RAM budget and increases state backend complexity.
- **Pure Lambda Architecture with Hive/Presto**: Rejected due to the legacy burden of dual-code maintenance and lack of ACID transactional table formats.

## Compliance & Verification
- **Functional Requirements**: Satisfies RF-03, RF-04, RF-10, and RF-11.
- **Verification**: Verified via `pipelines/serving/unified_view.sql` and the automated test scenario in `scripts/demo.sh`.
