-- =============================================================================
-- unified_view.sql - Trino Serving Layer Unified Views
-- Implements RF-11: Unified Batch + Stream Query Interface (no double counting)
--
-- Purpose:
--   Presents consumers with a single query surface that transparently
--   merges historical batch data (Gold Lakehouse) with real-time stream
--   data (Speed tables) without leaking internal storage architecture.
--   Uses standard 3-part ANSI SQL identifiers: <catalog>.<schema>.<table>
--   Stream windows (5-min tumbling grain) are aggregated to day grain
--   (order_date, status) BEFORE the anti-join so each key appears once.
--
-- Catalog change (2026-10-11):
--   Three Trino catalogs (iceberg/gold/stream) collapsed into ONE catalog
--   named "iceberg" pointing at the Polaris REST catalog. Tables that were
--   formerly in "gold." and "stream." Trino catalogs are now in Iceberg
--   namespaces "sales" and "stream" respectively, accessed via a single
--   catalog "iceberg":
--     OLD: gold.sales.daily_metrics      → NEW: iceberg.sales.daily_metrics
--     OLD: stream.sales.orders_windowed  → NEW: iceberg.stream.orders_windowed
--
--   The Flink jobs write to:
--     iceberg.sales.orders_windowed      (stream speed-layer table)
--   The Spark gold job writes to:
--     iceberg.sales.daily_metrics        (batch gold table)
--
-- Note (VIEW 1):
--   sales_unified_revenue is the SQL equivalent of
--   src/use_cases/query_serving.py :: QueryServingUseCase.execute,
--   used when Trino is enabled. It expresses the EXACT same Lambda fusion
--   semantics: batch wins on (order_date, status) key collision and stream
--   rows only fill keys absent from batch (NOT EXISTS anti-join), so no key
--   is counted twice.
--
-- Deployed objects in this file: schemas (serving/sales/stream/marketing/finance)
-- + LIVE VIEWS iceberg.serving.<name>_live
-- + VIEW 1 iceberg.serving.sales_unified_revenue (RF-11)
-- Former marketing/finance VIEWS 2-3 were REMOVED (K5/K8): their source
-- tables are produced by no pipeline. Do not re-add them without the pipelines.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.serving;
CREATE SCHEMA IF NOT EXISTS iceberg.sales;
CREATE SCHEMA IF NOT EXISTS iceberg.stream;
CREATE SCHEMA IF NOT EXISTS iceberg.marketing;
CREATE SCHEMA IF NOT EXISTS iceberg.finance;

-- ---------------------------------------------------------------------------
-- LIVE VIEWS: iceberg.serving.<physical>_live (serving no-rewrite contract)
-- Pure pass-through over Iceberg tables. `_row_superseded` is a SQLite-only
-- versioning concept unknown to Iceberg/Trino — no WHERE clause needed here.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW iceberg.serving.gold_sales_daily_metrics_live AS
SELECT * FROM iceberg.sales.daily_metrics;

CREATE OR REPLACE VIEW iceberg.serving.stream_sales_orders_windowed_live AS
SELECT * FROM iceberg.sales.orders_windowed;

-- ---------------------------------------------------------------------------
-- VIEW 1: iceberg.serving.sales_unified_revenue (RF-11)
-- Same semantics as QueryServingUseCase: batch exact values win, stream
-- windows merged to day grain only fill (order_date, status) keys absent
-- from batch - no double counting, no duplicate keys.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW iceberg.serving.sales_unified_revenue AS

SELECT
    order_date,
    status,
    total_orders,
    total_revenue,
    avg_order_value,
    source_tier
FROM (
    -- 1. Historical Batch Layer (Gold) - exact aggregates, always wins
    SELECT
        order_date                                   AS order_date,
        status                                       AS status,
        total_orders                                 AS total_orders,
        gross_revenue                                AS total_revenue,
        average_order_value                          AS avg_order_value,
        'batch_historical'                           AS source_tier
    FROM iceberg.sales.daily_metrics

    UNION ALL

    -- 2. Near Real-Time Stream Layer (Speed) - windows merged to day grain,
    -- only keys ABSENT from batch
    SELECT
        s.order_date                                 AS order_date,
        s.status                                     AS status,
        SUM(CAST(s.window_orders AS BIGINT))         AS total_orders,
        SUM(CAST(s.window_revenue AS DECIMAL(14,2))) AS total_revenue,
        SUM(CAST(s.window_revenue AS DECIMAL(14,2)))
            / NULLIF(SUM(CAST(s.window_orders AS BIGINT)), 0)
                                                     AS avg_order_value,
        'stream_speed'                               AS source_tier
    FROM iceberg.sales.orders_windowed s
    WHERE NOT EXISTS (
        SELECT 1
        FROM iceberg.sales.daily_metrics b
        WHERE b.order_date = s.order_date
          AND b.status = s.status
    )
    GROUP BY s.order_date, s.status
) unified
ORDER BY order_date DESC, status DESC;

-- Consumer usage (mirrors QueryServingUseCase parameters order_date_gte / limit):
--   SELECT * FROM iceberg.serving.sales_unified_revenue
--   WHERE order_date >= '2026-09-20'
--   ORDER BY order_date DESC, status DESC
--   LIMIT 20;


-- ---------------------------------------------------------------------------
-- VIEWS 2-3 REMOVED (2026-10-04, K8s remediation K5/K8) - DO NOT RE-ADD
-- WITHOUT A PRODUCING PIPELINE.
-- The former marketing_unified_campaign_performance read
-- iceberg.marketing.campaigns + iceberg.marketing.funnel_metrics and
-- finance_unified_pl_summary read iceberg.finance.financial_closings.
-- NONE of those three tables is produced by any pipeline in this repo.
-- Rewriting the views onto clickstream_windowed / daily_metrics would fake
-- campaign/finance semantics, so there is NO honest equivalent: the views are
-- deleted and only VIEW 1 (RF-11) is deployed.
-- ---------------------------------------------------------------------------
