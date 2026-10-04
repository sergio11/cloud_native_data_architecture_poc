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
-- Note (VIEW 1):
--   sales_unified_revenue is the SQL equivalent of
--   src/use_cases/query_serving.py :: QueryServingUseCase.execute,
--   used when Trino is enabled. It expresses the EXACT same Lambda fusion
--   semantics: batch wins on (order_date, status) key collision and stream
--   rows only fill keys absent from batch (NOT EXISTS anti-join), so no key
--   is counted twice. The base tables below (gold.sales.daily_metrics,
--   stream.sales.orders_windowed) are the versioned sources; the serving SQL
--   itself references the LIVE VIEWS above verbatim (no rewriting on any
--   backend), while VIEW 1 keeps reading the base tables with identical
--   anti-join semantics.
--
-- Deployed objects in this file: schemas (serving/sales/marketing/finance)
-- + LIVE VIEWS gold_sales_daily_metrics_live / stream_sales_orders_windowed_live
-- (pass-through twins of the SQLite `<physical>_live` serving views, SSOT
-- src.domain.naming.serving_view_name - the names the serving SQL references
-- verbatim on every backend)
-- + VIEW 1 sales_unified_revenue (RF-11). Former marketing/finance VIEWS 2-3
-- were REMOVED (K5/K8): their source tables are produced by no pipeline, so
-- the file deploys exactly what the repo can honestly serve.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.serving;
CREATE SCHEMA IF NOT EXISTS iceberg.sales;
CREATE SCHEMA IF NOT EXISTS iceberg.marketing;
CREATE SCHEMA IF NOT EXISTS iceberg.finance;

-- ---------------------------------------------------------------------------
-- LIVE VIEWS: iceberg.serving.<physical>_live (serving no-rewrite contract)
-- Pure pass-through twins of the SQLite `<physical>_live` views the serving
-- SQL references verbatim (SSOT src.domain.naming.serving_view_name):
-- SQLite provisions them as `SELECT * ... WHERE _row_superseded IS NULL`
-- (or pass-through when that column is absent); here they are plain
-- pass-through over Iceberg because `_row_superseded` is a SQLite-only
-- versioning concept, unknown to Iceberg/Trino. Bare view names resolve
-- against the serving schema (TRINO_SCHEMA=serving in the serving adapter).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW iceberg.serving.gold_sales_daily_metrics_live AS
SELECT * FROM gold.sales.daily_metrics;

CREATE OR REPLACE VIEW iceberg.serving.stream_sales_orders_windowed_live AS
SELECT * FROM stream.sales.orders_windowed;

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
    FROM gold.sales.daily_metrics

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
    FROM stream.sales.orders_windowed s
    WHERE NOT EXISTS (
        SELECT 1
        FROM gold.sales.daily_metrics b
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
-- NONE of those three tables is produced by any pipeline in this repo:
--   * catalog_registration.py registers only gold.sales.daily_metrics,
--     gold.sales.customers, stream.sales.orders_windowed and
--     stream.marketing.clickstream_windowed (PIPELINE_JOBS: spark_sales_gold_etl,
--     flink_orders_stream, flink_clickstream_stream, unified_serving_views);
--   * the clickstream driver writes generic tumbling-window aggregates
--     (window_start/window_end/order_date/status/window_revenue/window_orders),
--     NOT funnel/campaign columns (no campaign_id/channel/budget/clicks);
--   * no finance pipeline exists at all.
-- Rewriting the views onto clickstream_windowed / daily_metrics would fake
-- campaign/finance semantics, so there is NO honest equivalent: the views are
-- deleted and only VIEW 1 (RF-11) is deployed. Re-introduce them only together
-- with the pipelines + catalog entries that produce their source tables.
-- ---------------------------------------------------------------------------
