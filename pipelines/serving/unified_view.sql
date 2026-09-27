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
--   src/use_cases/query_serving.py :: QueryServingUseCase.execute_unified_revenue_query,
--   used when Trino is enabled. It expresses the EXACT same Lambda fusion
--   semantics: batch wins on (order_date, status) key collision and stream
--   rows only fill keys absent from batch (NOT EXISTS anti-join), so no key
--   is counted twice. The logical table names below (gold.sales.daily_metrics,
--   stream.sales.orders_windowed - also accepted in iceberg.* catalog form)
--   are exactly those the serving adapter rewrites for embedded SQLite.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.serving;
CREATE SCHEMA IF NOT EXISTS iceberg.sales;
CREATE SCHEMA IF NOT EXISTS iceberg.marketing;
CREATE SCHEMA IF NOT EXISTS iceberg.finance;

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
-- VIEW 2: iceberg.serving.marketing_unified_campaign_performance
-- PLANNED (not deployed): iceberg.marketing.funnel_metrics is not produced
-- by any pipeline in this POC (see PIPELINE_JOBS in catalog_registration.py).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW iceberg.serving.marketing_unified_campaign_performance AS
SELECT
    c.id                                           AS campaign_id,
    c.channel                                      AS channel,
    c.budget                                       AS allocated_budget,
    COALESCE(f.total_clicks, 0)                    AS total_clicks,
    COALESCE(f.total_purchases, 0)                 AS total_conversions,
    CAST(COALESCE(f.attributed_revenue, 0) AS DECIMAL(14, 2)) AS total_revenue
FROM iceberg.marketing.campaigns c
LEFT JOIN iceberg.marketing.funnel_metrics f
    ON c.channel = f.channel;

-- ---------------------------------------------------------------------------
-- VIEW 3: iceberg.serving.finance_unified_pl_summary
-- PLANNED (not deployed): iceberg.finance.financial_closings is not produced
-- by any pipeline in this POC (see PIPELINE_JOBS in catalog_registration.py).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW iceberg.serving.finance_unified_pl_summary AS
SELECT
    fiscal_year,
    fiscal_period,
    gross_revenue,
    net_revenue,
    total_orders,
    status AS audit_status,
    closed_at
FROM iceberg.finance.financial_closings;
