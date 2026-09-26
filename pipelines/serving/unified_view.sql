-- =============================================================================
-- unified_view.sql - Trino Serving Layer Unified Views
-- Implements RF-11: Unified Batch + Stream Query Interface
--
-- Purpose:
--   Presents consumers with a single query surface that transparently
--   merges historical batch data (Gold Lakehouse) with real-time stream
--   data (Speed tables) without leaking internal storage architecture.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS serving.sales;
CREATE SCHEMA IF NOT EXISTS serving.marketing;
CREATE SCHEMA IF NOT EXISTS serving.finance;
CREATE SCHEMA IF NOT EXISTS serving.cross_domain;

-- ---------------------------------------------------------------------------
-- VIEW 1: serving.sales.unified_revenue (RF-11)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW serving.sales.unified_revenue AS

-- 1. Historical Batch Layer (Iceberg Gold)
SELECT
    CAST(order_date AS DATE)                       AS order_date,
    status                                         AS order_status,
    CAST(gross_revenue AS DECIMAL(14, 2))          AS total_revenue,
    total_orders                                   AS total_orders,
    CAST(average_order_value AS DECIMAL(10, 2))    AS avg_order_value,
    'batch_historical'                             AS source_tier
FROM iceberg.gold.sales_daily_metrics
WHERE order_date < CURRENT_DATE

UNION ALL

-- 2. Near Real-Time Stream Layer (Flink Window Aggregates)
SELECT
    CAST(window_end AS DATE)                       AS order_date,
    status                                         AS order_status,
    CAST(window_revenue AS DECIMAL(14, 2))         AS total_revenue,
    window_orders                                  AS total_orders,
    CAST(window_revenue / NULLIF(window_orders, 0) AS DECIMAL(10, 2)) AS avg_order_value,
    'stream_speed'                                 AS source_tier
FROM iceberg.stream.sales_orders_windowed
WHERE window_end >= CURRENT_DATE;

-- ---------------------------------------------------------------------------
-- VIEW 2: serving.marketing.unified_campaign_performance
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW serving.marketing.unified_campaign_performance AS
SELECT
    c.id                                           AS campaign_id,
    c.channel                                      AS channel,
    c.budget                                       AS allocated_budget,
    COALESCE(f.total_clicks, 0)                    AS total_clicks,
    COALESCE(f.total_purchases, 0)                 AS total_conversions,
    CAST(COALESCE(f.attributed_revenue, 0) AS DECIMAL(14, 2)) AS total_revenue
FROM iceberg.gold.marketing_campaigns c
LEFT JOIN iceberg.stream.marketing_funnel_metrics f
    ON c.channel = f.channel;

-- ---------------------------------------------------------------------------
-- VIEW 3: serving.finance.unified_pl_summary
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW serving.finance.unified_pl_summary AS
SELECT
    fiscal_year,
    fiscal_period,
    gross_revenue,
    net_revenue,
    total_orders,
    status AS audit_status,
    closed_at
FROM iceberg.gold.finance_financial_closings;
