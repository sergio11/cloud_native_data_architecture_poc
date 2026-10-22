-- =============================================================================
-- Trino serving layer unified views.
--
-- Single query surface merging batch Gold data with the real-time Speed tables,
-- using 3-part <catalog>.<schema>.<table> identifiers. Stream windows are
-- aggregated to day grain before the anti-join so each key appears once.
--
-- One catalog: "iceberg" points at the Polaris REST catalog; Flink writes
-- iceberg.sales.orders_windowed and Spark writes iceberg.sales.daily_metrics.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.serving;
CREATE SCHEMA IF NOT EXISTS iceberg.sales;
CREATE SCHEMA IF NOT EXISTS iceberg.stream;
CREATE SCHEMA IF NOT EXISTS iceberg.marketing;
CREATE SCHEMA IF NOT EXISTS iceberg.finance;

-- LIVE VIEWS: pure pass-through over Iceberg tables; `_row_superseded` is a
-- SQLite-only versioning concept unknown to Iceberg/Trino.
CREATE OR REPLACE VIEW iceberg.serving.gold_sales_daily_metrics_live AS
SELECT * FROM iceberg.sales.daily_metrics;

CREATE OR REPLACE VIEW iceberg.serving.stream_sales_orders_windowed_live AS
SELECT * FROM iceberg.sales.orders_windowed;

-- VIEW 1: same semantics as QueryServingUseCase.execute: batch exact values
-- win, stream windows merged to day grain fill only keys absent from batch.
CREATE OR REPLACE VIEW iceberg.serving.sales_unified_revenue AS

SELECT
    order_date,
    status,
    total_orders,
    total_revenue,
    avg_order_value,
    source_tier
FROM (
    -- Historical batch (Gold): exact aggregates, always wins.
    SELECT
        order_date                                   AS order_date,
        status                                       AS status,
        total_orders                                 AS total_orders,
        gross_revenue                                AS total_revenue,
        average_order_value                          AS avg_order_value,
        'batch_historical'                           AS source_tier
    FROM iceberg.sales.daily_metrics

    UNION ALL

    -- Real-time stream (Speed): day grain, only keys absent from batch.
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

-- Example: SELECT * FROM iceberg.serving.sales_unified_revenue
--          WHERE order_date >= CURRENT_DATE - INTERVAL '30' DAY
--          ORDER BY order_date DESC LIMIT 20;


-- Views 2-3 (marketing/finance) are intentionally absent: no pipeline produces
-- marketing.campaigns, marketing.funnel_metrics or finance.financial_closings.
