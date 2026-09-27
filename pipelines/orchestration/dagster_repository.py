"""
Modern Data Architecture POC - Dagster Software-Defined Assets & Jobs
Implements Medallion pipeline orchestration (Bronze -> Silver -> Gold),
Quality gates execution, and Kappa streaming replay triggers.
All asset names, tables, and job definitions are in English.
"""

from dagster import (
    AssetIn,
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from pipelines.orchestration.resources import (
    FlinkResource,
    GreatExpectationsResource,
    MarquezResource,
    SparkResource,
)

# -------------------------------------------------------------------------
# Medallion Architecture Assets (S2)
# -------------------------------------------------------------------------

@asset(group_name="medallion_bronze", description="Raw order transactions ingested idempotently from PostgreSQL")
def bronze_orders(spark: SparkResource) -> dict:
    spark.submit_job("pipelines/medallion/bronze_ingestion.py", ["--entity", "orders"])
    return {"table": "bronze.sales.orders", "status": "ingested"}


@asset(group_name="medallion_bronze", description="Raw customer master records from OLTP")
def bronze_customers(spark: SparkResource) -> dict:
    spark.submit_job("pipelines/medallion/bronze_ingestion.py", ["--entity", "customers"])
    return {"table": "bronze.sales.customers", "status": "ingested"}


@asset(
    group_name="medallion_silver",
    ins={"bronze_orders": AssetIn()},
    description="Cleaned, typed, deduplicated orders with quality gates applied"
)
def silver_orders(bronze_orders, spark: SparkResource, ge: GreatExpectationsResource) -> dict:
    spark.submit_job("pipelines/medallion/silver_transform.py", ["--entity", "orders"])
    passed = ge.run_suite("quality/expectations/silver_orders_suite.json", "silver.sales.orders")
    if not passed:
        raise ValueError(
            "Quality gate FAILED for silver.sales.orders "
            "(suite quality/expectations/silver_orders_suite.json); asset not promoted."
        )
    return {"table": "silver.sales.orders", "status": "validated"}


@asset(
    group_name="medallion_silver",
    ins={"bronze_customers": AssetIn()},
    description="Conformed customer dimensions with PII pseudonymization"
)
def silver_customers(bronze_customers, spark: SparkResource, ge: GreatExpectationsResource) -> dict:
    spark.submit_job("pipelines/medallion/silver_transform.py", ["--entity", "customers"])
    passed = ge.run_suite("quality/expectations/silver_customers_suite.json", "silver.sales.customers")
    if not passed:
        raise ValueError(
            "Quality gate FAILED for silver.sales.customers "
            "(suite quality/expectations/silver_customers_suite.json); asset not promoted."
        )
    return {"table": "silver.sales.customers", "status": "validated"}


@asset(
    group_name="medallion_gold",
    ins={"silver_orders": AssetIn(), "silver_customers": AssetIn()},
    description="Curated daily sales metrics and revenue aggregations for serving"
)
def gold_daily_metrics(silver_orders, silver_customers, spark: SparkResource) -> dict:
    spark.submit_job("pipelines/medallion/gold_aggregation.py", ["--target", "daily_metrics"])
    return {"table": "gold.sales.daily_metrics", "status": "published"}


# -------------------------------------------------------------------------
# Jobs & Schedules
# -------------------------------------------------------------------------

batch_medallion_job = define_asset_job(
    name="batch_medallion_job",
    selection=AssetSelection.groups("medallion_bronze", "medallion_silver", "medallion_gold"),
    description="Executes end-to-end Medallion lakehouse processing from Bronze raw to Gold serving."
)

daily_batch_schedule = ScheduleDefinition(
    job=batch_medallion_job,
    cron_schedule="0 2 * * *",
    description="Daily full historical recalculation batch (Lambda architecture layer S3)"
)

hourly_ingestion_schedule = ScheduleDefinition(
    job=define_asset_job(name="hourly_bronze_job", selection=AssetSelection.groups("medallion_bronze")),
    cron_schedule="0 * * * *",
    description="Hourly snapshot ingestion from PostgreSQL OLTP into Bronze Iceberg tables"
)

defs = Definitions(
    assets=[
        bronze_orders,
        bronze_customers,
        silver_orders,
        silver_customers,
        gold_daily_metrics
    ],
    jobs=[batch_medallion_job],
    schedules=[daily_batch_schedule, hourly_ingestion_schedule],
    resources={
        "spark": SparkResource(),
        "flink": FlinkResource(),
        "marquez": MarquezResource(),
        "ge": GreatExpectationsResource()
    }
)
