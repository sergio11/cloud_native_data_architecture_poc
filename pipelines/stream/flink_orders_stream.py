"""
flink_orders_stream.py - Apache Flink 1.18 speed-layer job for sales orders.

Kafka (sales.orders.events) -> event-time watermarks -> 4 s tumbling windows
keyed by status -> upsert into Iceberg stream.sales.orders_windowed. Submitted
by the Flink Kubernetes Operator; config comes only from the environment.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from typing import Any

# The PythonDriver runs this file from the image, where only its own directory is
# on sys.path. Make the repo root importable for the sibling pure module.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyflink.common import Duration, Row, Time, WatermarkStrategy  # noqa: E402
from pyflink.common.serialization import SimpleStringSchema  # noqa: E402
from pyflink.common.typeinfo import Types  # noqa: E402
from pyflink.datastream import DataStream, StreamExecutionEnvironment  # noqa: E402
from pyflink.datastream.checkpointing_mode import CheckpointingMode  # noqa: E402
from pyflink.datastream.connectors.kafka import (  # noqa: E402
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (  # noqa: E402
    AggregateFunction,
    FlatMapFunction,
    ProcessFunction,
)
from pyflink.datastream.window import TumblingEventTimeWindows  # noqa: E402
from pyflink.table import StreamTableEnvironment  # noqa: E402

# Deliberately no `pyflink.util.type_hints` import: PyFlink 1.18 does not ship
# it, so the import fails at module load. The window function below returns the
# rows to emit; DataStream.process() passes no Collector.
from pipelines.stream.window_logic import (  # noqa: E402
    ALLOWED_LATENESS_MS,
    MAX_OUT_OF_ORDERNESS_MS,
    WINDOW_SIZE_MS,
    OrderWindowEvent,
    build_orders_row,
    orders_row_to_columns,
    parse_order_event,
    window_start_ms,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("flink_orders_stream")

JOB_NAME = "flink-orders-speed-layer"
JOB_UID = "flink-orders-speed-layer"

#: Temporary view the Table API writes from.
TEMP_VIEW = "orders_speed_window"

#: Iceberg identifiers: Polaris namespace "sales", table "orders_windowed".
ICEBERG_CATALOG_TABLE = "polaris.sales.orders_windowed"


class ConfigurationError(RuntimeError):
    """Raised when a required environment variable is missing or unusable."""


def _required(name: str) -> str:
    """Reads a mandatory environment variable, failing closed when unset."""
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigurationError(
            f"Required environment variable {name} is not set. This job refuses to start on a "
            f"guessed default: declare it in the FlinkDeployment "
            f"(infra/helm/s4-stream/templates/flink-deployments.yaml)."
        )
    return value


@dataclass(frozen=True)
class JobConfig:
    """Runtime configuration resolved strictly from the environment."""

    bootstrap_servers: str
    topic: str
    group_id: str
    start_offset: str
    catalog_name: str
    catalog_uri: str
    warehouse: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    # OAuth2 credential for the Polaris 1.x REST catalog, "client_id:client_secret",
    # injected via POLARIS_OAUTH2_CREDENTIAL in the FlinkDeployment.
    oauth2_credential: str

    @property
    def oauth2_server_uri(self) -> str:
        """Polaris token endpoint, derived from the catalog URI.

        Iceberg must know where to fetch the OAuth2 token; deriving it from the
        catalog URI keeps one source of truth.
        """
        return f"{self.catalog_uri.rstrip('/')}/v1/oauth/tokens"

    @classmethod
    def from_env(cls) -> JobConfig:
        """Builds the configuration, raising ConfigurationError if incomplete."""
        start_offset = os.getenv("KAFKA_START_OFFSET", "latest").strip().lower()
        if start_offset not in ("earliest", "latest"):
            raise ConfigurationError(
                f"KAFKA_START_OFFSET must be 'earliest' or 'latest', got {start_offset!r}. "
                f"'earliest' is the RF-10 Kappa full-replay mode (regenerates every window from "
                f"offset 0); 'latest' resumes from the committed consumer group position."
            )
        return cls(
            bootstrap_servers=_required("KAFKA_BOOTSTRAP_SERVERS"),
            topic=os.getenv("KAFKA_TOPIC", "sales.orders.events").strip() or "sales.orders.events",
            group_id=os.getenv("KAFKA_GROUP_ID", "flink-orders-speed-group").strip() or "flink-orders-speed-group",
            start_offset=start_offset,
            catalog_name=os.getenv("ICEBERG_CATALOG_NAME", "polaris").strip() or "polaris",
            catalog_uri=_required("ICEBERG_CATALOG_URI"),
            warehouse=_required("ICEBERG_WAREHOUSE"),
            s3_endpoint=_required("S3_ENDPOINT"),
            s3_access_key=_required("S3_ACCESS_KEY"),
            s3_secret_key=_required("S3_SECRET_KEY"),
            oauth2_credential=_required("POLARIS_OAUTH2_CREDENTIAL"),
        )


class _ParseOrderEvent(FlatMapFunction):
    """Parses Kafka payloads; records without usable event time are dropped."""

    def flat_map(self, value: str) -> list[OrderWindowEvent]:
        event = parse_order_event(value)
        return [] if event is None else [event]


#: The aggregate accumulator: (total_revenue, order_count, status, max_event_ms).
#: A plain tuple, not a dataclass: PyFlink pickles module classes by reference to
#: __main__, where the TaskManager cannot find them and the job restart-loops.
Accumulator = tuple[float, int, str, int]


class _RevenueAggregate(AggregateFunction):
    """Accumulates revenue sum, order count and latest event time per window."""

    def create_accumulator(self) -> Accumulator:
        return (0.0, 0, "", 0)

    def add(self, value: OrderWindowEvent, accumulator: Accumulator) -> Accumulator:
        return (
            accumulator[0] + float(value.total_amount),
            accumulator[1] + 1,
            str(value.status),
            max(accumulator[3], int(value.event_time_ms)),
        )

    def get_result(self, accumulator: Accumulator) -> Row:
        return Row(
            accumulator[2],
            float(accumulator[0]),
            int(accumulator[1]),
            int(accumulator[3]),
        )

    def merge(self, acc_a: Accumulator, acc_b: Accumulator) -> Accumulator:
        return (
            acc_a[0] + acc_b[0],
            acc_a[1] + acc_b[1],
            acc_a[2] or acc_b[2],
            max(acc_a[3], acc_b[3]),
        )


class _OrdersWindowFunction(ProcessFunction):
    """Turns one aggregate result into the speed-layer row for its window.

    PyFlink 1.18 calls ``process_element(value, ctx)`` with no Collector and
    emits the returned values; ``ProcessWindowFunction`` is a different code
    path. The aggregate already ran the window, so this sees one element.
    """

    def process_element(self, value: Row, ctx: Any) -> list[Row]:
        status, revenue, orders, max_event_ms = value[0], value[1], value[2], value[3]
        window_start = window_start_ms(max_event_ms)
        row = build_orders_row(window_start, str(status), float(revenue), int(orders))
        return [Row(*orders_row_to_columns(row))]


class _AssignEventTime:
    """Timestamp assigner: the event time carried by the payload, never wall clock.

    A class, not a bare function: PyFlink 1.18 requires ``extract_timestamp``.
    """

    def extract_timestamp(self, event: OrderWindowEvent, record_timestamp: int) -> int:
        return int(event.event_time_ms)


def build_source_and_windows(env: StreamExecutionEnvironment, config: JobConfig) -> DataStream[Row]:
    """Builds the Kafka -> parse -> watermark -> 4 s tumbling window topology."""
    offsets = (
        KafkaOffsetsInitializer.earliest() if config.start_offset == "earliest" else KafkaOffsetsInitializer.latest()
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_topics(config.topic)
        .set_group_id(config.group_id)
        .set_starting_offsets(offsets)
        # Mandatory: KafkaSourceBuilder.build() fails without a deserialization
        # schema. SimpleStringSchema decodes the JSON payload to str.
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermarks = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(MAX_OUT_OF_ORDERNESS_MS))
        .with_timestamp_assigner(_AssignEventTime())
        .with_idleness(Duration.of_millis(MAX_OUT_OF_ORDERNESS_MS * 4))
    )

    # PyFlink 1.18 has no POJO TypeInformation, so the parsed dataclass travels
    # pickled and output_type/key_type stay unset.
    events = (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "orders-kafka-source")
        .name("orders-kafka-source")
        .uid("orders-kafka-source")
        .flat_map(_ParseOrderEvent())
        .name("parse-order-events")
        .uid("parse-order-events")
        .assign_timestamps_and_watermarks(watermarks)
        .name("order-event-time")
        .uid("order-event-time")
    )

    return (
        events.key_by(lambda event: event.status)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE_MS)))
        # allowed_lateness takes MILLISECONDS as an int, unlike .window() which
        # takes a Time; passing a Time crashes the window operator.
        .allowed_lateness(ALLOWED_LATENESS_MS)
        .aggregate(_RevenueAggregate())
        .process(
            _OrdersWindowFunction(),
            # Positional only: RowTypeInfo with named fields NPEs in the JVM on
            # PyFlink 1.18. Names are attached via an explicit Schema at view
            # registration.
            Types.ROW(
                [
                    Types.SQL_TIMESTAMP(),  # window_start
                    Types.SQL_TIMESTAMP(),  # window_end
                    Types.SQL_DATE(),  # order_date
                    Types.STRING(),  # status
                    Types.DOUBLE(),  # window_revenue
                    # Types.INT(), not Types.BIG_INT(): PyFlink 1.18 maps
                    # BIG_INT to java.math.BigInteger (RAW), which the Table API
                    # rejects at the sink. INTEGER widens to BIGINT cleanly and
                    # a 4-second window cannot overflow 32 bits.
                    Types.INT(),  # window_orders
                    Types.DOUBLE(),  # avg_order_value
                    Types.STRING(),  # source_tier
                ]
            ),
        )
        .name("orders-tumbling-window")
        .uid("orders-tumbling-window")
    )


#: Column names for the windowed stream, in the same order as the Types.ROW
#: above and as the Iceberg table definition.
WINDOW_COLUMNS = (
    "window_start",
    "window_end",
    "order_date",
    "status",
    "window_revenue",
    "window_orders",
    "avg_order_value",
    "source_tier",
)


def window_schema() -> Any:
    """Builds the Table schema that names the windowed stream's columns.

    PyFlink 1.18 does not apply it (the view keeps positional f0..f7), but it
    documents the intended column order and types that main() projects.
    """
    from pyflink.table import DataTypes, Schema

    types = [
        DataTypes.TIMESTAMP(6),
        DataTypes.TIMESTAMP(6),
        DataTypes.DATE(),
        DataTypes.STRING(),
        DataTypes.DOUBLE(),
        DataTypes.BIGINT(),
        DataTypes.DOUBLE(),
        DataTypes.STRING(),
    ]
    builder = Schema.new_builder()
    for name, data_type in zip(WINDOW_COLUMNS, types, strict=True):
        builder.column(name, data_type)
    return builder.build()


def register_iceberg_catalog(table_env: StreamTableEnvironment, config: JobConfig) -> None:
    """Registers the Polaris REST catalog and the speed-layer Iceberg table.

    Both statements are idempotent, so the job owns its own table definition and
    a restart never depends on out-of-band provisioning.
    """
    # Flink 1.18 rejects "CREATE CATALOG IF NOT EXISTS", so check the registered
    # catalogs and only create ours when missing. (CREATE TABLE IF NOT EXISTS works.)
    if config.catalog_name not in table_env.list_catalogs():
        table_env.execute_sql(
            f"""
            CREATE CATALOG `{config.catalog_name}` WITH (
                'type' = 'iceberg',
                'catalog-type' = 'rest',
                'uri' = '{config.catalog_uri}',
                'warehouse' = '{config.warehouse}',
                's3.endpoint' = '{config.s3_endpoint}',
                's3.access-key' = '{config.s3_access_key}',
                's3.secret-key' = '{config.s3_secret_key}',
                's3.path-style-access' = 'true',
                's3.region' = 'us-east-1',
                -- OAuth2 required by Polaris 1.x REST catalog.
                -- Without these the catalog registration call returns 401 and
                -- no Iceberg table is reachable from this job.
                'credential' = '{config.oauth2_credential}',
                'oauth2-server-uri' = '{config.oauth2_server_uri}',
                'scope' = 'PRINCIPAL_ROLE:ALL',
                'token-refresh-enabled' = 'true'
            )
            """
        )

    # The job owns its table definition, so drop first: CREATE TABLE IF NOT
    # EXISTS silently keeps an outdated definition. The drop is tolerant because
    # Polaris may refuse it for this principal; carry on with the existing table.
    logger.info("dropping any previous %s.orders_windowed definition", config.catalog_name)
    try:
        table_env.execute_sql(f"DROP TABLE IF EXISTS `{config.catalog_name}`.`sales`.`orders_windowed`")
    except Exception as exc:  # noqa: BLE001 - Polaris refusing the drop is expected
        logger.warning(
            "Polaris refused to drop the previous %s.orders_windowed definition (%s); "
            "keeping it and continuing with CREATE TABLE IF NOT EXISTS.",
            config.catalog_name,
            str(exc)[:200],
        )

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS `{config.catalog_name}`.`sales`.`orders_windowed` (
            `window_start`    TIMESTAMP(6),
            `window_end`      TIMESTAMP(6),
            `order_date`      DATE,
            `status`          STRING,
            `window_revenue`  DOUBLE,
            `window_orders`   BIGINT,
            `avg_order_value` DOUBLE,
            `source_tier`     STRING,
            PRIMARY KEY (`window_start`, `status`) NOT ENFORCED
        )
        -- No PARTITIONED BY (days(`window_start`)) clause. Flink 1.18's parser
        -- rejects a computed partitioning expression in a catalog CREATE TABLE
        -- ("SQL parse failed. Encountered "(" at line 13, column 29", the "(" of
        -- days(). Iceberg partitions on its own default here; if daily
        -- partitioning is needed it has to be applied to the catalog table, not
        -- declared in this statement.
        --
        -- No 'io-impl' here on purpose. Polaris INTERNAL catalogs own the
        -- FileIO: setting it per table is rejected outright
        --   BadRequestException: Cannot set property 'io-impl' to
        --   'org.apache.iceberg.aws.s3.S3FileIO' for this catalog.
        -- and GET /v1/config returns no s3.* properties either, so the client
        -- keeps Iceberg's HadoopFileIO and resolves the warehouse scheme
        -- through Hadoop. The image therefore ships hadoop-aws (see
        -- infra/docker/flink.Dockerfile); endpoint, credentials and
        -- path-style access come from the s3.* keys in flinkConfiguration.
        WITH (
            'format' = 'parquet',
            'write.upsert.enabled' = 'true',
            -- What makes the DROP above legal. Polaris issues
            -- DROP_TABLE_WITH_PURGE by default and this principal
            -- (catalog_admin) is not authorised for it:
            --   ForbiddenException: Principal 'root' with activated
            --   PrincipalRoles '[service_admin]' and activated grants via
            --   '[service_admin, catalog_admin]' is not authorized for op
            --   DROP_TABLE_WITH_PURGE
            -- so every restart after the first failed. With purge disabled the
            -- drop removes only the metadata, which is what a job recreating
            -- its own table wants; Parquet files stay in MinIO and the table
            -- that replaces them writes its own.
            'table.purge-on-drop' = 'false'
        )
        """
    )


def main() -> None:
    """Entry point executed by the Flink Kubernetes Operator (PythonDriver)."""
    config = JobConfig.from_env()
    logger.info(
        "Submitting %s | topic=%s offset=%s catalog=%s window=%dms allowed_lateness=%dms",
        JOB_NAME,
        config.topic,
        config.start_offset,
        config.catalog_uri,
        WINDOW_SIZE_MS,
        ALLOWED_LATENESS_MS,
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)
    env.get_config().set_auto_watermark_interval(1_000)

    windowed = build_source_and_windows(env, config)

    table_env = StreamTableEnvironment.create(env)
    # Iceberg `timestamp` has no zone; pin the session to UTC so window bounds
    # do not depend on the container clock.
    table_env.get_config().set("table.exec.timezone", "UTC")
    register_iceberg_catalog(table_env, config)
    # from_data_stream without a schema; the Schema goes to create_temporary_view
    # instead, because PyFlink 1.18 derives positional fields (f0..f7) and
    # rejects named columns.
    table_env.create_temporary_view(TEMP_VIEW, table_env.from_data_stream(windowed), window_schema())

    # Terminal submission; without execute() Flink reports no execute() calls.
    # The projection is positional (f0..f7) and CAST(f5 AS BIGINT) is required
    # because PyFlink maps a Python int to BigInteger.
    result = table_env.execute_sql(
        f"""
        INSERT INTO `{config.catalog_name}`.`sales`.`orders_windowed`
        SELECT
            CAST(f0 AS TIMESTAMP(6)),
            CAST(f1 AS TIMESTAMP(6)),
            f2,
            f3,
            f4,
            CAST(f5 AS BIGINT),
            f6,
            f7
        FROM {TEMP_VIEW}
        """
    )

    job_client = result.get_job_client()
    if job_client is None:
        raise RuntimeError("Flink accepted the plan but returned no JobClient: the job never started.")
    logger.info("JobID=%s running (uid=%s)", job_client.get_job_id(), JOB_UID)

    # A streaming job is unbounded and never completes, so nothing here may
    # block waiting for a result: the Flink Operator supervises the deployment.
    logger.info("%s submitted; the Operator owns supervision and checkpoints", JOB_NAME)


if __name__ == "__main__":
    main()
