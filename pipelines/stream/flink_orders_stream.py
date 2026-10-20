"""
flink_orders_stream.py - Apache Flink 1.18 speed-layer job for sales orders.

Submitted by the Flink Kubernetes Operator through
``infra/helm/s4-stream/templates/flink-deployments.yaml`` with the PyFlink
``PythonDriver``. Not a CLI, and no fallback: if PyFlink, the cluster or the
configuration is missing the job fails rather than running something else.

    Kafka (sales.orders.events) -> dead-letter -> event-time watermarks
      -> 4 s tumbling windows keyed by status
      -> upsert into Iceberg stream.sales.orders_windowed (Polaris, Parquet/MinIO)

PyFlink ships no DataStream binding for the Iceberg connector, so this uses the
documented "Mixing DataStream and Table" pattern: source, watermarks and
windowing on the DataStream API, then a temporary view the Table API writes.

Configuration comes exclusively from the environment declared in the
FlinkDeployment. A missing variable is a hard error, never a localhost default.
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

# There is deliberately NO `from pyflink.util import type_hints` here. PyFlink
# 1.18 has no `pyflink.util.type_hints` and no `pyflink.common.type_hints`
# either - both were verified inside the image:
#   >>> import pyflink.common.type_hints
#   ModuleNotFoundError: No module named 'pyflink.common.type_hints'
#   >>> import pyflink.datastream.functions as f
#   >>> [n for n in dir(f) if 'Collector' in n]
#   []
# so the import failed at module load, before any topology was built, and the
# JobManager died with only:
#   ImportError: cannot import name 'type_hints' from 'pyflink.util'
#   Python process exits with code: 1
# The window function below returns the rows it wants to emit; DataStream
# .process() passes no Collector to annotate.
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
    # OAuth2 credential for Polaris 1.x REST catalog.
    # Format: "client_id:client_secret". Injected via POLARIS_OAUTH2_CREDENTIAL
    # env var in the FlinkDeployment (s4-stream/templates/flink-deployments.yaml).
    oauth2_credential: str

    @property
    def oauth2_server_uri(self) -> str:
        """Polaris token endpoint, derived from the catalog URI.

        Iceberg must be told where to fetch the OAuth2 token. Without it the
        client calls GET /v1/config unauthenticated and Polaris answers 404
        "Unable to find warehouse <warehouse>" - even though the catalog exists
        with exactly that base location. The derivation keeps the catalog URI as
        the single source of truth instead of a second env var to keep in sync.
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
#:
#: A plain TUPLE, and not a dataclass, on purpose. Flink sends the accumulator
#: to the TaskManager with standard pickle, and the Beam runner executes the
#: operator module under the name __main__ - so a class defined in this module
#: is pickled by reference to __main__, where the TaskManager cannot find it:
#:
#:     _pickle.PicklingError: Can't pickle <class '__main__.RevenueAccumulator'>:
#:     attribute lookup RevenueAccumulator on __main__ failed
#:
#: The TaskManager then fails the task while INITIALIZING and the job restarts
#: in a loop, with "Not all required tasks are currently running" on the
#: checkpoints as the only symptom. A tuple of primitives pickles with no
#: module lookup at all, so it cannot fail that way.
#:
#: The indices are literal because a tuple is read positionally here; the
#: names below are the documentation.
#:   0 total_revenue   1 order_count   2 status   3 max_event_ms
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

    The contract is the one PyFlink 1.18 actually invokes, read from the
    image's own source rather than assumed:

        # pyflink/datastream/data_stream.py:641
        def process(self, func: ProcessFunction, output_type: TypeInformation = None) -> 'DataStream'

        # pyflink/fn_execution/datastream/process/operations.py, PROCESS branch
        process_element = user_defined_func.process_element
        results = process_element(value[2], ctx)

    So ``process_element`` takes (value, ctx) - TWO arguments, no Collector -
    and RETURNS the values to emit. ``ProcessWindowFunction.process(key, ctx,
    elements)`` is a different API on a different code path and is never called
    here, which is why every window silently produced nothing.

    The aggregate already ran the tumbling window, so this operator sees one
    element per window and needs no window bookkeeping of its own.
    """

    def process_element(self, value: Row, ctx: Any) -> list[Row]:
        status, revenue, orders, max_event_ms = value[0], value[1], value[2], value[3]
        window_start = window_start_ms(max_event_ms)
        row = build_orders_row(window_start, str(status), float(revenue), int(orders))
        return [Row(*orders_row_to_columns(row))]


class _AssignEventTime:
    """Timestamp assigner: the event time carried by the payload, never wall clock.

    A class, not a bare function, because PyFlink 1.18 reads
    ``timestamp_assigner.extract_timestamp`` in
    ``pyflink/datastream/data_stream.py:683`` and does not fall back to calling
    the object. Measured in-cluster:
      AttributeError: 'function' object has no attribute 'extract_timestamp'
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
        # Mandatory: without a deserialization schema KafkaSourceBuilder.build()
        # fails job submission with "Deserialization schema is required but not
        # provided". The value is the JSON payload decoded to str, which is
        # exactly what window_logic.decode_payload expects.
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermarks = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(MAX_OUT_OF_ORDERNESS_MS))
        .with_timestamp_assigner(_AssignEventTime())
        .with_idleness(Duration.of_millis(MAX_OUT_OF_ORDERNESS_MS * 4))
    )

    # PyFlink 1.18 has no POJO TypeInformation, so the parsed dataclass travels
    # pickled (the default when output_type is omitted). output_type/key_type are
    # left unset throughout for the same reason: the only Types factories that
    # accept multiple arguments are the DataType ones from pyflink.common, which
    # the DataStream API rejects.
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
        # takes a Time. Passing Time.milliseconds(...) here crashes the window
        # operator the first time a window is evaluated:
        #   window_operator.py:514  time = window.max_timestamp() + self.allowed_lateness
        #   TypeError: unsupported operand type(s) for +: 'int' and 'Time'
        .allowed_lateness(ALLOWED_LATENESS_MS)
        .aggregate(_RevenueAggregate())
        .process(
            _OrdersWindowFunction(),
            # Positional only: RowTypeInfo with named fields blows up in the JVM
            # (TupleTypeInfoBase NPE) on PyFlink 1.18. Column names are attached
            # when the stream is registered as a view, via an explicit Schema.
            Types.ROW(
                [
                    Types.SQL_TIMESTAMP(),  # window_start
                    Types.SQL_TIMESTAMP(),  # window_end
                    Types.SQL_DATE(),  # order_date
                    Types.STRING(),  # status
                    Types.DOUBLE(),  # window_revenue
                    # Types.INT(), not Types.BIG_INT().
                    #
                    # This is the actual root cause of the column-type failure,
                    # and it is in THIS line, not in the SQL. In PyFlink 1.18
                    # Types.BIG_INT() maps to BigIntTypeInfo, whose logical type
                    # is java.math.BigInteger. Declaring it here makes PyFlink
                    # convert the Python value into a BigInteger, from_data_stream
                    # re-derives the RowType from that, and the Table API sees
                    #   f5: RAW('java.math.BigInteger')
                    # which then fails BOTH ways:
                    #   * no cast  -> "Column types of query result and sink do
                    #     not match. Incompatible types for sink column
                    #     'window_orders' at position 5"
                    #   * with CAST(f5 AS BIGINT) -> "Cast function cannot
                    #     convert value of type RAW('java.math.BigInteger') to
                    #     type BIGINT" - the cast refuses to unwrap a RAW payload
                    #
                    # Types.INT() maps to IntegerTypeInfo -> SQL INTEGER, which
                    # the planner widens to BIGINT cleanly. A 4-second window
                    # cannot overflow a 32-bit count, and the sink column is
                    # still declared BIGINT in the Iceberg table.
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

    OBSERVED BEHAVIOUR: this schema is NOT applied. create_temporary_view()
    accepts it without error, but the resulting view keeps the generic
    positional RowType that from_data_stream() derived - f0..f7, with
    f0: TIMESTAMP(9) rather than the TIMESTAMP(6) declared here. The INSERT
    therefore has to project positionally (see main), and referencing
    window_start/order_date fails with 'Column not found in any table'.

    It is kept because it is the only written-down statement of the intended
    column order and types, and it costs nothing. If a future PyFlink honours
    it, the projection in main() should switch back to named columns.
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
    # Flink 1.18's parser rejects "CREATE CATALOG IF NOT EXISTS" with
    # ParseException: Encountered "NOT". The idempotency is therefore explicit:
    # ask for the registered catalogs and only issue the statement when ours is
    # missing. (CREATE TABLE IF NOT EXISTS below IS supported and stays.)
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

    # The job owns its table definition (see this function's docstring), so a
    # property change must actually reach the table. CREATE TABLE IF NOT EXISTS
    # silently KEEPS the existing definition, so adding 'io-impl' to the table
    # below had no effect at all and the writer kept using HadoopFileIO:
    #   RuntimeIOException: No FileSystem for scheme: s3
    # Dropping first makes the definition below authoritative on every start.
    # It is scoped to this one table, and the speed layer is its only writer.
    #
    # The drop is TOLERANT because Polaris refuses it for this principal:
    #   TableException: Could not execute DropTable in path `polaris`.`sales`.`orders_windowed`
    #   ForbiddenException: Principal 'root' ... is not authorized for op
    #   DROP_TABLE_WITH_PURGE
    # and the job died at startup, before a single row was written. The
    # 'table.purge-on-drop' = 'false' property below is what makes a drop
    # legal, but it is a property of the TABLE, so it only helps once the
    # table has been created with it - and a table left behind by a previous
    # incarnation may not have it.
    #
    # So: try to drop, and if Polaris says no, keep the existing definition and
    # carry on. The table is written by this job alone, so its definition
    # cannot drift between two writers.
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
    # from_data_stream WITHOUT a schema, and the Schema handed to
    # create_temporary_view instead. PyFlink 1.18 derives a generic RowTypeInfo
    # with positional fields (f0..f7), so passing named columns to
    # from_data_stream is rejected with
    #   ValidationException: Unable to find a field named 'window_start' ...
    #   Currently available fields are: [f0, f1, f2, f3, f4, f5, f6, f7]
    # Passing the Schema here names the columns positionally instead.
    table_env.create_temporary_view(TEMP_VIEW, table_env.from_data_stream(windowed), window_schema())

    # Terminal submission. Its total absence is what produced the original
    # "The application contains no execute() calls" failure.
    #
    # POSITIONAL references, deliberately. from_data_stream() derives a generic
    # RowType whose fields are f0..f7, and create_temporary_view(..., schema) does
    # NOT rename them - the view really has no window_start/order_date columns:
    #
    #   ValidationException: SQL validation failed. From line 4, column 13 to
    #   line 4, column 24: Column 'window_start' not found in any table
    #
    # So the projection addresses f0..f7, whose order and types the planner
    # reports are:
    #   f0 TIMESTAMP(9)  f1 TIMESTAMP(9)  f2 DATE  f3 STRING
    #   f4 DOUBLE        f5 RAW(java.math.BigInteger)  f6 DOUBLE  f7 STRING
    # and WINDOW_COLUMNS pins that order to the Iceberg table below.
    #
    # The CAST on f5 is required, not cosmetic: PyFlink maps a Python int to
    # BigInteger (arbitrary precision), so without it the write is rejected with
    #   ValidationException: Column types of query result and sink for
    #   'polaris.sales.orders_windowed' do not match.
    #   Cause: Incompatible types for sink column 'window_orders' at position 5.
    # Declaring Types.BIG_INT() in process() does not prevent this: that is the
    # upstream StreamRecord type, and from_data_stream re-derives the RowType
    # from the Python values.
    #
    # The TIMESTAMP(6) casts on f0/f1 are explicit for symmetry; the planner
    # would narrow them implicitly, but relying on that hides the width.
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
