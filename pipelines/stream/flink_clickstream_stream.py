"""
flink_clickstream_stream.py - Apache Flink 1.18 speed-layer job for clickstream.

Kafka (marketing.clickstream.raw) -> event-time watermarks -> 4 s tumbling
windows keyed by event_type -> event_count and distinct sessions per window ->
upsert into Iceberg stream.marketing.clickstream_windowed.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

# Make the repo root importable for the sibling pure module.
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
    ProcessWindowFunction,
)
from pyflink.datastream.window import TumblingEventTimeWindows  # noqa: E402
from pyflink.table import StreamTableEnvironment  # noqa: E402

from pipelines.stream.window_logic import (  # noqa: E402
    ALLOWED_LATENESS_MS,
    MAX_OUT_OF_ORDERNESS_MS,
    WINDOW_SIZE_MS,
    ClickstreamWindowEvent,
    build_clickstream_row,
    clickstream_row_to_columns,
    parse_clickstream_event,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("flink_clickstream_stream")

JOB_NAME = "flink-clickstream-speed-layer"
TEMP_VIEW = "clickstream_speed_window"


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

        Without it Iceberg calls GET /v1/config unauthenticated and Polaris
        answers 404 "Unable to find warehouse".
        """
        return f"{self.catalog_uri.rstrip('/')}/v1/oauth/tokens"

    @classmethod
    def from_env(cls) -> JobConfig:
        """Builds the configuration, raising ConfigurationError if incomplete."""
        start_offset = os.getenv("KAFKA_START_OFFSET", "latest").strip().lower()
        if start_offset not in ("earliest", "latest"):
            raise ConfigurationError(
                f"KAFKA_START_OFFSET must be 'earliest' or 'latest', got {start_offset!r}. "
                f"'earliest' is the RF-10 Kappa full-replay mode."
            )
        return cls(
            bootstrap_servers=_required("KAFKA_BOOTSTRAP_SERVERS"),
            topic=os.getenv("KAFKA_CLICKSTREAM_TOPIC", "marketing.clickstream.raw").strip()
            or "marketing.clickstream.raw",
            group_id=os.getenv("KAFKA_CLICKSTREAM_GROUP_ID", "flink-clickstream-speed-group").strip()
            or "flink-clickstream-speed-group",
            start_offset=start_offset,
            catalog_name=os.getenv("ICEBERG_CATALOG_NAME", "polaris").strip() or "polaris",
            catalog_uri=_required("ICEBERG_CATALOG_URI"),
            warehouse=_required("ICEBERG_WAREHOUSE"),
            s3_endpoint=_required("S3_ENDPOINT"),
            s3_access_key=_required("S3_ACCESS_KEY"),
            s3_secret_key=_required("S3_SECRET_KEY"),
            oauth2_credential=_required("POLARIS_OAUTH2_CREDENTIAL"),
        )


class _ParseClickstreamEvent(FlatMapFunction):
    """Parses Kafka payloads; records without session or event time are dropped."""

    def flat_map(self, value: str) -> list[ClickstreamWindowEvent]:
        event = parse_clickstream_event(value)
        return [] if event is None else [event]


@dataclass
class ClickstreamAccumulator:
    """Event count and the set of distinct sessions seen in one window."""

    events: int = 0
    sessions: set[str] = field(default_factory=set)


class _ClickstreamAggregate(AggregateFunction):
    """Incremental COUNT(*) and COUNT(DISTINCT session_id) per window."""

    def create_accumulator(self) -> ClickstreamAccumulator:
        return ClickstreamAccumulator()

    def add(self, value: ClickstreamWindowEvent, accumulator: ClickstreamAccumulator) -> ClickstreamAccumulator:
        accumulator.events += 1
        accumulator.sessions.add(str(value.session_id))
        return accumulator

    def get_result(self, accumulator: ClickstreamAccumulator) -> Row:
        return Row(int(accumulator.events), int(len(accumulator.sessions)))

    def merge(self, acc_a: ClickstreamAccumulator, acc_b: ClickstreamAccumulator) -> ClickstreamAccumulator:
        return ClickstreamAccumulator(events=acc_a.events + acc_b.events, sessions=acc_a.sessions | acc_b.sessions)


class _ClickstreamWindowFunction(ProcessWindowFunction):
    """Converts a windowed aggregate into a published speed-layer row."""

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable[Row],
    ) -> Iterable[Row]:
        aggregates = list(elements)
        if not aggregates:
            return []
        aggregate = aggregates[0]
        row = build_clickstream_row(
            context.window().start,
            key,
            int(aggregate[0]),
            int(aggregate[1]),
        )
        return [Row(*clickstream_row_to_columns(row))]


def _assign_event_time(event: ClickstreamWindowEvent, record_timestamp: int) -> int:
    """Timestamp assigner: the event time carried by the payload, never wall clock."""
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
        .with_timestamp_assigner(_assign_event_time)
        .with_idleness(Duration.of_millis(MAX_OUT_OF_ORDERNESS_MS * 4))
    )

    # No POJO TypeInformation exists in PyFlink 1.18, so the parsed dataclass
    # travels pickled; output_type/key_type stay unset for the same reason.
    events = (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "clickstream-kafka-source")
        .name("clickstream-kafka-source")
        .uid("clickstream-kafka-source")
        .flat_map(_ParseClickstreamEvent())
        .name("parse-clickstream-events")
        .uid("parse-clickstream-events")
        .assign_timestamps_and_watermarks(watermarks)
        .name("clickstream-event-time")
        .uid("clickstream-event-time")
    )

    return (
        events.key_by(lambda event: event.event_type, key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE_MS)))
        .allowed_lateness(Time.milliseconds(ALLOWED_LATENESS_MS))
        .aggregate(_ClickstreamAggregate())
        .process(
            _ClickstreamWindowFunction(),
            # Positional only; names come from window_schema() at registration.
            Types.ROW(
                [
                    Types.SQL_TIMESTAMP(),  # window_start
                    Types.SQL_TIMESTAMP(),  # window_end
                    Types.SQL_DATE(),  # order_date
                    Types.STRING(),  # event_type
                    Types.BIG_INT(),  # event_count
                    Types.BIG_INT(),  # unique_sessions
                    Types.STRING(),  # source_tier
                ]
            ),
        )
        .name("clickstream-tumbling-window")
        .uid("clickstream-tumbling-window")
    )


#: Column names for the windowed stream, matching the Types.ROW above and the
#: Iceberg table definition.
WINDOW_COLUMNS = (
    "window_start",
    "window_end",
    "order_date",
    "event_type",
    "event_count",
    "unique_sessions",
    "source_tier",
)


def window_schema() -> Any:
    """Builds the Table schema that names the windowed stream's columns."""
    from pyflink.table import DataTypes, Schema

    types = [
        DataTypes.TIMESTAMP(6),
        DataTypes.TIMESTAMP(6),
        DataTypes.DATE(),
        DataTypes.STRING(),
        DataTypes.BIGINT(),
        DataTypes.BIGINT(),
        DataTypes.STRING(),
    ]
    builder = Schema.new_builder()
    for name, data_type in zip(WINDOW_COLUMNS, types, strict=True):
        builder.column(name, data_type)
    return builder.build()


def register_iceberg_catalog(table_env: StreamTableEnvironment, config: JobConfig) -> None:
    """Registers the Polaris REST catalog and the clickstream Iceberg table.

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
    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS `{config.catalog_name}`.`marketing`.`clickstream_windowed` (
            `window_start`   TIMESTAMP(6),
            `window_end`     TIMESTAMP(6),
            `order_date`     DATE,
            `event_type`     STRING,
            `event_count`    BIGINT,
            `unique_sessions` BIGINT,
            `source_tier`    STRING,
            PRIMARY KEY (`window_start`, `event_type`) NOT ENFORCED
        )
        -- No PARTITIONED BY (days(`window_start`)) clause: Flink 1.18's parser
        -- rejects a computed partitioning expression in a catalog CREATE TABLE
        -- ("Encountered "("" at the days() call). Iceberg uses its own default.
        WITH ('format' = 'parquet', 'write.upsert.enabled' = 'true')
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
    table_env.get_config().set("table.exec.timezone", "UTC")
    register_iceberg_catalog(table_env, config)
    # from_data_stream without a schema; the Schema goes to create_temporary_view
    # because PyFlink 1.18 derives positional fields (f0..f7).
    table_env.create_temporary_view(TEMP_VIEW, table_env.from_data_stream(windowed), window_schema())

    result = table_env.execute_sql(
        f"INSERT INTO `{config.catalog_name}`.`marketing`.`clickstream_windowed` SELECT * FROM {TEMP_VIEW}"
    )

    job_client = result.get_job_client()
    if job_client is None:
        raise RuntimeError("Flink accepted the plan but returned no JobClient: the job never started.")
    logger.info("JobID=%s running", job_client.get_job_id())
    # Unbounded streaming job: never block waiting for a result. The Flink
    # Operator owns supervision, checkpointing and restarts.
    logger.info("%s submitted; the Operator owns supervision and checkpoints", JOB_NAME)


if __name__ == "__main__":
    main()
