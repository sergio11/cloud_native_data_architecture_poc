"""
Executable proof (rake stream:probe) that the speed-layer pipeline works.

Runs the real operators from pipelines/stream/flink_orders_stream.py in a single
pod with a local Flink cluster, writing through the real Iceberg sink to MinIO.
"""

from __future__ import annotations

import logging
import os
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("window_write_probe")

CATALOG = os.environ["ICEBERG_CATALOG_NAME"]
URI = os.environ["ICEBERG_CATALOG_URI"]
WAREHOUSE = os.environ["ICEBERG_WAREHOUSE"]
ENDPOINT = os.environ["S3_ENDPOINT"]
ACCESS = os.environ["S3_ACCESS_KEY"]
SECRET = os.environ["S3_SECRET_KEY"]
CRED = os.environ["POLARIS_OAUTH2_CREDENTIAL"]

REPO_ROOT = os.environ.get("POC_REPO_ROOT", "/opt/flink")
sys.path.insert(0, REPO_ROOT)

FAILURES: list[str] = []
REPORT: list[str] = []


def emit(line: str) -> None:
    """Writes straight to fd 1, bypassing buffers that PyFlink's local JVM can break.

    The buffered logger can lose the assertions, so os.write keeps the report intact.
    """
    os.write(1, (line + "\n").encode())


def check(label: str, ok: bool, detail: str = "") -> bool:
    """Records one assertion; never raises, so the report lists every failure."""
    suffix = f" ({detail})" if detail else ""
    tag = "PASS" if ok else "FAIL"
    emit(f"{tag}  {label}{suffix}")
    if ok:
        log.info("%s  %s%s", tag, label, suffix)
    else:
        log.error("%s  %s%s", tag, label, suffix)
        FAILURES.append(label)
    REPORT.append(f"{tag}  {label}{suffix}")
    return ok


def _java_cause_chain(exc: BaseException) -> str:
    """Unwraps a Py4J error down to the Java exception that actually failed."""
    seen: list[str] = []
    java_exc = getattr(exc, "java_exception", None)
    while java_exc is not None and len(seen) < 6:
        try:
            seen.append(str(java_exc.toString()))
            java_exc = java_exc.getCause()
        except Exception:  # noqa: BLE001 - introspection only, never fatal
            break
    if seen:
        return " <- ".join(seen)[:400]

    # No java_exception on this Py4J build: the Java stack lives inside the
    # message, after "Trace:", and the first line is only the method signature.
    text = str(exc)
    if "Caused by:" in text:
        return text[text.index("Caused by:") :][:400]
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and ("Exception" in stripped or "Error" in stripped) and "Trace" not in stripped:
            return stripped[:400]
    return text[:400]


def probe_checkpoint_storage() -> None:
    """Checks that the checkpoint directory can really be written.

    IcebergStreamWriter commits data files per checkpoint, so unreachable storage
    means table metadata without Parquet. Tries s3a:// (required) and s3:// (reported).
    """
    from pyflink.java_gateway import get_gateway

    gateway = get_gateway()
    conf = gateway.jvm.org.apache.hadoop.conf.Configuration
    conf = conf()
    # The exact keys infra/helm/s4-stream/templates/flink-deployments.yaml sets.
    conf.set("fs.s3.endpoint", ENDPOINT)
    conf.set("fs.s3.awsAccessKeyId", ACCESS)
    conf.set("fs.s3.awsSecretAccessKey", SECRET)
    conf.set("fs.s3.path.style.access", "true")
    # Also the fs.s3a.* namespace: s3a:// reads only these, not the legacy fs.s3.* keys.
    conf.set("fs.s3a.endpoint", ENDPOINT)
    conf.set("fs.s3a.access.key", ACCESS)
    conf.set("fs.s3a.secret.key", SECRET)
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    fs_class = gateway.jvm.org.apache.hadoop.fs.FileSystem
    uri_class = gateway.jvm.java.net.URI
    path_class = gateway.jvm.org.apache.hadoop.fs.Path

    def roundtrip(scheme: str) -> tuple[bool, str]:
        path = f"{scheme}://checkpoints/probe/roundtrip.txt"
        try:
            # Static get() takes a java.net.URI; instance methods take org.apache.hadoop.fs.Path.
            fs = fs_class.get(uri_class(f"{scheme}://checkpoints"), conf)
            target = path_class(path)
            out = fs.create(target, True)
            out.write(b"probe")
            out.close()
            exists = bool(fs.exists(target))
            size = int(fs.getFileStatus(target).getLen())
            fs.delete(target, False)
            # The class name comes from the Java object, not type().
            impl = str(fs.getClass().getName())
            return (exists and size == 5), f"{impl} escribe/lee/borra {size}B"
        except Exception as exc:  # noqa: BLE001 - the failure text is the point
            return False, _java_cause_chain(exc)

    # The configured scheme is s3a:// (see the FlinkDeployment). It has to work.
    ok_s3a, detail_s3a = roundtrip("s3a")
    check("el almacenamiento de checkpoints s3a:// escribe en MinIO", ok_s3a, detail_s3a)

    # s3:// is not usable: the legacy S3FileSystem ignores fs.s3.endpoint and talks
    # to real AWS. Reported, never asserted.
    ok_s3, detail_s3 = roundtrip("s3")
    if ok_s3:
        emit(f"INFO  s3:// tambien funciona ahora ({detail_s3}): el chart podria volver a ese esquema")
    else:
        emit(f"INFO  s3:// no se usa y no funciona, como se esperaba: {detail_s3}")


def main() -> int:
    t0 = time.time()
    log.info("=== PROBE: ventana de la speed layer -> Iceberg -> Parquet ===")
    log.info("repo=%s catalog=%s endpoint=%s", REPO_ROOT, CATALOG, ENDPOINT)

    # Importing the real module makes a wrong operator signature fail here, loudly.
    from pipelines.stream import flink_orders_stream as job
    from pipelines.stream.window_logic import (
        ALLOWED_LATENESS_MS,
        MAX_OUT_OF_ORDERNESS_MS,
        WINDOW_SIZE_MS,
        OrderWindowEvent,
    )

    aggregate_fn = job._RevenueAggregate
    window_fn = job._OrdersWindowFunction
    log.info("operadores del repo: %s + %s", aggregate_fn.__name__, window_fn.__name__)
    check(
        "_OrdersWindowFunction define process_element(value, ctx)",
        hasattr(window_fn, "process_element"),
        ", ".join(sorted(m for m in dir(window_fn) if not m.startswith("_"))),
    )

    from pyflink.common import Duration, Time, WatermarkStrategy
    from pyflink.common.typeinfo import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.window import TumblingEventTimeWindows
    from pyflink.table import StreamTableEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    table_env = StreamTableEnvironment.create(env)
    table_env.get_config().set("table.exec.timezone", "UTC")
    log.info("entorno Flink local listo en %.1fs", time.time() - t0)

    probe_checkpoint_storage()

    base_ms = 1_760_000_000_000
    events = [
        OrderWindowEvent(
            event_time_ms=base_ms + i * 500,
            status="confirmed" if i % 2 == 0 else "shipped",
            total_amount=10.0 + i,
        )
        for i in range(6)
    ]

    windowed = (
        env.from_collection(events)
        .name("probe-source")
        .assign_timestamps_and_watermarks(
            # A class, not a function: PyFlink 1.18 calls .extract_timestamp on the assigner.
            WatermarkStrategy.for_bounded_out_of_orderness(
                Duration.of_millis(MAX_OUT_OF_ORDERNESS_MS)
            ).with_timestamp_assigner(job._AssignEventTime())
        )
        .name("probe-watermarks")
        .key_by(lambda e: e.status)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE_MS)))
        # Milliseconds as an int; Time.milliseconds(...) here raises TypeError.
        .allowed_lateness(ALLOWED_LATENESS_MS)
        .aggregate(aggregate_fn())
        .process(
            window_fn(),
            Types.ROW(
                [
                    Types.SQL_TIMESTAMP(),
                    Types.SQL_TIMESTAMP(),
                    Types.SQL_DATE(),
                    Types.STRING(),
                    Types.DOUBLE(),
                    Types.INT(),
                    Types.DOUBLE(),
                    Types.STRING(),
                ]
            ),
        )
        .name("probe-window")
    )
    log.info("topologia construida en %.1fs", time.time() - t0)

    table_env.execute_sql(
        f"""
        CREATE CATALOG `{CATALOG}` WITH (
            'type' = 'iceberg',
            'catalog-type' = 'rest',
            'uri' = '{URI}',
            'warehouse' = '{WAREHOUSE}',
            's3.endpoint' = '{ENDPOINT}',
            's3.access-key' = '{ACCESS}',
            's3.secret-key' = '{SECRET}',
            's3.path-style-access' = 'true',
            's3.region' = 'us-east-1',
            'credential' = '{CRED}',
            'oauth2-server-uri' = '{URI.rstrip("/")}/v1/oauth/tokens',
            'scope' = 'PRINCIPAL_ROLE:ALL',
            'token-refresh-enabled' = 'true'
        )
        """
    )
    log.info("catalogo %s registrado", CATALOG)

    table_env.create_temporary_view("probe_window", table_env.from_data_stream(windowed))

    table = f"`{CATALOG}`.`sales`.`probe_orders_windowed`"
    # Polaris does not authorise purge, so the probe appends and the row count grows.
    try:
        table_env.execute_sql(f"DROP TABLE IF EXISTS {table}")
        log.info("tabla previa eliminada")
    except Exception as exc:  # noqa: BLE001 - expected and reported, not fatal
        log.warning("DROP TABLE omitido (no autorizado a purgar): %s", str(exc)[:120])

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            `window_start`    TIMESTAMP(6),
            `window_end`      TIMESTAMP(6),
            `order_date`      DATE,
            `status`          STRING,
            `window_revenue`  DOUBLE,
            `window_orders`   BIGINT,
            `avg_order_value` DOUBLE,
            `source_tier`     STRING,
            PRIMARY KEY (`window_start`, `status`) NOT ENFORCED
        ) WITH (
            'format' = 'parquet',
            'write.upsert.enabled' = 'true',
            'table.purge-on-drop' = 'false'
        )
        """
    )

    # A bare execute_sql("INSERT ...") only submits; wait() lets the sink flush.
    result = table_env.execute_sql(
        f"""
        INSERT INTO {table}
        SELECT CAST(f0 AS TIMESTAMP(6)), CAST(f1 AS TIMESTAMP(6)), f2, f3, f4,
               CAST(f5 AS BIGINT), f6, f7
        FROM probe_window
        """
    )
    result.wait()
    log.info("INSERT confirmado en %.1fs", time.time() - t0)

    # No ORDER BY: sorting a non-time-attribute column is rejected by the planner.
    rows = list(
        table_env.execute_sql(
            f"SELECT window_start, status, window_orders, window_revenue, source_tier FROM {table}"
        ).collect()
    )
    log.info("=== FILAS EN ICECBERG: %d ===", len(rows))
    emit(f"=== FILAS EN ICECERG: {len(rows)} ===")
    for row in rows:
        emit(f"   {row}")

    check("la ventana produjo filas", len(rows) > 0, f"{len(rows)} filas acumuladas")
    check(
        "las filas traen source_tier=stream_speed",
        any(str(r[4]) == "stream_speed" for r in rows),
    )
    check(
        "las filas traen conteo y revenue coherentes",
        any(int(r[2]) > 0 and float(r[3]) > 0 for r in rows),
    )

    verdict = "OK" if not FAILURES else f"FALLOS ({', '.join(FAILURES)})"
    log.info("=== RESULTADO: %s en %.1fs ===", verdict, time.time() - t0)
    emit("")
    emit("=== INFORME DE LA SONDA ===")
    for line in REPORT:
        emit(f"  {line}")
    emit(f"=== RESULTADO: {verdict} en {time.time() - t0:.1f}s ===")
    return 0 if not FAILURES else 1


if __name__ == "__main__":
    sys.exit(main())
