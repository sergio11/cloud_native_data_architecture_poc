"""
run_quality_gate.py - Quality gate runner for data pipelines.

Runs Great Expectations validation suites against Spark/Pandas DataFrames
or Trino tables. Blocks pipeline progression if quality threshold not met.

Usage:
  python run_quality_gate.py \\
    --suite quality/expectations/silver_orders_suite.json \\
    --table silver.sales.orders \\
    --threshold 0.95 \\
    --marquez-url http://marquez:5000 \\
    --webhook-url https://hooks.example.com/alerts

Exit codes:
  0: Quality gate PASSED
  1: Quality gate FAILED (blocks pipeline)
  2: Execution error (infrastructure issue)
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Any, Optional

import click
import pandas as pd
import requests
from rich.console import Console
from rich.table import Table
from rich import box
from rich.panel import Panel
from rich.text import Text

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("quality_gate")

console = Console()


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ValidationResult:
    """Immutable result object produced by a quality gate run.

    Attributes:
        suite_name: Name of the Great Expectations suite that was executed.
        table_name: Fully-qualified table name that was validated.
        pass_rate: Fraction of expectations that passed (0.0 – 1.0).
        passed: True when pass_rate >= configured threshold.
        total_expectations: Number of expectations evaluated.
        failed_expectations: List of expectation details that did not pass.
        run_id: UUID v4 uniquely identifying this validation run.
        run_at: UTC timestamp when the run started.
        duration_seconds: Wall-clock seconds taken for the full validation.
    """

    suite_name: str
    table_name: str
    pass_rate: float
    passed: bool
    total_expectations: int
    failed_expectations: list[dict]
    run_id: str
    run_at: datetime
    duration_seconds: float

    def to_dict(self) -> dict:
        """Serialise to a JSON-safe dictionary."""
        d = asdict(self)
        d["run_at"] = self.run_at.isoformat()
        return d


# ---------------------------------------------------------------------------
# Trino reader
# ---------------------------------------------------------------------------


class TrinoTableReader:
    """Reads data from a Trino cluster into a pandas DataFrame.

    Args:
        host: Trino coordinator hostname or IP.
        port: Trino coordinator port (default 8080).
        user: Trino user for authentication.
        catalog: Default Trino catalog.
    """

    def __init__(
        self,
        host: str,
        port: int = 8080,
        user: str = "data-platform",
        catalog: str = "hive",
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._catalog = catalog
        self._conn = self._build_connection()

    def _build_connection(self):  # type: ignore[return]
        """Create and return a trino DBAPI connection.

        Returns:
            trino.dbapi.Connection: active connection to the Trino cluster.

        Raises:
            RuntimeError: if the trino package is not installed.
        """
        try:
            import trino  # type: ignore[import]

            conn = trino.dbapi.connect(
                host=self._host,
                port=self._port,
                user=self._user,
                catalog=self._catalog,
            )
            logger.info(
                "Trino connection established: %s:%d (catalog=%s)",
                self._host,
                self._port,
                self._catalog,
            )
            return conn
        except ImportError as exc:
            raise RuntimeError(
                "trino package is required: pip install trino"
            ) from exc

    def read_sample(self, table: str, sample_size: int = 100_000) -> pd.DataFrame:
        """Read up to *sample_size* rows from *table* as a pandas DataFrame.

        Args:
            table: Fully-qualified table name, e.g. ``silver.sales.orders``.
            sample_size: Maximum number of rows to read (TABLESAMPLE may be
                used by Trino internally). Defaults to 100 000.

        Returns:
            pd.DataFrame: Sample of the table contents.

        Raises:
            RuntimeError: if the query fails.
        """
        query = f"SELECT * FROM {table} LIMIT {sample_size}"
        logger.info("Executing Trino query: %s", query)
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            logger.info(
                "Fetched %d rows from %s (sample_size=%d)",
                len(df),
                table,
                sample_size,
            )
            return df
        except Exception as exc:
            raise RuntimeError(
                f"Failed to read sample from Trino table {table!r}: {exc}"
            ) from exc

    def get_row_count(self, table: str) -> int:
        """Execute a fast COUNT(*) query and return the result.

        Args:
            table: Fully-qualified table name.

        Returns:
            int: Total number of rows in the table.

        Raises:
            RuntimeError: if the query fails.
        """
        query = f"SELECT COUNT(*) FROM {table}"
        logger.info("Counting rows: %s", query)
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            count = int(row[0])
            logger.info("Row count for %s: %d", table, count)
            return count
        except Exception as exc:
            raise RuntimeError(
                f"Failed to count rows for Trino table {table!r}: {exc}"
            ) from exc


# ---------------------------------------------------------------------------
# GE Validator
# ---------------------------------------------------------------------------


class GEValidator:
    """Runs Great Expectations validations against a pandas DataFrame.

    This validator implements a lightweight, dependency-free execution engine
    that interprets GE-format expectation suite JSON files without requiring
    the full ``great_expectations`` package to be installed (though it is
    compatible with suites written for GE 0.18.x).

    Args:
        suite_path: Absolute or relative path to the expectation suite JSON.

    Raises:
        FileNotFoundError: if *suite_path* does not exist.
        ValueError: if the JSON cannot be parsed as a valid suite.
    """

    def __init__(self, suite_path: str) -> None:
        path = Path(suite_path)
        if not path.exists():
            raise FileNotFoundError(f"Suite file not found: {suite_path}")
        with path.open("r", encoding="utf-8") as fh:
            raw = json.load(fh)
        if "expectations" not in raw:
            raise ValueError(
                f"Invalid suite file (missing 'expectations' key): {suite_path}"
            )
        self._suite = raw
        self._suite_name: str = raw.get("expectation_suite_name", path.stem)
        self._expectations: list[dict] = raw["expectations"]
        logger.info(
            "Loaded suite '%s' with %d expectations from %s",
            self._suite_name,
            len(self._expectations),
            suite_path,
        )

    @property
    def suite_name(self) -> str:
        """Return the name of the loaded expectation suite."""
        return self._suite_name

    # ------------------------------------------------------------------
    # Internal expectation handlers
    # ------------------------------------------------------------------

    def _expect_column_values_to_not_be_null(
        self, df: pd.DataFrame, kwargs: dict
    ) -> dict:
        col = kwargs["column"]
        mostly = kwargs.get("mostly", 1.0)
        if col not in df.columns:
            return {"success": False, "reason": f"Column '{col}' not found"}
        null_count = int(df[col].isna().sum())
        total = len(df)
        pass_rate = 1.0 - (null_count / total) if total > 0 else 1.0
        success = pass_rate >= mostly
        return {
            "success": success,
            "null_count": null_count,
            "total": total,
            "pass_rate": round(pass_rate, 4),
        }

    def _expect_column_values_to_be_unique(
        self, df: pd.DataFrame, kwargs: dict
    ) -> dict:
        col = kwargs["column"]
        mostly = kwargs.get("mostly", 1.0)
        if col not in df.columns:
            return {"success": False, "reason": f"Column '{col}' not found"}
        total = len(df)
        duplicated = int(df[col].duplicated(keep=False).sum())
        unique_rate = 1.0 - (duplicated / total) if total > 0 else 1.0
        success = unique_rate >= mostly
        return {
            "success": success,
            "duplicate_count": duplicated,
            "total": total,
            "unique_rate": round(unique_rate, 4),
        }

    def _expect_column_values_to_be_between(
        self, df: pd.DataFrame, kwargs: dict
    ) -> dict:
        col = kwargs["column"]
        min_val = kwargs.get("min_value")
        max_val = kwargs.get("max_value")
        mostly = kwargs.get("mostly", 1.0)
        if col not in df.columns:
            return {"success": False, "reason": f"Column '{col}' not found"}
        series = pd.to_numeric(df[col], errors="coerce")
        mask = pd.Series([True] * len(series), index=series.index)
        if min_val is not None:
            mask &= series >= float(min_val)
        if max_val is not None:
            mask &= series <= float(max_val)
        total = len(df)
        passing = int(mask.sum())
        pass_rate = passing / total if total > 0 else 1.0
        success = pass_rate >= mostly
        return {
            "success": success,
            "passing_rows": passing,
            "total": total,
            "pass_rate": round(pass_rate, 4),
        }

    def _expect_column_values_to_match_regex(
        self, df: pd.DataFrame, kwargs: dict
    ) -> dict:
        col = kwargs["column"]
        regex = kwargs["regex"]
        mostly = kwargs.get("mostly", 1.0)
        if col not in df.columns:
            return {"success": False, "reason": f"Column '{col}' not found"}
        pattern = re.compile(regex)
        series = df[col].astype(str)
        matches = series.apply(lambda v: bool(pattern.match(v)))
        total = len(df)
        passing = int(matches.sum())
        pass_rate = passing / total if total > 0 else 1.0
        success = pass_rate >= mostly
        return {
            "success": success,
            "passing_rows": passing,
            "total": total,
            "pass_rate": round(pass_rate, 4),
        }

    def _expect_column_values_to_be_in_set(
        self, df: pd.DataFrame, kwargs: dict
    ) -> dict:
        col = kwargs["column"]
        value_set = set(kwargs.get("value_set", []))
        mostly = kwargs.get("mostly", 1.0)
        if col not in df.columns:
            return {"success": False, "reason": f"Column '{col}' not found"}
        total = len(df)
        passing = int(df[col].isin(value_set).sum())
        pass_rate = passing / total if total > 0 else 1.0
        success = pass_rate >= mostly
        return {
            "success": success,
            "passing_rows": passing,
            "total": total,
            "pass_rate": round(pass_rate, 4),
        }

    def _expect_column_values_to_be_of_type(
        self, df: pd.DataFrame, kwargs: dict
    ) -> dict:
        col = kwargs["column"]
        expected_type = kwargs["type_"].upper()
        if col not in df.columns:
            return {"success": False, "reason": f"Column '{col}' not found"}
        dtype = str(df[col].dtype)
        type_map = {
            "INTEGER": ["int32", "int64", "Int32", "Int64"],
            "DOUBLE": ["float32", "float64", "Float32", "Float64"],
            "STRING": ["object", "string"],
            "DATE": ["object", "datetime64[ns]", "datetime64[us]"],
            "BOOLEAN": ["bool"],
        }
        accepted = type_map.get(expected_type, [expected_type.lower()])
        success = any(dtype.startswith(a) for a in accepted)
        return {
            "success": success,
            "actual_dtype": dtype,
            "expected_type": expected_type,
        }

    def _expect_table_row_count_to_be_between(
        self, df: pd.DataFrame, kwargs: dict
    ) -> dict:
        min_val = kwargs.get("min_value")
        max_val = kwargs.get("max_value")
        total = len(df)
        success = True
        if min_val is not None and total < int(min_val):
            success = False
        if max_val is not None and total > int(max_val):
            success = False
        return {"success": success, "row_count": total, "min_value": min_val, "max_value": max_val}

    def _expect_column_value_lengths_to_be_between(
        self, df: pd.DataFrame, kwargs: dict
    ) -> dict:
        col = kwargs["column"]
        min_val = kwargs.get("min_value")
        max_val = kwargs.get("max_value")
        mostly = kwargs.get("mostly", 1.0)
        if col not in df.columns:
            return {"success": False, "reason": f"Column '{col}' not found"}
        lengths = df[col].astype(str).str.len()
        mask = pd.Series([True] * len(lengths), index=lengths.index)
        if min_val is not None:
            mask &= lengths >= int(min_val)
        if max_val is not None:
            mask &= lengths <= int(max_val)
        total = len(df)
        passing = int(mask.sum())
        pass_rate = passing / total if total > 0 else 1.0
        success = pass_rate >= mostly
        return {
            "success": success,
            "passing_rows": passing,
            "total": total,
            "pass_rate": round(pass_rate, 4),
        }

    def _expect_table_columns_to_match_ordered_list(
        self, df: pd.DataFrame, kwargs: dict
    ) -> dict:
        expected = kwargs.get("column_list", [])
        actual = list(df.columns)
        success = actual == expected
        return {
            "success": success,
            "expected_columns": expected,
            "actual_columns": actual,
        }

    # ------------------------------------------------------------------
    # Dispatcher
    # ------------------------------------------------------------------

    _HANDLERS: dict[str, str] = {
        "expect_column_values_to_not_be_null": "_expect_column_values_to_not_be_null",
        "expect_column_values_to_be_unique": "_expect_column_values_to_be_unique",
        "expect_column_values_to_be_between": "_expect_column_values_to_be_between",
        "expect_column_values_to_match_regex": "_expect_column_values_to_match_regex",
        "expect_column_values_to_be_in_set": "_expect_column_values_to_be_in_set",
        "expect_column_values_to_be_of_type": "_expect_column_values_to_be_of_type",
        "expect_table_row_count_to_be_between": "_expect_table_row_count_to_be_between",
        "expect_column_value_lengths_to_be_between": "_expect_column_value_lengths_to_be_between",
        "expect_table_columns_to_match_ordered_list": "_expect_table_columns_to_match_ordered_list",
    }

    def validate(self, df: pd.DataFrame) -> list[dict]:
        """Run all expectations in the suite against *df*.

        Args:
            df: pandas DataFrame to validate.

        Returns:
            List of per-expectation result dicts, each containing at minimum
            ``expectation_type``, ``kwargs``, ``meta``, and ``result``
            (including a ``success`` bool).
        """
        results: list[dict] = []
        for exp in self._expectations:
            exp_type: str = exp["expectation_type"]
            kwargs: dict = exp.get("kwargs", {})
            meta: dict = exp.get("meta", {})
            handler_name = self._HANDLERS.get(exp_type)
            if handler_name is None:
                logger.warning("No handler for expectation type: %s — skipping", exp_type)
                result_detail: dict = {
                    "success": None,
                    "reason": f"Unsupported expectation type: {exp_type}",
                }
            else:
                try:
                    handler = getattr(self, handler_name)
                    result_detail = handler(df, kwargs)
                except Exception as exc:
                    logger.error(
                        "Error running %s (kwargs=%s): %s", exp_type, kwargs, exc
                    )
                    result_detail = {"success": False, "reason": str(exc)}
            results.append(
                {
                    "expectation_type": exp_type,
                    "kwargs": kwargs,
                    "meta": meta,
                    "result": result_detail,
                }
            )
        return results

    def compute_pass_rate(self, results: list[dict]) -> float:
        """Compute the fraction of expectations that passed.

        Expectations with ``success=None`` (unsupported types) are excluded
        from both numerator and denominator.

        Args:
            results: Output of :meth:`validate`.

        Returns:
            float: Pass rate in [0.0, 1.0]. Returns 1.0 for an empty suite.
        """
        evaluated = [r for r in results if r["result"].get("success") is not None]
        if not evaluated:
            return 1.0
        passed = sum(1 for r in evaluated if r["result"]["success"] is True)
        return passed / len(evaluated)

    def format_failures(self, results: list[dict]) -> list[dict]:
        """Extract failed (or errored) expectations with diagnostic details.

        Args:
            results: Output of :meth:`validate`.

        Returns:
            List of dicts for expectations where ``success`` is False.
        """
        failures = []
        for r in results:
            if r["result"].get("success") is False:
                failures.append(
                    {
                        "expectation_type": r["expectation_type"],
                        "kwargs": r["kwargs"],
                        "result": r["result"],
                        "notes": r["meta"].get("notes", ""),
                        "severity": r["meta"].get("severity", "unknown"),
                    }
                )
        return failures


# ---------------------------------------------------------------------------
# Marquez publisher
# ---------------------------------------------------------------------------


class MarquezPublisher:
    """Publishes quality gate results to a Marquez lineage server.

    Marquez is an open-source metadata server that tracks data lineage and
    dataset quality metrics. Results are posted to the ``/api/v1/runs``
    endpoint with quality metrics attached as facets.
    """

    _TIMEOUT_SECONDS = 10

    def publish_quality_run(
        self, result: ValidationResult, marquez_url: str
    ) -> None:
        """POST quality metrics to Marquez.

        Failures to reach Marquez are logged and silently swallowed so they
        never block the pipeline gate decision.

        Args:
            result: The :class:`ValidationResult` from a validation run.
            marquez_url: Base URL of the Marquez API, e.g. ``http://marquez:5000``.
        """
        url = f"{marquez_url.rstrip('/')}/api/v1/lineage"
        payload = {
            "eventType": "COMPLETE",
            "eventTime": result.run_at.isoformat() + "Z",
            "run": {
                "runId": result.run_id,
                "facets": {
                    "dataQuality": {
                        "_producer": "run_quality_gate",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsFacet.json",
                        "suiteName": result.suite_name,
                        "passRate": result.pass_rate,
                        "passed": result.passed,
                        "totalExpectations": result.total_expectations,
                        "failedExpectations": len(result.failed_expectations),
                        "durationSeconds": result.duration_seconds,
                    }
                },
            },
            "job": {
                "namespace": "data-platform",
                "name": f"quality_gate.{result.suite_name}",
            },
            "inputs": [
                {
                    "namespace": "trino",
                    "name": result.table_name,
                    "facets": {
                        "dataQualityAssertions": {
                            "_producer": "run_quality_gate",
                            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityAssertionsFacet.json",
                            "assertions": [
                                {
                                    "assertion": f["expectation_type"],
                                    "success": False,
                                    "column": f["kwargs"].get("column"),
                                }
                                for f in result.failed_expectations
                            ],
                        }
                    },
                }
            ],
            "outputs": [],
            "producer": "run_quality_gate",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
        }
        try:
            resp = requests.post(
                url, json=payload, timeout=self._TIMEOUT_SECONDS
            )
            resp.raise_for_status()
            logger.info(
                "Published quality run %s to Marquez (HTTP %d)",
                result.run_id,
                resp.status_code,
            )
        except requests.exceptions.ConnectionError:
            logger.warning(
                "Marquez unavailable at %s — skipping lineage publish", marquez_url
            )
        except requests.exceptions.Timeout:
            logger.warning(
                "Marquez request timed out after %ds — skipping lineage publish",
                self._TIMEOUT_SECONDS,
            )
        except requests.exceptions.HTTPError as exc:
            logger.warning(
                "Marquez returned HTTP error: %s — skipping lineage publish", exc
            )
        except Exception as exc:
            logger.warning(
                "Unexpected error publishing to Marquez: %s — skipping", exc
            )


# ---------------------------------------------------------------------------
# Webhook alerter
# ---------------------------------------------------------------------------


class WebhookAlerter:
    """Sends Slack-compatible webhook alerts for quality gate results.

    Retries up to 3 times with a 5-second back-off on transient failures.
    """

    _MAX_RETRIES = 3
    _BACKOFF_SECONDS = 5
    _TIMEOUT_SECONDS = 10

    def send_alert(self, result: ValidationResult, webhook_url: str, threshold: float = 0.95) -> None:
        """POST a Slack-compatible alert payload to *webhook_url*.

        Sent regardless of pass/fail so teams can monitor both outcomes.
        Color coding: green = passed, red = failed.

        Args:
            result: The :class:`ValidationResult` from a validation run.
            webhook_url: Slack-compatible incoming webhook URL.
            threshold: The numeric threshold used for the gate decision.
        """
        color = "#36a64f" if result.passed else "#ff0000"
        status_emoji = "✅" if result.passed else "❌"
        status_label = "PASSED" if result.passed else "FAILED"

        failed_summary = (
            "\n".join(
                f"• `{f['expectation_type']}` on column `{f['kwargs'].get('column', 'N/A')}` "
                f"[{f.get('severity', 'unknown')}]"
                for f in result.failed_expectations[:10]
            )
            or "_No failures_"
        )
        if len(result.failed_expectations) > 10:
            failed_summary += f"\n_…and {len(result.failed_expectations) - 10} more_"

        payload = {
            "text": f"{status_emoji} Quality Gate *{status_label}* — `{result.table_name}`",
            "attachments": [
                {
                    "color": color,
                    "title": f"Suite: {result.suite_name}",
                    "fields": [
                        {
                            "title": "Table",
                            "value": result.table_name,
                            "short": True,
                        },
                        {
                            "title": "Status",
                            "value": status_label,
                            "short": True,
                        },
                        {
                            "title": "Pass Rate",
                            "value": f"{result.pass_rate * 100:.1f}%",
                            "short": True,
                        },
                        {
                            "title": "Threshold",
                            "value": f"{threshold * 100:.1f}%",
                            "short": True,
                        },
                        {
                            "title": "Total Expectations",
                            "value": str(result.total_expectations),
                            "short": True,
                        },
                        {
                            "title": "Failed Expectations",
                            "value": str(len(result.failed_expectations)),
                            "short": True,
                        },
                        {
                            "title": "Run ID",
                            "value": result.run_id,
                            "short": False,
                        },
                        {
                            "title": "Failed Expectations Detail",
                            "value": failed_summary,
                            "short": False,
                        },
                    ],
                    "footer": "Data Platform Quality Gate",
                    "ts": int(result.run_at.timestamp()),
                }
            ],
        }

        last_exc: Optional[Exception] = None
        for attempt in range(1, self._MAX_RETRIES + 1):
            try:
                resp = requests.post(
                    webhook_url, json=payload, timeout=self._TIMEOUT_SECONDS
                )
                resp.raise_for_status()
                logger.info(
                    "Webhook alert sent (attempt %d/%d, HTTP %d)",
                    attempt,
                    self._MAX_RETRIES,
                    resp.status_code,
                )
                return
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                logger.warning(
                    "Webhook attempt %d/%d failed: %s",
                    attempt,
                    self._MAX_RETRIES,
                    exc,
                )
                if attempt < self._MAX_RETRIES:
                    time.sleep(self._BACKOFF_SECONDS)

        logger.error(
            "All %d webhook attempts exhausted. Last error: %s",
            self._MAX_RETRIES,
            last_exc,
        )


# ---------------------------------------------------------------------------
# Rich console output helpers
# ---------------------------------------------------------------------------


def _print_results_table(results: list[dict]) -> None:
    """Render a Rich table summarising all expectation results to stdout.

    Args:
        results: Per-expectation result dicts from :meth:`GEValidator.validate`.
    """
    tbl = Table(
        title="Expectation Results",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold cyan",
        show_lines=True,
    )
    tbl.add_column("Expectation Type", style="bold", min_width=40)
    tbl.add_column("Column / Target", min_width=20)
    tbl.add_column("Status", justify="center", min_width=10)
    tbl.add_column("Detail", min_width=30)

    for r in results:
        exp_type = r["expectation_type"]
        column = r["kwargs"].get("column", r["kwargs"].get("column_list", "—"))
        if isinstance(column, list):
            column = ", ".join(column[:4]) + ("…" if len(column) > 4 else "")

        success = r["result"].get("success")
        if success is True:
            status = Text("✅ PASS", style="bold green")
        elif success is False:
            status = Text("❌ FAIL", style="bold red")
        else:
            status = Text("⚠ SKIP", style="bold yellow")

        # Build a human-readable detail string from the result dict.
        result_d = {k: v for k, v in r["result"].items() if k != "success"}
        detail_parts = []
        for k, v in list(result_d.items())[:3]:
            detail_parts.append(f"{k}={v}")
        detail = ", ".join(detail_parts)

        tbl.add_row(exp_type, str(column), status, detail)

    console.print(tbl)


def _print_summary_panel(result: ValidationResult, threshold: float) -> None:
    """Print a final summary panel with pass/fail verdict.

    Args:
        result: The completed :class:`ValidationResult`.
        threshold: Numeric threshold used for the gate decision.
    """
    color = "green" if result.passed else "red"
    verdict = "✅  QUALITY GATE PASSED" if result.passed else "❌  QUALITY GATE FAILED"
    body = (
        f"Suite:              [bold]{result.suite_name}[/bold]\n"
        f"Table:              [bold]{result.table_name}[/bold]\n"
        f"Pass Rate:          [bold]{result.pass_rate * 100:.2f}%[/bold]  "
        f"(threshold: {threshold * 100:.1f}%)\n"
        f"Total Expectations: {result.total_expectations}\n"
        f"Failed:             {len(result.failed_expectations)}\n"
        f"Run ID:             {result.run_id}\n"
        f"Duration:           {result.duration_seconds:.2f}s"
    )
    console.print(
        Panel(
            body,
            title=f"[bold {color}]{verdict}[/bold {color}]",
            border_style=color,
            padding=(1, 2),
        )
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--suite",
    required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True),
    help="Path to the Great Expectations JSON suite file.",
)
@click.option(
    "--table",
    required=True,
    type=str,
    help="Fully-qualified Trino table name, e.g. silver.sales.orders.",
)
@click.option(
    "--threshold",
    default=0.95,
    show_default=True,
    type=click.FloatRange(0.0, 1.0),
    help="Minimum pass rate required to pass the quality gate (0.0–1.0).",
)
@click.option(
    "--trino-host",
    default="localhost",
    show_default=True,
    envvar="TRINO_HOST",
    help="Trino coordinator host.",
)
@click.option(
    "--trino-port",
    default=8080,
    show_default=True,
    envvar="TRINO_PORT",
    type=int,
    help="Trino coordinator port.",
)
@click.option(
    "--trino-user",
    default="data-platform",
    show_default=True,
    envvar="TRINO_USER",
    help="Trino user for authentication.",
)
@click.option(
    "--trino-catalog",
    default="hive",
    show_default=True,
    envvar="TRINO_CATALOG",
    help="Default Trino catalog.",
)
@click.option(
    "--sample-size",
    default=100_000,
    show_default=True,
    type=int,
    help="Maximum number of rows to read from Trino for validation.",
)
@click.option(
    "--input-file",
    default=None,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True),
    help="Local Parquet file to validate instead of reading from Trino.",
)
@click.option(
    "--marquez-url",
    default=None,
    envvar="MARQUEZ_URL",
    help="Base URL of the Marquez lineage API (optional).",
)
@click.option(
    "--webhook-url",
    default=None,
    envvar="WEBHOOK_URL",
    help="Slack-compatible incoming webhook URL for alerts (optional).",
)
@click.option(
    "--output-dir",
    default=None,
    type=click.Path(file_okay=False, dir_okay=True),
    help="Directory to write the JSON result file to (optional).",
)
def main(
    suite: str,
    table: str,
    threshold: float,
    trino_host: str,
    trino_port: int,
    trino_user: str,
    trino_catalog: str,
    sample_size: int,
    input_file: Optional[str],
    marquez_url: Optional[str],
    webhook_url: Optional[str],
    output_dir: Optional[str],
) -> None:
    """Quality gate runner — validates a table against a GE expectation suite.

    Exits with code 0 (pass), 1 (fail), or 2 (execution error).
    """
    run_id = str(uuid.uuid4())
    run_at = datetime.utcnow()
    start = time.monotonic()

    console.rule(f"[bold cyan]Quality Gate Run — {run_id}[/bold cyan]")
    logger.info("Starting quality gate run: run_id=%s, table=%s, suite=%s", run_id, table, suite)

    exit_code = 0

    try:
        # ── Step 1: Load data ────────────────────────────────────────────
        if input_file is not None:
            console.print(f"[cyan]Reading local Parquet file:[/cyan] {input_file}")
            logger.info("Reading from local Parquet: %s", input_file)
            df = pd.read_parquet(input_file)
            logger.info("Loaded %d rows from %s", len(df), input_file)
        else:
            console.print(
                f"[cyan]Connecting to Trino:[/cyan] {trino_host}:{trino_port} "
                f"(catalog={trino_catalog})"
            )
            reader = TrinoTableReader(
                host=trino_host,
                port=trino_port,
                user=trino_user,
                catalog=trino_catalog,
            )
            df = reader.read_sample(table, sample_size=sample_size)

        console.print(
            f"[green]Data loaded:[/green] {len(df):,} rows × {len(df.columns)} columns"
        )

        # ── Step 2: Load suite and validate ─────────────────────────────
        validator = GEValidator(suite_path=suite)
        console.print(
            f"[cyan]Running suite:[/cyan] {validator.suite_name} "
            f"({len(validator._expectations)} expectations)"
        )
        results = validator.validate(df)

        # ── Step 3: Compute metrics ──────────────────────────────────────
        pass_rate = validator.compute_pass_rate(results)
        failures = validator.format_failures(results)
        duration = time.monotonic() - start
        gate_passed = pass_rate >= threshold

        validation_result = ValidationResult(
            suite_name=validator.suite_name,
            table_name=table,
            pass_rate=round(pass_rate, 6),
            passed=gate_passed,
            total_expectations=len(
                [r for r in results if r["result"].get("success") is not None]
            ),
            failed_expectations=failures,
            run_id=run_id,
            run_at=run_at,
            duration_seconds=round(duration, 3),
        )

        # ── Step 4: Rich output ──────────────────────────────────────────
        _print_results_table(results)
        _print_summary_panel(validation_result, threshold)

        # ── Step 5: Publish to Marquez ───────────────────────────────────
        if marquez_url:
            console.print(f"[cyan]Publishing to Marquez:[/cyan] {marquez_url}")
            publisher = MarquezPublisher()
            publisher.publish_quality_run(validation_result, marquez_url)

        # ── Step 6: Send webhook alert ────────────────────────────────────
        if webhook_url:
            console.print(f"[cyan]Sending webhook alert:[/cyan] {webhook_url}")
            alerter = WebhookAlerter()
            alerter.send_alert(validation_result, webhook_url, threshold=threshold)

        # ── Step 7: Save results JSON ─────────────────────────────────────
        if output_dir:
            out_path = Path(output_dir)
            out_path.mkdir(parents=True, exist_ok=True)
            filename = (
                f"quality_gate_{validator.suite_name}_{run_id[:8]}.json"
            )
            full_path = out_path / filename
            with full_path.open("w", encoding="utf-8") as fh:
                json.dump(validation_result.to_dict(), fh, indent=2, default=str)
            console.print(f"[green]Results saved:[/green] {full_path}")
            logger.info("Results written to %s", full_path)

        # ── Step 8: Set exit code ────────────────────────────────────────
        exit_code = 0 if gate_passed else 1

    except FileNotFoundError as exc:
        logger.error("File not found: %s", exc)
        console.print(f"[bold red]ERROR:[/bold red] File not found — {exc}")
        exit_code = 2
    except RuntimeError as exc:
        logger.error("Infrastructure error: %s", exc)
        console.print(f"[bold red]ERROR:[/bold red] Infrastructure error — {exc}")
        exit_code = 2
    except Exception as exc:
        logger.exception("Unexpected error during quality gate run: %s", exc)
        console.print(f"[bold red]UNEXPECTED ERROR:[/bold red] {exc}")
        exit_code = 2

    logger.info(
        "Quality gate run complete: run_id=%s, exit_code=%d", run_id, exit_code
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
