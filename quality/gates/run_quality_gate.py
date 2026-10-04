"""
run_quality_gate.py - Quality gate runner for data pipelines.
Driving Adapter invoking RunQualityGateUseCase via Hexagonal Architecture container.
Runs Great Expectations validation suites against Lakehouse tables or local files.
Blocks pipeline progression if quality threshold is not met (RF-06).

The CLI never evaluates expectations itself: it delegates to
``container.run_quality_gate.execute(...)`` so the fail-closed core rules
(missing suite -> SuiteNotFoundError, empty record set -> failed evaluation)
always apply. Threshold precedence mirrors the Silver/Gold promotion gate: the
thresholds declared by the contract governing the table's entity win per
dimension, and ``--threshold`` is only the fallback when that contract declares
none - so this CLI can never PASS a table that promotion then rejects.

Exit codes:
  0: Quality gate PASSED
  1: Quality gate FAILED (blocks pipeline) - includes "no records to evaluate"
     (fail-closed empty evaluation, RF-06)
  2: Execution error (e.g. SuiteNotFoundError, unreadable --input-file)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import click
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.container import container
from src.domain.entities import QualityGateEvaluation
from src.domain.exceptions import QualityGateBreachError, SuiteNotFoundError
from src.domain.naming import contract_entity_for_table
from src.domain.quality_gate import FAIL_CLOSED_RULE, assess_gate
from src.use_cases.gate_support import find_contract_thresholds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("quality_gate")
console = Console()


def _print_results_table(evaluation: QualityGateEvaluation) -> None:
    tbl = Table(
        box=box.ROUNDED,
        show_header=True,
        header_style="bold cyan",
        title=f"[bold]Expectation Suite Evaluation: {evaluation.suite_name}[/bold]",
    )
    tbl.add_column("Rule / Assertion", style="cyan")
    tbl.add_column("Target Column", style="magenta")
    tbl.add_column("Status", justify="center")
    tbl.add_column("Details", style="white")

    if not evaluation.failures:
        tbl.add_row("All Suite Expectations", "*", "[bold green]PASS[/bold green]", "100% assertions satisfied")
    else:
        for f in evaluation.failures:
            tbl.add_row(
                f.rule_name,
                str(f.column or "*"),
                "[bold red]FAIL[/bold red]",
                f"Observed: {f.observed_value}",
            )
    console.print(tbl)


def _print_summary_panel(
    evaluation: QualityGateEvaluation,
    threshold: float,
    reason: str | None = None,
    thresholds: dict[str, float] | None = None,
) -> None:
    status_text = "[bold green]PASSED[/bold green]" if evaluation.passed else "[bold red]FAILED[/bold red]"
    pct = evaluation.pass_rate * 100.0
    thresh_pct = threshold * 100.0

    lines = [
        f"Table: [bold]{evaluation.table_name}[/bold]",
        f"Suite: [bold]{evaluation.suite_name}[/bold]",
        f"Passed: {evaluation.passed_assertions} / {evaluation.total_assertions} assertions",
        f"Pass Rate: [bold]{pct:.1f}%[/bold] (Threshold: {thresh_pct:.1f}%)",
        f"Gate Verdict: {status_text}",
    ]
    effective = thresholds or {}
    if effective:
        rendered = ", ".join(f"{dim}={required:.2f}" for dim, required in sorted(effective.items()))
        lines.append(f"Effective Thresholds: {rendered}")
    if reason:
        lines.append(f"Reason: {reason}")
    border_color = "green" if evaluation.passed else "red"
    console.print(
        Panel(
            "\n".join(lines),
            title="[bold]Quality Gate Summary[/bold]",
            border_style=border_color,
            expand=False,
        )
    )


@click.command()
@click.option("--suite", required=True, help="Path to Great Expectations suite JSON")
@click.option("--table", required=True, help="Target Lakehouse table (e.g. silver.sales.orders)")
@click.option(
    "--threshold",
    default=0.95,
    type=float,
    help="Fallback pass-rate threshold (0.0-1.0) when the contract declares no thresholds",
)
@click.option("--input-file", default=None, help="Optional local JSON/Parquet file to validate")
@click.option("--marquez-url", default=None, help="Marquez URL")
@click.option("--output-dir", default=None, help="Directory to save run results JSON")
def main(
    suite: str,
    table: str,
    threshold: float,
    input_file: str | None,
    marquez_url: str | None,
    output_dir: str | None,
) -> None:
    run_id = str(uuid.uuid4())
    console.rule(f"[bold cyan]Quality Gate Run — {run_id}[/bold cyan]")
    logger.info("Starting quality gate run for table '%s' using suite '%s'", table, suite)

    # Flag > env > default: point the lazily-built lineage adapter at the
    # requested Marquez before it is first constructed, and drop any adapter
    # cached by a previous invocation in this process so the POST lands on
    # this run's URL instead of a stale one.
    if marquez_url:
        os.environ["MARQUEZ_URL"] = marquez_url
        container.__dict__.pop("lineage", None)

    try:
        # 1. Resolve records: --input-file is passed through via records=;
        #    otherwise the use case loads the table from storage itself.
        records: list[dict[str, Any]] | None = None
        if input_file:
            input_path = Path(input_file)
            if not input_path.is_file():
                raise FileNotFoundError(f"Input file not found: {input_file}")
            with open(input_path, encoding="utf-8") as f:
                records = json.load(f)
            if not isinstance(records, list):
                raise ValueError(f"Input file must contain a JSON array of records: {input_file}")

        # 2. Delegate to the core use case - the fail-closed rules (RF-06) live
        #    there: a failing verdict raises QualityGateBreachError carrying
        #    the evaluation (exit 1: blocked), a missing suite raises
        #    SuiteNotFoundError (exit 2: execution error).
        breach_reason: str | None = None
        try:
            evaluation = container.run_quality_gate.execute(
                table_name=table,
                suite_path=suite,
                threshold=threshold,
                records=records,
            )
        except QualityGateBreachError as breach:
            if breach.evaluation is None:  # pragma: no cover - defensive, always attached
                raise
            logger.error("Quality gate FAILED: %s", breach)
            evaluation = breach.evaluation
            breach_reason = str(breach)

        _print_results_table(evaluation)
        # Display-only verdict: recompute the shared policy verdict so the
        # panel shows the effective per-dimension thresholds that governed
        # (contract thresholds when they apply, else the fallback). The
        # pass/fail decision itself already came from the use case above.
        entity = contract_entity_for_table(table)
        contract_thresholds = find_contract_thresholds(container.domain_catalog, entity)
        verdict = assess_gate(evaluation, contract_thresholds, threshold)
        _print_summary_panel(evaluation, threshold, reason=breach_reason, thresholds=verdict.thresholds)

        if not evaluation.passed and any(f.rule_name == FAIL_CLOSED_RULE for f in evaluation.failures):
            console.print(
                "[bold red]No records to evaluate[/bold red] - the fail-closed "
                "quality gate cannot approve an empty dataset (RF-06)."
            )

        # Emit Lineage if configured
        if marquez_url or os.getenv("MARQUEZ_URL"):
            container.lineage.emit_job_event(
                job_name=f"quality_gate_{table}",
                inputs=[table],
                outputs=[table],
                status="COMPLETE" if evaluation.passed else "FAIL",
            )

        if output_dir:
            out_path = Path(output_dir)
            out_path.mkdir(parents=True, exist_ok=True)
            res_file = out_path / f"quality_result_{evaluation.suite_name}_{run_id[:8]}.json"
            with open(res_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "suite": evaluation.suite_name,
                        "table": table,
                        "passed": evaluation.passed,
                        "pass_rate": evaluation.pass_rate,
                        "threshold": threshold,
                        "timestamp": datetime.now(UTC).isoformat(),
                    },
                    f,
                    indent=2,
                )

        exit_code = 0 if evaluation.passed else 1
        sys.exit(exit_code)

    except SuiteNotFoundError as exc:
        logger.error("Quality gate execution error: %s", exc)
        console.print(f"[bold red]Quality Gate Execution Error:[/bold red] {exc}")
        sys.exit(2)
    except Exception as exc:
        logger.error("Quality gate execution error: %s", exc)
        console.print(f"[bold red]Quality Gate Error:[/bold red] {exc}")
        sys.exit(2)


if __name__ == "__main__":
    main()
