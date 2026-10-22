"""Modern Data Architecture POC - Quality engine adapter.

Implements QualityEnginePort over Great Expectations JSON suites. Missing
suites, empty data and unsupported expectations all fail closed.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Callable
from typing import Any

from src.domain.entities import QualityAssertionResult, QualityGateEvaluation
from src.domain.exceptions import SuiteNotFoundError
from src.domain.quality_gate import DEFAULT_QUALITY_THRESHOLD, FAIL_CLOSED_RULE
from src.ports.outbound import QualityEnginePort

logger = logging.getLogger("adapter.quality.engine")

FAIL_CLOSED_MARKER = "no data / no assertions to evaluate"

# Quality dimension per expectation type. Unsupported types map to no dimension.
DIMENSION_BY_EXPECTATION: dict[str, str] = {
    "expect_column_values_to_not_be_null": "completeness",
    "expect_table_row_count_to_be_between": "completeness",
    "expect_column_values_to_be_unique": "uniqueness",
    "expect_column_values_to_be_in_set": "validity",
    "expect_column_values_to_be_between": "validity",
    "expect_column_values_to_match_regex": "validity",
}

# Shared handler signature: (records, kwargs, mostly, col, col_key) ->
# (success, observed, details, column, failed_row_indices).
_ExpectationHandler = Callable[
    [list[dict[str, Any]], dict[str, Any], float, Any, str],
    tuple[bool, Any, str, Any, set[int] | None],
]


class ExpectationEngineAdapter(QualityEnginePort):
    """Evaluates Great Expectations JSON suites against records (fail-closed).

    New expectation types are added by registering one handler in the map.
    """

    def evaluate_suite(
        self,
        records: list[dict[str, Any]],
        suite_path: str,
        threshold: float = DEFAULT_QUALITY_THRESHOLD,
        table_name: str = "",
    ) -> QualityGateEvaluation:
        if not os.path.isfile(suite_path):
            raise SuiteNotFoundError(f"Quality expectation suite not found: {suite_path}")

        with open(suite_path, encoding="utf-8") as f:
            suite = json.load(f)

        suite_name = suite.get("expectation_suite_name") or os.path.basename(suite_path)
        expectations = suite.get("expectations") or []
        total_assertions = len(expectations)

        # Fail-closed: a gate that cannot evaluate must not approve.
        if not records or total_assertions == 0:
            reason = (
                f"{FAIL_CLOSED_MARKER}: {len(records)} record(s) and "
                f"{total_assertions} expectation(s) available; the engine cannot "
                "verify data quality, so the gate fails closed (RF-06)."
            )
            logger.error("Quality gate fail-closed for '%s': %s", table_name or suite_name, reason)
            return QualityGateEvaluation(
                suite_name=suite_name,
                table_name=table_name,
                total_assertions=total_assertions,
                passed_assertions=0,
                pass_rate=0.0,
                threshold=threshold,
                passed=False,
                failures=[
                    QualityAssertionResult(
                        rule_name=FAIL_CLOSED_RULE,
                        column=None,
                        success=False,
                        observed_value=f"{len(records)} records, {total_assertions} expectations",
                        expected_value={"min_records": 1, "min_expectations": 1},
                        details=reason,
                    )
                ],
            )

        passed_count = 0
        failures: list[QualityAssertionResult] = []
        unsupported = False
        dimension_total: dict[str, int] = {}
        dimension_passed: dict[str, int] = {}
        dimension_failed_rows: dict[str, set[int]] = {}
        dimension_attributable: dict[str, bool] = {}

        handlers: dict[str, _ExpectationHandler] = {
            "expect_column_values_to_not_be_null": self._eval_not_null,
            "expect_column_values_to_be_unique": self._eval_unique,
            "expect_column_values_to_be_in_set": self._eval_in_set,
            "expect_column_values_to_be_between": self._eval_between,
            "expect_column_values_to_match_regex": self._eval_regex,
            "expect_table_row_count_to_be_between": self._eval_row_count,
        }

        for exp in expectations:
            exp_type = exp.get("expectation_type") or ""
            kwargs = exp.get("kwargs") or {}
            col = kwargs.get("column")
            col_key = "" if col is None else str(col)
            mostly = kwargs.get("mostly", 1.0)
            success: bool
            details = ""
            observed: Any = None

            handler = handlers.get(exp_type)
            failed_rows: set[int] | None = None
            if handler is None:
                # Unknown expectation type: fail closed - the engine cannot verify it.
                success = False
                unsupported = True
                observed = f"Unsupported expectation type: {exp_type}"
                details = (
                    f"the quality engine cannot verify '{exp_type}'; unsupported expectations "
                    "are recorded as failures and block approval (fail-closed, RF-06)"
                )
                logger.error("Unsupported expectation type '%s' in suite '%s'; failing closed.", exp_type, suite_name)
            else:
                success, observed, details, col, failed_rows = handler(records, kwargs, mostly, col, col_key)

            dimension = DIMENSION_BY_EXPECTATION.get(exp_type)
            if dimension is not None:
                dimension_total[dimension] = dimension_total.get(dimension, 0) + 1
                if success:
                    dimension_passed[dimension] = dimension_passed.get(dimension, 0) + 1
                if failed_rows is None:
                    # Non-attributable assertion (row-count): no failing rows.
                    dimension_attributable.setdefault(dimension, False)
                else:
                    dimension_attributable[dimension] = True
                    dimension_failed_rows.setdefault(dimension, set()).update(failed_rows)

            if success:
                passed_count += 1
            else:
                failures.append(
                    QualityAssertionResult(
                        rule_name=exp_type or "unknown_expectation",
                        column=col,
                        success=False,
                        observed_value=observed,
                        expected_value=kwargs,
                        details=details,
                    )
                )

        pass_rate = round(passed_count / total_assertions, 4)
        # Unsupported expectation types force passed=False regardless of arithmetic.
        passed = (pass_rate >= threshold) and not unsupported
        dimension_rates: dict[str, float] = {}
        for dimension, total in dimension_total.items():
            if dimension_attributable.get(dimension, False):
                # Per-record semantics: 1 - (distinct failing rows / total rows).
                failed = len(dimension_failed_rows.get(dimension, set()))
                dimension_rates[dimension] = round(1.0 - failed / len(records), 4)
            else:
                # Fallback: dimension measured only by non-attributable assertions.
                dimension_rates[dimension] = round(dimension_passed.get(dimension, 0) / total, 4)
        return QualityGateEvaluation(
            suite_name=suite_name,
            table_name=table_name,
            total_assertions=total_assertions,
            passed_assertions=passed_count,
            pass_rate=pass_rate,
            threshold=threshold,
            passed=passed,
            failures=failures,
            dimension_rates=dimension_rates,
            unsupported=unsupported,
        )

    def _eval_not_null(
        self,
        records: list[dict[str, Any]],
        kwargs: dict[str, Any],
        mostly: float,
        col: Any,
        col_key: str,
    ) -> tuple[bool, Any, str, Any, set[int] | None]:
        failed = {i for i, r in enumerate(records) if r.get(col_key) is None or str(r.get(col_key)).strip() == ""}
        success = (len(failed) / len(records)) <= (1.0 - mostly)
        observed = f"{len(failed)} nulls ({len(failed) / len(records):.1%})"
        details = f"mostly={mostly}: null/blank fraction must be <= {1.0 - mostly:.2f} (nulls counted over all rows)"
        return success, observed, details, col, failed

    def _eval_unique(
        self,
        records: list[dict[str, Any]],
        kwargs: dict[str, Any],
        mostly: float,
        col: Any,
        col_key: str,
    ) -> tuple[bool, Any, str, Any, set[int] | None]:
        counts: dict[Any, int] = {}
        for r in records:
            val = r.get(col_key)
            if val is None:
                continue
            counts[val] = counts.get(val, 0) + 1
        duplicated_values = {val for val, n in counts.items() if n > 1}
        failed = {i for i, r in enumerate(records) if r.get(col_key) in duplicated_values}
        dups = len([r for r in records if r.get(col_key) is not None]) - len(counts)
        success = dups == 0
        observed = f"{dups} duplicates"
        details = "nulls are ignored for uniqueness; duplicate non-null values fail"
        return success, observed, details, col, failed

    def _eval_in_set(
        self,
        records: list[dict[str, Any]],
        kwargs: dict[str, Any],
        mostly: float,
        col: Any,
        col_key: str,
    ) -> tuple[bool, Any, str, Any, set[int] | None]:
        val_set = set(kwargs.get("value_set") or [])
        failed = {i for i, r in enumerate(records) if r.get(col_key) not in val_set}
        success = (len(failed) / len(records)) <= (1.0 - mostly)
        observed = f"{len(failed)} outside allowed set"
        details = (
            f"mostly={mostly}: values outside value_set must be <= {1.0 - mostly:.2f}; "
            "nulls count as invalid unless None is explicitly in value_set"
        )
        return success, observed, details, col, failed

    def _eval_between(
        self,
        records: list[dict[str, Any]],
        kwargs: dict[str, Any],
        mostly: float,
        col: Any,
        col_key: str,
    ) -> tuple[bool, Any, str, Any, set[int] | None]:
        min_v = kwargs.get("min_value")
        max_v = kwargs.get("max_value")
        failed: set[int] = set()
        for i, r in enumerate(records):
            val = r.get(col_key)
            if val is None:
                continue
            try:
                fval = float(val)
                if min_v is not None and fval < min_v:
                    failed.add(i)
                elif max_v is not None and fval > max_v:
                    failed.add(i)
            except (ValueError, TypeError):
                failed.add(i)
        success = (len(failed) / len(records)) <= (1.0 - mostly)
        observed = f"{len(failed)} values out of bounds [{min_v}, {max_v}]"
        details = (
            f"mostly={mostly}: non-null values outside [{min_v}, {max_v}] must be "
            f"<= {1.0 - mostly:.2f}; nulls are skipped (not bounds-checked); "
            "non-numeric values count as out of bounds"
        )
        return success, observed, details, col, failed

    def _eval_regex(
        self,
        records: list[dict[str, Any]],
        kwargs: dict[str, Any],
        mostly: float,
        col: Any,
        col_key: str,
    ) -> tuple[bool, Any, str, Any, set[int] | None]:
        regex_pat = re.compile(kwargs.get("regex") or ".*")
        failed = {i for i, r in enumerate(records) if not regex_pat.match(str(r.get(col_key, "")))}
        success = (len(failed) / len(records)) <= (1.0 - mostly)
        observed = f"{len(failed)} regex non-matches"
        details = (
            f"mostly={mostly}: regex non-match fraction must be <= {1.0 - mostly:.2f} "
            "(match anchored at start of value)"
        )
        return success, observed, details, col, failed

    def _eval_row_count(
        self,
        records: list[dict[str, Any]],
        kwargs: dict[str, Any],
        mostly: float,
        col: Any,
        col_key: str,
    ) -> tuple[bool, Any, str, Any, set[int] | None]:
        min_rows = kwargs.get("min_value")
        max_rows = kwargs.get("max_value")
        count = len(records)
        success = (min_rows is None or count >= min_rows) and (max_rows is None or count <= max_rows)
        observed = f"{count} rows (expected between {min_rows} and {max_rows})"
        details = "row count bounds are inclusive; None means unbounded"
        return success, observed, details, None, None
