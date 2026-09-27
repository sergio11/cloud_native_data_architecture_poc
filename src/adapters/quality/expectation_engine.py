"""
Modern Data Architecture POC - Great Expectations Quality Engine Adapter.
Implements QualityEnginePort evaluating assertion suites on datasets without heavy C dependencies.

Fail-closed semantics (RF-06):
  * Missing suite file        -> raises SuiteNotFoundError (never a silent pass).
  * Empty records or 0 expectations -> evaluation with passed=False, pass_rate=0.0
    and a failure entry explaining "no data / no assertions to evaluate".
  * Unsupported expectation type    -> recorded as a failing assertion AND forces
    passed=False regardless of the threshold arithmetic.

Null / ``mostly`` policy:
  * ``mostly`` (default 1.0, i.e. strict) is honoured consistently for not-null,
    regex, in-set and between checks: the tolerated invalid fraction is
    ``1 - mostly``.
  * not-null counts ``None`` and blank/whitespace strings as invalid.
  * in-set counts ``None`` as invalid unless ``None`` is explicitly part of
    ``value_set`` (kept from the previous semantics).
  * between only bounds-checks non-null values; nulls are skipped (not invalid).
  * unique ignores nulls; duplicates among non-null values fail.
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
from src.ports.outbound import QualityEnginePort

logger = logging.getLogger("adapter.quality.engine")

FAIL_CLOSED_RULE = "fail_closed"
FAIL_CLOSED_MARKER = "no data / no assertions to evaluate"

# Shared signature of every per-type handler:
# (records, kwargs, mostly, col, col_key) -> (success, observed, details, column).
_ExpectationHandler = Callable[
    [list[dict[str, Any]], dict[str, Any], float, Any, str],
    tuple[bool, Any, str, Any],
]


class ExpectationEngineAdapter(QualityEnginePort):
    """Evaluates Great Expectations JSON suites against records (fail-closed, RF-06).

    Open/Closed design: ``evaluate_suite`` dispatches each expectation through
    the handler map instead of an if/elif chain. To support a new expectation
    type, add one small ``_eval_*`` method with the shared handler signature
    (preserving the null/``mostly``/fail-closed conventions from the module
    docstring) and register it with one map entry — the core loop is never
    modified.
    """

    def evaluate_suite(
        self,
        records: list[dict[str, Any]],
        suite_path: str,
        threshold: float = 0.95,
        table_name: str = "",
    ) -> QualityGateEvaluation:
        if not os.path.isfile(suite_path):
            raise SuiteNotFoundError(f"Quality expectation suite not found: {suite_path}")

        with open(suite_path, encoding="utf-8") as f:
            suite = json.load(f)

        suite_name = suite.get("expectation_suite_name") or os.path.basename(suite_path)
        expectations = suite.get("expectations") or []
        total_assertions = len(expectations)

        # Fail-closed: a gate that cannot evaluate must not approve (RF-06).
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
                failures=[QualityAssertionResult(
                    rule_name=FAIL_CLOSED_RULE,
                    column=None,
                    success=False,
                    observed_value=f"{len(records)} records, {total_assertions} expectations",
                    expected_value={"min_records": 1, "min_expectations": 1},
                    details=reason,
                )],
            )

        passed_count = 0
        failures: list[QualityAssertionResult] = []
        unsupported = False

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
            if handler is None:
                # Unknown expectation type: fail closed - the engine cannot verify it.
                success = False
                unsupported = True
                observed = f"Unsupported expectation type: {exp_type}"
                details = (
                    f"the quality engine cannot verify '{exp_type}'; unsupported expectations "
                    "are recorded as failures and block approval (fail-closed, RF-06)"
                )
                logger.error(
                    "Unsupported expectation type '%s' in suite '%s'; failing closed.",
                    exp_type, suite_name
                )
            else:
                success, observed, details, col = handler(records, kwargs, mostly, col, col_key)

            if success:
                passed_count += 1
            else:
                failures.append(QualityAssertionResult(
                    rule_name=exp_type or "unknown_expectation",
                    column=col,
                    success=False,
                    observed_value=observed,
                    expected_value=kwargs,
                    details=details,
                ))

        pass_rate = round(passed_count / total_assertions, 4)
        # Threshold governs the verdict, but fail-closed conditions (unsupported
        # expectation types) force passed=False regardless of the arithmetic.
        passed = (pass_rate >= threshold) and not unsupported
        return QualityGateEvaluation(
            suite_name=suite_name,
            table_name=table_name,
            total_assertions=total_assertions,
            passed_assertions=passed_count,
            pass_rate=pass_rate,
            threshold=threshold,
            passed=passed,
            failures=failures,
        )

    def _eval_not_null(
        self,
        records: list[dict[str, Any]],
        kwargs: dict[str, Any],
        mostly: float,
        col: Any,
        col_key: str,
    ) -> tuple[bool, Any, str, Any]:
        nulls = sum(1 for r in records if r.get(col_key) is None or str(r.get(col_key)).strip() == "")
        success = (nulls / len(records)) <= (1.0 - mostly)
        observed = f"{nulls} nulls ({nulls / len(records):.1%})"
        details = (
            f"mostly={mostly}: null/blank fraction must be <= {1.0 - mostly:.2f} "
            "(nulls counted over all rows)"
        )
        return success, observed, details, col

    def _eval_unique(
        self,
        records: list[dict[str, Any]],
        kwargs: dict[str, Any],
        mostly: float,
        col: Any,
        col_key: str,
    ) -> tuple[bool, Any, str, Any]:
        vals = [r.get(col_key) for r in records if r.get(col_key) is not None]
        dups = len(vals) - len(set(vals))
        success = dups == 0
        observed = f"{dups} duplicates"
        details = "nulls are ignored for uniqueness; duplicate non-null values fail"
        return success, observed, details, col

    def _eval_in_set(
        self,
        records: list[dict[str, Any]],
        kwargs: dict[str, Any],
        mostly: float,
        col: Any,
        col_key: str,
    ) -> tuple[bool, Any, str, Any]:
        val_set = set(kwargs.get("value_set") or [])
        invalids = sum(1 for r in records if r.get(col_key) not in val_set)
        success = (invalids / len(records)) <= (1.0 - mostly)
        observed = f"{invalids} outside allowed set"
        details = (
            f"mostly={mostly}: values outside value_set must be <= {1.0 - mostly:.2f}; "
            "nulls count as invalid unless None is explicitly in value_set"
        )
        return success, observed, details, col

    def _eval_between(
        self,
        records: list[dict[str, Any]],
        kwargs: dict[str, Any],
        mostly: float,
        col: Any,
        col_key: str,
    ) -> tuple[bool, Any, str, Any]:
        min_v = kwargs.get("min_value")
        max_v = kwargs.get("max_value")
        out_of_bounds = 0
        for r in records:
            val = r.get(col_key)
            if val is None:
                continue
            try:
                fval = float(val)
                if min_v is not None and fval < min_v:
                    out_of_bounds += 1
                elif max_v is not None and fval > max_v:
                    out_of_bounds += 1
            except (ValueError, TypeError):
                out_of_bounds += 1
        success = (out_of_bounds / len(records)) <= (1.0 - mostly)
        observed = f"{out_of_bounds} values out of bounds [{min_v}, {max_v}]"
        details = (
            f"mostly={mostly}: non-null values outside [{min_v}, {max_v}] must be "
            f"<= {1.0 - mostly:.2f}; nulls are skipped (not bounds-checked); "
            "non-numeric values count as out of bounds"
        )
        return success, observed, details, col

    def _eval_regex(
        self,
        records: list[dict[str, Any]],
        kwargs: dict[str, Any],
        mostly: float,
        col: Any,
        col_key: str,
    ) -> tuple[bool, Any, str, Any]:
        regex_pat = re.compile(kwargs.get("regex") or ".*")
        non_matches = sum(1 for r in records if not regex_pat.match(str(r.get(col_key, ""))))
        success = (non_matches / len(records)) <= (1.0 - mostly)
        observed = f"{non_matches} regex non-matches"
        details = (
            f"mostly={mostly}: regex non-match fraction must be <= {1.0 - mostly:.2f} "
            "(match anchored at start of value)"
        )
        return success, observed, details, col

    def _eval_row_count(
        self,
        records: list[dict[str, Any]],
        kwargs: dict[str, Any],
        mostly: float,
        col: Any,
        col_key: str,
    ) -> tuple[bool, Any, str, Any]:
        min_rows = kwargs.get("min_value")
        max_rows = kwargs.get("max_value")
        count = len(records)
        success = (min_rows is None or count >= min_rows) and (
            max_rows is None or count <= max_rows
        )
        observed = f"{count} rows (expected between {min_rows} and {max_rows})"
        details = "row count bounds are inclusive; None means unbounded"
        return success, observed, details, None
