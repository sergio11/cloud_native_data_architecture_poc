"""
Modern Data Architecture POC - Unit Tests: quality dimension rates & contract thresholds.
Covers the expectation engine ``dimension_rates`` breakdown per quality dimension
(completeness / uniqueness / validity) and the ``extract_quality_thresholds``
contract helper. Hermetic: suites are written under tmp_path, never under data/.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from src.adapters.quality.expectation_engine import ExpectationEngineAdapter
from src.domain.contracts import extract_quality_thresholds
from src.domain.quality_gate import assess_gate


def _write_suite(tmp_path: Any, name: str, expectations: list[dict[str, Any]]) -> str:
    suite_file = tmp_path / f"{name}.json"
    suite_file.write_text(
        json.dumps({"expectation_suite_name": name, "expectations": expectations}),
        encoding="utf-8",
    )
    return str(suite_file)


def _exp(exp_type: str, **kwargs: Any) -> dict[str, Any]:
    return {"expectation_type": exp_type, "kwargs": kwargs}


# ---------------------------------------------------------------------------
# Engine: dimension_rates
# ---------------------------------------------------------------------------


def test_dimension_rates_all_dimensions(tmp_path: Any) -> None:
    suite_path = _write_suite(
        tmp_path,
        "dim_suite",
        [
            _exp("expect_column_values_to_not_be_null", column="id"),
            _exp("expect_table_row_count_to_be_between", min_value=1, max_value=10),
            _exp("expect_column_values_to_be_unique", column="id"),
            _exp("expect_column_values_to_be_in_set", column="status", value_set=["confirmed", "pending"]),
            _exp("expect_column_values_to_be_between", column="amount", min_value=0, max_value=1000),
            _exp("expect_column_values_to_match_regex", column="code", regex=r"^A-"),
        ],
    )
    records = [
        {"id": 1, "status": "confirmed", "amount": 10.0, "code": "A-1"},
        {"id": 2, "status": "pending", "amount": 20.0, "code": "A-2"},
    ]

    evaluation = ExpectationEngineAdapter().evaluate_suite(records, suite_path, threshold=1.0)

    assert evaluation.passed is True
    assert evaluation.pass_rate == 1.0
    assert evaluation.dimension_rates == {"completeness": 1.0, "uniqueness": 1.0, "validity": 1.0}


def test_dimension_rates_partial_pass(tmp_path: Any) -> None:
    suite_path = _write_suite(
        tmp_path,
        "partial_suite",
        [
            _exp("expect_column_values_to_not_be_null", column="id"),
            _exp("expect_table_row_count_to_be_between", min_value=100),
            _exp("expect_column_values_to_be_unique", column="id"),
            _exp("expect_column_values_to_be_in_set", column="status", value_set=["confirmed"]),
        ],
    )
    # Per-record semantics: row-count is not attributable to rows, so the
    # passing not-null assertion alone yields completeness 1.0; both rows share
    # a duplicated id => uniqueness 0.0; one of two rows has an unknown status
    # => validity 0.5.
    records = [
        {"id": 1, "status": "confirmed"},
        {"id": 1, "status": "cancelled"},
    ]

    evaluation = ExpectationEngineAdapter().evaluate_suite(records, suite_path, threshold=1.0)

    assert evaluation.passed is False
    assert evaluation.dimension_rates == {"completeness": 1.0, "uniqueness": 0.0, "validity": 0.5}


def test_dimension_rates_only_present_dimensions(tmp_path: Any) -> None:
    suite_path = _write_suite(
        tmp_path,
        "unique_only_suite",
        [
            _exp("expect_column_values_to_be_unique", column="id"),
        ],
    )
    records = [{"id": 1}, {"id": 2}]

    evaluation = ExpectationEngineAdapter().evaluate_suite(records, suite_path, threshold=1.0)

    assert evaluation.passed is True
    assert evaluation.dimension_rates == {"uniqueness": 1.0}


def test_unsupported_expectation_has_no_dimension_and_fails_closed(tmp_path: Any) -> None:
    suite_path = _write_suite(
        tmp_path,
        "unsupported_suite",
        [
            _exp("expect_column_values_to_not_be_null", column="id"),
            _exp("expect_column_values_to_be_zip_code", column="zip"),
        ],
    )

    evaluation = ExpectationEngineAdapter().evaluate_suite(
        [{"id": 1, "zip": "12345"}],
        suite_path,
        threshold=0.0,
        table_name="silver.sales.orders",
    )

    assert evaluation.passed is False
    assert evaluation.dimension_rates == {"completeness": 1.0}


def test_dimension_rates_do_not_change_global_verdict(tmp_path: Any) -> None:
    suite_path = _write_suite(
        tmp_path,
        "verdict_suite",
        [
            _exp("expect_column_values_to_not_be_null", column="id"),
            _exp("expect_column_values_to_be_unique", column="id"),
        ],
    )
    records = [{"id": 1}, {"id": 1}]

    evaluation = ExpectationEngineAdapter().evaluate_suite(records, suite_path, threshold=0.5)

    assert evaluation.pass_rate == 0.5
    assert evaluation.passed is True
    assert evaluation.dimension_rates == {"completeness": 1.0, "uniqueness": 0.0}


def test_dimension_rates_per_record_union_across_assertions(tmp_path: Any) -> None:
    """Two validity assertions failing on different rows union their failing rows."""
    suite_path = _write_suite(
        tmp_path,
        "union_suite",
        [
            _exp("expect_column_values_to_be_in_set", column="status", value_set=["confirmed"]),
            _exp("expect_column_values_to_match_regex", column="code", regex=r"^A-"),
        ],
    )
    records = [
        {"status": "cancelled", "code": "A-1"},  # fails validity via status only
        {"status": "confirmed", "code": "B-2"},  # fails validity via code only
        {"status": "confirmed", "code": "A-3"},  # clean
        {"status": "confirmed", "code": "A-4"},  # clean
    ]

    evaluation = ExpectationEngineAdapter().evaluate_suite(records, suite_path, threshold=1.0)

    assert evaluation.passed is False
    assert evaluation.dimension_rates == {"validity": 0.5}


def test_dimension_rate_falls_back_to_assertion_ratio_when_not_attributable(tmp_path: Any) -> None:
    """A dimension measured ONLY by row-count keeps the legacy assertion ratio."""
    suite_path = _write_suite(
        tmp_path,
        "row_count_only_suite",
        [
            _exp("expect_table_row_count_to_be_between", min_value=100),
        ],
    )

    failing = ExpectationEngineAdapter().evaluate_suite([{"id": 1}, {"id": 2}], suite_path, threshold=1.0)
    assert failing.passed is False
    assert failing.dimension_rates == {"completeness": 0.0}

    passing = ExpectationEngineAdapter().evaluate_suite(
        [{"id": i} for i in range(100)],
        suite_path,
        threshold=1.0,
    )
    assert passing.passed is True
    assert passing.dimension_rates == {"completeness": 1.0}


def test_mostly_does_not_dilute_record_validity_rate(tmp_path: Any) -> None:
    """Regression (A3): ``mostly: 0.95`` passes the assertion on 4% invalid rows,
    but the per-record validity rate (~0.96) still breaches a 0.99 contract."""
    suite_path = _write_suite(
        tmp_path,
        "mostly_regression_suite",
        [
            _exp(
                "expect_column_values_to_be_in_set",
                column="status",
                value_set=["confirmed"],
                mostly=0.95,
            ),
        ],
    )
    records = [{"status": "cancelled" if i < 4 else "confirmed"} for i in range(100)]

    evaluation = ExpectationEngineAdapter().evaluate_suite(records, suite_path, threshold=0.95)

    # The assertion verdict honours mostly (4% invalid <= 5% tolerated)...
    assert evaluation.passed is True
    assert evaluation.pass_rate == 1.0
    # ...but the record-quality rate counts the 4 invalid rows out of 100.
    assert evaluation.dimension_rates["validity"] == pytest.approx(0.96)
    verdict = assess_gate(evaluation, {"validity": 0.99}, 0.95)
    assert verdict.approved is False
    assert verdict.dimension == "validity"
    assert verdict.rate == pytest.approx(0.96)
    assert verdict.required == 0.99


# ---------------------------------------------------------------------------
# Domain: extract_quality_thresholds
# ---------------------------------------------------------------------------


def _contract_with_thresholds(thresholds: Any) -> dict[str, Any]:
    return {"spec": {"quality": {"thresholds": thresholds}}}


def test_extract_quality_thresholds_full_block() -> None:
    contract = _contract_with_thresholds({"completeness": 0.98, "uniqueness": 1.0, "validity": 0.99})

    assert extract_quality_thresholds(contract) == {"completeness": 0.98, "uniqueness": 1.0, "validity": 0.99}


def test_extract_quality_thresholds_partial_and_unknown_keys() -> None:
    contract = _contract_with_thresholds({"completeness": 1, "freshness": 0.5})

    assert extract_quality_thresholds(contract) == {"completeness": 1.0}


def test_extract_quality_thresholds_missing_blocks() -> None:
    assert extract_quality_thresholds({}) is None
    assert extract_quality_thresholds({"spec": {}}) is None
    assert extract_quality_thresholds({"spec": {"quality": {}}}) is None
    assert extract_quality_thresholds({"spec": {"quality": {"thresholds": {"freshness": 0.5}}}}) is None
    assert extract_quality_thresholds({"spec": {"quality": {"thresholds": {}}}}) is None
    assert extract_quality_thresholds({"spec": "quality"}) is None


def test_extract_quality_thresholds_invalid_values_raise() -> None:
    """Misconfigured governance fails loudly (fail-closed) instead of
    silently degrading to the global fallback threshold."""
    import pytest

    with pytest.raises(ValueError, match="completeness"):
        extract_quality_thresholds(_contract_with_thresholds({"completeness": 1.5}))
    with pytest.raises(ValueError, match="completeness"):
        extract_quality_thresholds(_contract_with_thresholds({"completeness": -0.1}))
    with pytest.raises(ValueError, match="completeness"):
        extract_quality_thresholds(_contract_with_thresholds({"completeness": "high"}))
    with pytest.raises(ValueError, match="completeness"):
        extract_quality_thresholds(_contract_with_thresholds({"completeness": None}))
    with pytest.raises(ValueError, match="completeness"):
        extract_quality_thresholds(_contract_with_thresholds({"completeness": True}))
    with pytest.raises(ValueError, match="validity"):
        extract_quality_thresholds(_contract_with_thresholds({"validity": float("nan")}))
    with pytest.raises(ValueError, match="mapping"):
        extract_quality_thresholds({"spec": {"quality": {"thresholds": "high"}}})
