"""Modern Data Architecture POC - Shared fail-closed quality gate policy.

:func:`assess_gate` evaluates an engine result against the global threshold
and optional per-dimension contract thresholds.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from src.domain.entities import QualityGateEvaluation

logger = logging.getLogger("domain.quality_gate")

#: Fallback global threshold when neither the caller nor a contract says otherwise.
DEFAULT_QUALITY_THRESHOLD = 0.95

#: Rule name for the fail-closed "no records / no assertions" failure.
FAIL_CLOSED_RULE = "fail_closed"


@dataclass(frozen=True)
class GateVerdict:
    """Pure outcome of the gate policy assessment.

    On rejection, ``rate`` and ``required`` describe the breached threshold.
    """

    approved: bool
    reason: str
    dimension: str | None = None
    rate: float | None = None
    required: float | None = None
    # Effective threshold applied per measured dimension.
    thresholds: dict[str, float] = field(default_factory=dict)


def assess_gate(
    evaluation: QualityGateEvaluation,
    contract_thresholds: dict[str, float] | None,
    threshold: float,
) -> GateVerdict:
    """Assesses the gate verdict with no I/O and no state mutation.

    Measured contract dimensions govern; the global threshold is the fallback.
    An engine fail-closed veto always rejects.
    """
    rates: dict[str, Any] = evaluation.dimension_rates

    if contract_thresholds:
        measurable = {dim: required for dim, required in contract_thresholds.items() if dim in rates}
        if measurable:
            effective: dict[str, float] = dict(measurable)
            for dimension, required in measurable.items():
                rate = rates[dimension]
                if rate < required:
                    return GateVerdict(
                        approved=False,
                        reason=(f"dimension '{dimension}' rate {rate:.4f} < contract threshold {required:.2f}"),
                        dimension=dimension,
                        rate=rate,
                        required=required,
                        thresholds=effective,
                    )
            # Contract dimensions govern; the engine veto still blocks.
            if evaluation.unsupported:
                return GateVerdict(
                    approved=False,
                    reason=(
                        "engine fail-closed veto despite contract dimensions passing "
                        f"(pass rate: {evaluation.pass_rate:.2f})"
                    ),
                    rate=evaluation.pass_rate,
                    required=threshold,
                    thresholds=effective,
                )
            return GateVerdict(
                approved=True,
                reason=(f"contract dimensions {sorted(measurable)} govern (global threshold ignored)"),
                thresholds=effective,
            )
        logger.warning(
            "Contract thresholds %s declared for '%s' but the suite measures none of those "
            "dimensions; falling back to the global threshold (%.2f).",
            contract_thresholds,
            evaluation.table_name,
            threshold,
        )

    fallback: dict[str, float] = {dimension: threshold for dimension in rates}
    if not evaluation.passed:
        return GateVerdict(
            approved=False,
            reason=f"pass rate {evaluation.pass_rate:.2f} < threshold {threshold:.2f}",
            rate=evaluation.pass_rate,
            required=threshold,
            thresholds=fallback,
        )
    return GateVerdict(
        approved=True,
        reason=f"pass rate {evaluation.pass_rate:.2f} >= threshold {threshold:.2f}",
        thresholds=fallback,
    )
