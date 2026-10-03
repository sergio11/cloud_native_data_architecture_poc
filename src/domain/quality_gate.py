"""
Modern Data Architecture POC - Shared fail-closed quality gate policy.

Pure domain policy for the Silver, Gold and standalone quality-gate paths
(RF-06): :func:`assess_gate` evaluates an engine result against the global
pass-rate threshold plus (when provided) per-dimension contract thresholds and
returns a :class:`GateVerdict`. It performs no I/O and mutates no state — its
only observable output besides the verdict is a warning log when declared
contract dimensions are not measured by the suite. Application concerns
(emitting lineage events, raising ``QualityGateBreachError``, resolving
contracts from a catalog) live in the shared application collaborator
(``src.use_cases.gate_support``), never here, so this module depends only on
domain entities and the standard library.

Contract thresholds come from the agreed ``extract_quality_thresholds``
domain API; per-dimension pass ratios come from the evaluation's
``dimension_rates`` attribute filled by the engine.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from src.domain.entities import QualityGateEvaluation

logger = logging.getLogger("domain.quality_gate")

#: Fallback global threshold when neither the caller nor a contract says otherwise.
DEFAULT_QUALITY_THRESHOLD = 0.95

#: Rule name the quality engine assigns to the fail-closed "no records /
#: no assertions to evaluate" failure (RF-06). Part of the
#: ``QualityEnginePort`` contract so drivers can recognise the empty-dataset
#: condition without importing a concrete adapter.
FAIL_CLOSED_RULE = "fail_closed"


@dataclass(frozen=True)
class GateVerdict:
    """Pure outcome of the gate policy assessment (no I/O performed).

    ``approved`` is the policy decision. On rejection, ``rate`` carries the
    measured rate relevant to the breach and ``required`` the threshold it
    failed against, so the application layer can report and raise without
    re-running the evaluation.
    """

    approved: bool
    reason: str
    dimension: str | None = None
    rate: float | None = None
    required: float | None = None


def assess_gate(
    evaluation: QualityGateEvaluation,
    contract_thresholds: dict[str, float] | None,
    threshold: float,
) -> GateVerdict:
    """Assesses the gate verdict performing no I/O and mutating no state.

    The only side effect is a warning log when contract thresholds are
    declared but none of those dimensions is measured by the suite. A missing
    suite raises SuiteNotFoundError from the engine and propagates
    (never a silent pass). When ``contract_thresholds`` are declared and the
    evaluation measures at least one of those dimensions (via its
    engine-filled ``dimension_rates``), the contract dimensions govern the
    verdict: any measured ``dimension_rates[d] < contract[d]`` rejects, naming
    the dimension, and the global threshold arithmetic is ignored. An engine
    fail-closed veto (``evaluation.unsupported``) always rejects, even when
    the contract dimensions pass. Contract dimensions the suite does not
    measure are skipped - the engine only reports measured dimensions. When no
    declared dimension is measurable the global ``pass_rate >= threshold``
    rule applies as a fallback.
    """
    rates: dict[str, Any] = evaluation.dimension_rates

    if contract_thresholds:
        measurable = {dim: required for dim, required in contract_thresholds.items() if dim in rates}
        if measurable:
            for dimension, required in measurable.items():
                rate = rates[dimension]
                if rate < required:
                    return GateVerdict(
                        approved=False,
                        reason=(f"dimension '{dimension}' rate {rate:.4f} < contract threshold {required:.2f}"),
                        dimension=dimension,
                        rate=rate,
                        required=required,
                    )
            # Contract dimensions govern: the global arithmetic is ignored once
            # they all pass - unless the engine vetoed fail-closed (unsupported
            # expectation types still block even when the arithmetic passes).
            # The veto is read from the explicit ``unsupported`` flag: a
            # ``pass_rate < threshold`` heuristic could not distinguish an
            # ignorable arithmetic shortfall from a veto and silently approved
            # the latter.
            if evaluation.unsupported:
                return GateVerdict(
                    approved=False,
                    reason=(
                        "engine fail-closed veto despite contract dimensions passing "
                        f"(pass rate: {evaluation.pass_rate:.2f})"
                    ),
                    rate=evaluation.pass_rate,
                    required=threshold,
                )
            return GateVerdict(
                approved=True,
                reason=(f"contract dimensions {sorted(measurable)} govern (global threshold ignored)"),
            )
        logger.warning(
            "Contract thresholds %s declared for '%s' but the suite measures none of those "
            "dimensions; falling back to the global threshold (%.2f).",
            contract_thresholds,
            evaluation.table_name,
            threshold,
        )

    if not evaluation.passed:
        return GateVerdict(
            approved=False,
            reason=f"pass rate {evaluation.pass_rate:.2f} < threshold {threshold:.2f}",
            rate=evaluation.pass_rate,
            required=threshold,
        )
    return GateVerdict(
        approved=True,
        reason=f"pass rate {evaluation.pass_rate:.2f} >= threshold {threshold:.2f}",
    )
