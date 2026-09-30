"""
Modern Data Architecture POC - Domain Exceptions.
Fail-closed error types raised by the application core when quality suites,
expectations, or promotion rules are violated.
Pure domain logic with no external infrastructure dependencies (RF-06).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids a runtime import cycle
    from src.domain.entities import QualityGateEvaluation


class SuiteNotFoundError(Exception):
    """Raised when a quality expectation suite file is missing (fail-closed, RF-06).

    A missing suite must never be treated as an implicit pass: the quality gate
    aborts loudly instead of silently promoting unvalidated data. It deliberately
    does NOT derive from ``FileNotFoundError``/``OSError``: suite resolution is a
    governance-level failure, not a filesystem condition callers should catch by
    accident, and the core holds no filesystem vocabulary.
    """


class QualityGateBreachError(ValueError):
    """Raised when a quality gate fails and promotion must be blocked (RF-06).

    Carries the measured ``pass_rate`` and the required ``threshold`` so callers
    can report why the gate was breached without re-running evaluation, plus the
    full ``evaluation`` when available so CLIs can still render results tables,
    emit FAIL lineage and persist reports before exiting non-zero.
    """

    pass_rate: float
    threshold: float
    evaluation: QualityGateEvaluation | None

    def __init__(
        self,
        message: str,
        pass_rate: float,
        threshold: float,
        evaluation: QualityGateEvaluation | None = None,
    ) -> None:
        super().__init__(message)
        self.pass_rate = pass_rate
        self.threshold = threshold
        self.evaluation = evaluation
