"""Modern Data Architecture POC - Domain exceptions.

Fail-closed errors raised when quality suites or promotion rules are violated.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids a runtime import cycle
    from src.domain.entities import QualityGateEvaluation


class SuiteNotFoundError(Exception):
    """Raised when a quality expectation suite file is missing (fail-closed).

    A missing suite aborts the gate instead of silently promoting data.
    """


class QualityGateBreachError(ValueError):
    """Raised when a quality gate fails and promotion must be blocked.

    Carries the measured pass rate, required threshold and the evaluation.
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
