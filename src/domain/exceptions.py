"""
Modern Data Architecture POC - Domain Exceptions.
Fail-closed error types raised by the application core when quality suites,
expectations, or promotion rules are violated.
Pure domain logic with no external infrastructure dependencies (RF-06).
"""

from __future__ import annotations


class SuiteNotFoundError(FileNotFoundError):
    """Raised when a quality expectation suite file is missing (fail-closed, RF-06).

    A missing suite must never be treated as an implicit pass: the quality gate
    aborts loudly instead of silently promoting unvalidated data.
    """


class QualityGateBreachError(ValueError):
    """Raised when a quality gate fails and promotion must be blocked (RF-06).

    Carries the measured ``pass_rate`` and the required ``threshold`` so callers
    can report why the gate was breached without re-running evaluation.
    """

    pass_rate: float
    threshold: float

    def __init__(self, message: str, pass_rate: float, threshold: float) -> None:
        super().__init__(message)
        self.pass_rate = pass_rate
        self.threshold = threshold
