"""W8 fail-closed coverage: guards whose failure paths no other test exercises.

Covers empty contract discovery and both failure paths of the Marquez lineage
adapter's ``emit_job_event``.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.adapters.lineage.marquez_lineage import MarquezLineageAdapter
from src.use_cases.validate_contract import ValidateContractUseCase


# ---------------------------------------------------------------------------
# 1. Empty-discovery guard (contract validation)
# ---------------------------------------------------------------------------
class _CatalogStub:
    """Minimal DomainCatalogPort stub with configurable discovery results."""

    def __init__(
        self,
        products: list[tuple[str, dict[str, Any]]] | None = None,
        contracts: list[tuple[str, dict[str, Any]]] | None = None,
        schemas: list[tuple[str, dict[str, Any]]] | None = None,
    ) -> None:
        self._products = products or []
        self._contracts = contracts or []
        self._schemas = schemas or []

    def list_products(self) -> list[tuple[str, dict[str, Any]]]:
        return list(self._products)

    def list_contracts(self) -> list[tuple[str, dict[str, Any]]]:
        return list(self._contracts)

    def list_schemas(self) -> list[tuple[str, dict[str, Any]]]:
        return list(self._schemas)

    def load_governance_schema(self, schema_filename: str) -> dict[str, Any] | None:
        return None


def test_empty_discovery_fails_closed_with_discovery_error() -> None:
    """No products AND no contracts => passed=False + Discovery error."""
    results = ValidateContractUseCase(catalog=_CatalogStub()).execute()

    assert results["passed"] is False
    assert results["products_evaluated"] == 0
    assert results["contracts_evaluated"] == 0
    discovery = [entry for entry in results["errors"] if entry["kind"] == "Discovery"]
    assert len(discovery) == 1, f"expected exactly one Discovery error, got {results['errors']}"
    assert "No data product manifests or data contracts discovered" in " ".join(discovery[0]["errors"])
    assert discovery[0]["file"] == "domains"


def test_discovery_guard_only_applies_when_both_sides_are_empty() -> None:
    """A products-only catalog proceeds past the guard (contrast case)."""
    product = {
        "metadata": {"name": "alpha", "domain": "sales", "owner": "team@x"},
        "spec": {"classification": "internal", "assets": [{"name": "a"}]},
    }
    catalog = _CatalogStub(products=[("domains/sales/data-products/alpha/product.yaml", product)])

    results = ValidateContractUseCase(catalog=catalog).execute()

    kinds = {entry["kind"] for entry in results["errors"]}
    assert "Discovery" not in kinds, "guard must require BOTH products and contracts empty"
    assert results["products_evaluated"] == 1


# ---------------------------------------------------------------------------
# 2. Marquez lineage adapter failure paths
# ---------------------------------------------------------------------------
def _adapter(tmp_path: Path) -> MarquezLineageAdapter:
    return MarquezLineageAdapter(
        marquez_url="http://127.0.0.1:1",
        audit_file_path=str(tmp_path / "lineage.jsonl"),
    )


def test_emit_returns_false_when_audit_write_fails(tmp_path: Path) -> None:
    """Audit trail unwritable => False (fail-closed), even when HTTP is best effort.

    The audit path points AT A DIRECTORY so ``open(..., "a")`` always raises;
    the conftest's raising ``requests.post`` stub covers the HTTP side.
    """
    blocked = tmp_path / "audit_blocked"
    blocked.mkdir()
    adapter = MarquezLineageAdapter(
        marquez_url="http://127.0.0.1:1",
        audit_file_path=str(blocked),
    )

    emitted = adapter.emit_job_event(job_name="w8_audit_failure", inputs=["silver.sales.orders"], outputs=["gold.x"])

    assert emitted is False


def test_emit_with_http_500_still_returns_true_when_audit_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HTTP non-2xx + successful audit => True (local audit is source of truth).

    Deliberate divergence from a naive "any failure => False" reading: the
    adapter docstring promises the return value mirrors the AUDIT write, with
    the Marquez POST as best effort. Asserting False here would contradict the
    shipped contract.
    """
    adapter = _adapter(tmp_path)
    monkeypatch.setattr(
        "src.adapters.lineage.marquez_lineage.requests.post",
        lambda *args, **kwargs: SimpleNamespace(status_code=500, text="Internal Server Error"),
    )

    emitted = adapter.emit_job_event(job_name="w8_http_500", inputs=["silver.sales.orders"], outputs=["gold.x"])

    assert emitted is True
    audit_lines = (tmp_path / "lineage.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(audit_lines) == 1
    assert "w8_http_500" in audit_lines[0]


def test_emit_returns_false_when_audit_fails_and_http_returns_500(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both channels unhappy => False (the fail-closed case)."""
    blocked = tmp_path / "audit_blocked"
    blocked.mkdir()
    adapter = MarquezLineageAdapter(
        marquez_url="http://127.0.0.1:1",
        audit_file_path=str(blocked),
    )
    monkeypatch.setattr(
        "src.adapters.lineage.marquez_lineage.requests.post",
        lambda *args, **kwargs: SimpleNamespace(status_code=500, text="Internal Server Error"),
    )

    emitted = adapter.emit_job_event(job_name="w8_audit_and_http_fail", inputs=["in"], outputs=["out"])

    assert emitted is False
