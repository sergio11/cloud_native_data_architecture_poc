"""Unit tests: DI container laziness and hermetic wiring (hexagonal conformance).

Hermetic: every Container under test is bound to ``tmp_path`` (lakehouse DB,
event log and lineage audit all land next to ``db_path``), the network is
disabled (Marquez POST raises instantly, Trino/Postgres backends stripped),
and the repository's ``data/`` directory is snapshotted before/after to prove
nothing is created or modified there.
"""

from __future__ import annotations

from pathlib import Path

from src.container import Container

REPO_ROOT = Path(__file__).resolve().parents[2]

USE_CASE_ATTRS = [
    "ingest_bronze",
    "cleanse_silver",
    "aggregate_gold",
    "process_stream",
    "query_serving",
    "validate_contract",
    "run_quality_gate",
]

ADAPTER_ATTRS = [
    "storage",
    "source",
    "stream_broker",
    "lineage",
    "quality_engine",
    "serving_sql",
    "domain_catalog",
]


def _data_snapshot() -> dict[str, int]:
    data_dir = REPO_ROOT / "data"
    if not data_dir.is_dir():
        return {}
    return {entry.name: entry.stat().st_mtime_ns for entry in data_dir.iterdir()}


def test_construction_is_side_effect_free(tmp_path: Path) -> None:
    """Instantiating Container (and importing src.container) creates no files."""
    before = _data_snapshot()
    Container(db_path=str(tmp_path / "lakehouse.db"), repo_root=str(tmp_path))
    assert list(tmp_path.iterdir()) == []
    assert _data_snapshot() == before


def test_all_use_case_attributes_exist(tmp_path: Path) -> None:
    """Every public adapter/use-case attribute resolves on first access."""
    container = Container(db_path=str(tmp_path / "lakehouse.db"), repo_root=str(tmp_path))
    for attr in ADAPTER_ATTRS:
        assert getattr(container, attr) is not None
    for attr in USE_CASE_ATTRS:
        assert getattr(container, attr) is not None


def test_minimal_ingest_flow_is_hermetic(tmp_path: Path) -> None:
    """ingest_bronze over the synthetic seeded source lands Bronze in tmp only."""
    before = _data_snapshot()
    container = Container(db_path=str(tmp_path / "lakehouse.db"), repo_root=str(tmp_path))

    result = container.ingest_bronze.execute("orders")

    assert result["status"] == "SUCCESS"
    assert result["rows_ingested"] > 0
    assert container.storage.table_exists("bronze.sales.orders")
    assert len(container.storage.read_table("bronze.sales.orders")) == result["rows_ingested"]
    # Nothing leaked into the repository's data/ dir.
    assert _data_snapshot() == before
    # All container artifacts live under tmp_path.
    assert set(tmp_path.iterdir()) != set()
    assert all(str(entry).startswith(str(tmp_path)) for entry in tmp_path.rglob("*"))
