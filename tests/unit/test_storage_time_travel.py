"""
Modern Data Architecture POC - Unit Tests: Lakehouse Storage Time Travel.

Covers RF-01 (content-hash idempotent landing) and RF-07 (snapshot time travel,
controlled schema evolution, legacy table compatibility) for
``LakehouseStorageAdapter``. Every test owns its database path via ``tmp_path``.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest

from src.adapters.storage.lakehouse_storage import LakehouseStorageAdapter

INTERNAL_COLUMNS = ("_snapshot_time", "_row_version", "_row_superseded", "_content_hash")


@pytest.fixture
def storage(tmp_path: Path) -> LakehouseStorageAdapter:
    return LakehouseStorageAdapter(db_path=str(tmp_path / "lakehouse.db"))


def test_append_idempotent(storage: LakehouseStorageAdapter) -> None:
    records = [
        {"id": 1, "name": "alpha", "_batch_id": "batch-1"},
        {"id": 2, "name": "beta", "_batch_id": "batch-1"},
        {"id": 3, "name": "gamma", "_batch_id": "batch-1"},
    ]
    assert storage.write_table("bronze.demo.items", records, mode="append") == 3

    rows = storage.read_table("bronze.demo.items")
    assert len(rows) == 3
    # INTEGER affinity: native ints round-trip (no TEXT coercion).
    assert [row["id"] for row in rows] == [1, 2, 3]
    for row in rows:
        assert "_batch_id" in row
        assert "_ingested_at" not in row
        for column in INTERNAL_COLUMNS:
            assert column not in row

    rerun = [{**r, "_batch_id": "batch-2", "_ingested_at": "2026-01-01T00:00:00+00:00"} for r in records]
    assert storage.write_table("bronze.demo.items", rerun, mode="append") == 0

    rows = storage.read_table("bronze.demo.items")
    assert len(rows) == 3
    assert [row["id"] for row in rows] == [1, 2, 3]
    for row in rows:
        assert row["_batch_id"] == "batch-1"
        for column in INTERNAL_COLUMNS:
            assert column not in row

    versions = storage.list_versions("bronze.demo.items")
    assert len(versions) == 1
    assert versions[0]["snapshot_version"] == 1
    assert versions[0]["row_count"] == 3


def test_overwrite_supersedes_and_time_travel(storage: LakehouseStorageAdapter) -> None:
    original = [
        {"id": 1, "amount": 10},
        {"id": 2, "amount": 20},
        {"id": 3, "amount": 30},
    ]
    assert storage.write_table("silver.demo.orders", original, mode="append") == 3

    replacement = [{"id": 1, "amount": 99}, {"id": 4, "amount": 40}]
    assert storage.write_table("silver.demo.orders", replacement, mode="overwrite") == 2

    live = storage.read_table("silver.demo.orders")
    assert [row["id"] for row in live] == [1, 4]

    historical = storage.read_table("silver.demo.orders", version=1)
    assert [row["id"] for row in historical] == [1, 2, 3]
    assert [row["amount"] for row in historical] == [10, 20, 30]

    versions = storage.list_versions("silver.demo.orders")
    assert len(versions) >= 2
    assert [v["snapshot_version"] for v in versions] == [1, 2]
    assert [v["row_count"] for v in versions] == [3, 2]
    assert all(v["created_at"] for v in versions)

    assert storage.write_table("silver.demo.orders", replacement, mode="overwrite") == 0
    assert len(storage.list_versions("silver.demo.orders")) == 2
    assert len(storage.read_table("silver.demo.orders")) == 2
    assert len(storage.read_table("silver.demo.orders", version=1)) == 3


def test_overwrite_empty_set_clears_table_but_keeps_history(storage: LakehouseStorageAdapter) -> None:
    """Overwrite with zero records reconciles the table to empty (RF-10):
    live rows are superseded under a new row_count=0 snapshot, while prior
    versions stay readable. A missing table stays missing."""
    assert storage.write_table("silver.demo.orders", [{"id": 1}, {"id": 2}], mode="append") == 2

    assert storage.write_table("silver.demo.orders", [], mode="overwrite") == 0
    assert storage.read_table("silver.demo.orders") == []
    assert storage.read_table("silver.demo.orders", version=1) != []

    versions = storage.list_versions("silver.demo.orders")
    assert [v["snapshot_version"] for v in versions] == [1, 2]
    assert [v["row_count"] for v in versions] == [2, 0]

    assert storage.write_table("silver.demo.missing", [], mode="overwrite") == 0
    assert storage.table_exists("silver.demo.missing") is False
    assert storage.list_versions("silver.demo.missing") == []


def test_invalid_table_names_rejected_before_sql(storage: LakehouseStorageAdapter) -> None:
    """Table names reach f-string-interpolated SQL identifiers: anything
    outside the allowlist must fail before touching the database."""
    with pytest.raises(ValueError, match="Invalid table name"):
        storage.read_table('silver.demo."orders')
    with pytest.raises(ValueError, match="Invalid table name"):
        storage.write_table("silver.demo.orders; DROP TABLE x", [{"id": 1}], mode="append")
    with pytest.raises(ValueError, match="Invalid table name"):
        storage.table_exists("silver demo orders")
    # Dotted tier names remain valid.
    assert storage.table_exists("silver.demo.orders") is False


def test_concurrent_writes_produce_unique_sequential_versions(tmp_path: Path) -> None:
    """Regression (RF-07): two racing writers must never publish the same
    snapshot version (TOCTOU on SELECT MAX). BEGIN IMMEDIATE serializes them."""
    db_path = str(tmp_path / "lakehouse.db")
    seed = LakehouseStorageAdapter(db_path=db_path)
    assert seed.write_table("silver.demo.concurrent", [{"w": -1, "i": -1}], mode="append") == 1

    errors: list[BaseException] = []

    def worker(worker_id: int) -> None:
        try:
            adapter = LakehouseStorageAdapter(db_path=db_path)
            for i in range(5):
                adapter.write_table("silver.demo.concurrent", [{"w": worker_id, "i": i}], mode="append")
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)

    assert errors == []
    assert all(not thread.is_alive() for thread in threads)
    versions = LakehouseStorageAdapter(db_path=db_path).list_versions("silver.demo.concurrent")
    assert sorted(v["snapshot_version"] for v in versions) == list(range(1, 12))


def test_append_after_overwrite_time_travel(storage: LakehouseStorageAdapter) -> None:
    assert storage.write_table("silver.demo.orders", [{"id": 1}, {"id": 2}, {"id": 3}], mode="append") == 3
    assert storage.write_table("silver.demo.orders", [{"id": 1}, {"id": 2}], mode="overwrite") == 2
    assert storage.write_table("silver.demo.orders", [{"id": 9}], mode="append") == 1

    versions = storage.list_versions("silver.demo.orders")
    assert [v["snapshot_version"] for v in versions] == [1, 2, 3]
    latest = versions[-1]["snapshot_version"]
    assert latest == 3
    assert versions[-1]["row_count"] == 1

    assert [row["id"] for row in storage.read_table("silver.demo.orders")] == [1, 2, 9]
    assert [row["id"] for row in storage.read_table("silver.demo.orders", version=latest - 1)] == [1, 2]
    assert [row["id"] for row in storage.read_table("silver.demo.orders", version=1)] == [
        1,
        2,
        3,
    ]


def test_schema_evolution(storage: LakehouseStorageAdapter) -> None:
    assert storage.write_table("bronze.demo.wide", [{"id": 1, "name": "a"}]) == 1
    assert storage.write_table("bronze.demo.wide", [{"id": 2, "name": "b", "extra": "x"}]) == 1

    rows = storage.read_table("bronze.demo.wide")
    assert len(rows) == 2
    assert rows[1]["extra"] == "x"
    assert rows[0]["extra"] is None
    assert "extra" in storage.get_table_schema("bronze.demo.wide")
    assert len(storage.list_versions("bronze.demo.wide")) == 2


def test_legacy_table_compatibility(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    conn.execute('CREATE TABLE "legacy_demo" ("id" TEXT, "name" TEXT, _snapshot_time TEXT);')
    conn.execute(
        'INSERT INTO "legacy_demo" ("id", "name", _snapshot_time) '
        "VALUES ('1', 'alpha', '2024-01-01T00:00:00+00:00');"
    )
    conn.commit()
    conn.close()

    storage = LakehouseStorageAdapter(db_path=str(db_path))

    assert storage.write_table("legacy.demo", [{"id": 2, "name": "beta"}]) == 1

    live = storage.read_table("legacy.demo")
    assert [row["id"] for row in live] == ["1", "2"]
    for row in live:
        for column in INTERNAL_COLUMNS:
            assert column not in row

    assert [row["id"] for row in storage.read_table("legacy.demo", version=0)] == ["1"]
    assert [row["id"] for row in storage.read_table("legacy.demo", version=1)] == ["1", "2"]

    assert storage.write_table("legacy.demo", [{"id": 2, "name": "beta"}]) == 0
    versions = storage.list_versions("legacy.demo")
    assert len(versions) == 1
    assert versions[0]["snapshot_version"] == 1
    assert versions[0]["row_count"] == 1


def test_limit_and_deterministic_order(storage: LakehouseStorageAdapter) -> None:
    records = [{"id": index, "label": f"row-{index}"} for index in range(1, 6)]
    assert storage.write_table("bronze.demo.ordered", records, mode="append") == 5

    full = storage.read_table("bronze.demo.ordered")
    assert [row["id"] for row in full] == [1, 2, 3, 4, 5]

    first_three = storage.read_table("bronze.demo.ordered", limit=3)
    assert [row["id"] for row in first_three] == [1, 2, 3]
    assert first_three == full[:3]

    assert storage.read_table("bronze.demo.ordered", limit=10) == full
    assert storage.read_table("bronze.demo.ordered", limit=0) == []

    assert storage.read_table("bronze.demo.missing") == []
    assert storage.list_versions("bronze.demo.missing") == []
