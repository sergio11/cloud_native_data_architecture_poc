"""Tests for the Iceberg lakehouse adapter.

The catalog is a double, keeping the tests hermetic while exercising
idempotent writes, time travel, ordering and identifier mapping.
"""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from src.adapters.storage.iceberg_storage import IcebergLakehouseStorageAdapter
from src.domain.naming import iceberg_identifier


# ---------------------------------------------------------------------------
# A minimal pyarrow stand-in.
#
# The adapter only needs pa.schema/pa.field/pa.Table.from_pydict/pa.string and
# friends. Installing pyarrow for the unit tests would make them heavy and
# platform-dependent (it has no wheel for every interpreter), and the adapter
# itself is written against a narrow slice of the API, so a small stand-in
# exercises exactly the same code paths.
# ---------------------------------------------------------------------------
def _install_fake_pyarrow(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Field:
        def __init__(self, name: str, type_: Any, nullable: bool = True) -> None:
            self.name = name
            self.type = type_
            self.nullable = nullable

    class _Type:
        def __init__(self, kind: str) -> None:
            self.kind = kind

        def __call__(self) -> _Type:
            # pyarrow exposes the types as functions: pa.string() is a DataType.
            return self

        def __eq__(self, other: Any) -> bool:
            return isinstance(other, _Type) and other.kind == self.kind

        def __hash__(self) -> int:
            return hash(self.kind)

    class _Types:
        string = _Type("string")
        boolean = _Type("boolean")
        int64 = _Type("int64")
        float64 = _Type("float64")
        date32 = _Type("date32")
        timestamp = _Type("timestamp")

        @staticmethod
        def is_string(t: Any) -> bool:
            return t == _Types.string

        @staticmethod
        def is_boolean(t: Any) -> bool:
            return t == _Types.boolean

        @staticmethod
        def is_floating(t: Any) -> bool:
            return t == _Types.float64

        @staticmethod
        def is_integer(t: Any) -> bool:
            return t == _Types.int64

        @staticmethod
        def is_date(t: Any) -> bool:
            return t == _Types.date32

        @staticmethod
        def is_timestamp(t: Any) -> bool:
            return t == _Types.timestamp

    class _Column:
        def __init__(self, values: list[Any]) -> None:
            self._values = values

        def to_pylist(self) -> list[Any]:
            return list(self._values)

        def __getitem__(self, index: int) -> Any:
            return _Value(self._values[index])

    class _Value:
        def __init__(self, value: Any) -> None:
            self._value = value

        def as_py(self) -> Any:
            return self._value

    class _Table:
        def __init__(self, data: dict[str, list[Any]], schema: Any) -> None:
            self._data = data
            self.schema = schema
            self.num_rows = len(next(iter(data.values()), []))

        @property
        def column_names(self) -> list[str]:
            return list(self._data.keys())

        def column(self, name: str) -> _Column:
            return _Column(self._data.get(name, []))

        def to_pylist(self) -> list[dict[str, Any]]:
            names = self.column_names
            return [{name: self._data[name][index] for name in names} for index in range(self.num_rows)]

        def take(self, indices: list[int]) -> _Table:
            data = {name: [values[index] for index in indices] for name, values in self._data.items()}
            return _Table(data, self.schema)

    class _Schema:
        def __init__(self, fields: list[_Field]) -> None:
            self._fields = fields

        def fields(self) -> list[_Field]:
            return self._fields

        def __iter__(self) -> Any:
            # A pyarrow Schema is iterable and yields its fields.
            return iter(self._fields)

    module = types.ModuleType("pyarrow")
    module.field = _Field  # type: ignore[attr-defined]
    module.schema = _Schema  # type: ignore[attr-defined]
    module.string = _Types.string  # type: ignore[attr-defined]
    module.bool_ = _Types.boolean  # type: ignore[attr-defined]
    module.int64 = _Types.int64  # type: ignore[attr-defined]
    module.float64 = _Types.float64  # type: ignore[attr-defined]
    module.date32 = _Types.date32  # type: ignore[attr-defined]
    module.timestamp = _Types.timestamp  # type: ignore[attr-defined]
    module.types = _Types  # type: ignore[attr-defined]
    module.Table = types.SimpleNamespace(from_pydict=lambda data, schema: _Table(data, schema))  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyarrow", module)


# ---------------------------------------------------------------------------
# The catalog double
# ---------------------------------------------------------------------------
class _Snapshot:
    def __init__(self, snapshot_id: int, rows: list[dict[str, Any]], timestamp_ms: int) -> None:
        self.snapshot_id = snapshot_id
        self.timestamp_ms = timestamp_ms
        self.summary = {"total-records": str(len(rows))}
        self._rows = rows


class _FakeMetadata:
    def __init__(self, snapshots: list[_Snapshot]) -> None:
        self.snapshots = snapshots


class _FakeTable:
    def __init__(self, identifier: str, schema: Any) -> None:
        self.identifier = identifier
        self._schema = schema
        self.snapshots: list[_Snapshot] = []
        self._rows: list[dict[str, Any]] = []
        self._next_id = 1000

    @property
    def metadata(self) -> _FakeMetadata:
        return _FakeMetadata(self.snapshots)

    def schema(self) -> Any:
        return self._schema

    def scan(self, snapshot_id: int | None = None) -> Any:
        if snapshot_id is not None:
            return _FakeScan(self._snapshot_rows(snapshot_id), self._schema, self)
        return _FakeScan(self._rows, self._schema, self)

    def _snapshot_rows(self, snapshot_id: int) -> list[dict[str, Any]]:
        for snapshot in self.snapshots:
            if snapshot.snapshot_id == snapshot_id:
                return snapshot._rows
        raise AssertionError(f"no snapshot {snapshot_id}")

    def append(self, payload: Any) -> None:
        # Iceberg's append ADDS to the table: the new snapshot holds the old
        # rows plus the new ones. Overwrite REPLACES them.
        self._commit(list(self._rows) + list(payload.to_pylist()))

    def overwrite(self, payload: Any) -> None:
        self._commit(list(payload.to_pylist()))

    def _commit(self, rows: list[dict[str, Any]]) -> None:
        # Iceberg's model: a write creates ONE new snapshot that holds the new
        # content, and the previous snapshot keeps the rows it had. That is what
        # makes time travel work, and it is what the adapter relies on.
        self._next_id += 1
        self.snapshots.append(_Snapshot(self._next_id, list(rows), 1_700_000_000_000 + self._next_id))
        self._rows = rows

    def update_schema(self) -> Any:
        return _FakeSchemaUpdate(self)


class _FakeScan:
    def __init__(self, rows: list[dict[str, Any]], schema: Any, table: _FakeTable | None = None) -> None:
        self._rows = rows
        self._schema = schema
        self._table = table

    def snapshot_id(self, snapshot_id: int) -> _FakeScan:
        # Reading AT a snapshot: the rows that snapshot held, not the live ones.
        assert self._table is not None, "the scan needs the table to resolve a snapshot"
        return _FakeScan(self._table._snapshot_rows(snapshot_id), self._schema, self._table)

    def to_arrow(self) -> Any:
        import pyarrow

        names: list[str] = []
        for row in self._rows:
            for key in row:
                if key not in names:
                    names.append(key)
        data = {name: [row.get(name) for row in self._rows] for name in names}
        return pyarrow.Table.from_pydict(data, schema=self._schema)


class _FakeSchemaUpdate:
    def __init__(self, table: _FakeTable) -> None:
        self._table = table
        self._pending: list[Any] = []

    def add_column(self, name: str, type_: Any) -> None:
        # PyIceberg's UpdateSchema.add_column takes an Iceberg type name and
        # has no `nullable` argument: the column it adds is always optional.
        self._pending.append((name, type_, True))

    def commit(self) -> None:
        # In pyarrow, Schema.fields is a PROPERTY, not a method.
        fields = list(self._table.schema().fields)
        for name, type_, _nullable in self._pending:
            fields.append(_FakeField(name, type_))
        self._table._schema = _FakeSchema(fields)

    def __enter__(self) -> _FakeSchemaUpdate:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


class _FakeField:
    def __init__(self, name: str, type_: Any, required: bool = False) -> None:
        self.name = name
        self.type = type_
        # The Iceberg side of a field: `required` is what the catalogue
        # declared, and the write has to agree with it.
        self.required = required


class _FakeSchema:
    def __init__(self, fields: list[_FakeField]) -> None:
        self._fields = fields

    @property
    def fields(self) -> list[_FakeField]:
        # In pyarrow, Schema.fields is a PROPERTY.
        return self._fields

    def __iter__(self) -> Any:
        return iter(self._fields)


class _FakeCatalog:
    def __init__(self) -> None:
        self.namespaces: set[str] = set()
        self.tables: dict[str, _FakeTable] = {}

    def namespace_exists(self, namespace: str) -> bool:
        return namespace in self.namespaces

    def create_namespace(self, namespace: str) -> None:
        self.namespaces.add(namespace)

    def table_exists(self, identifier: str) -> bool:
        return identifier in self.tables

    def create_table(self, identifier: str, schema: Any, properties: Any = None) -> _FakeTable:
        table = _FakeTable(identifier, _FakeSchema([_FakeField(f.name, f.type) for f in schema.fields()]))
        self.tables[identifier] = table
        return table

    def load_table(self, identifier: str) -> _FakeTable:
        return self.tables[identifier]


@pytest.fixture
def adapter(monkeypatch: pytest.MonkeyPatch) -> IcebergLakehouseStorageAdapter:
    _install_fake_pyarrow(monkeypatch)
    return IcebergLakehouseStorageAdapter(catalog=_FakeCatalog())


# ---------------------------------------------------------------------------
# The identifier mapping
# ---------------------------------------------------------------------------
def test_published_tiers_drop_the_tier() -> None:
    # These are the names the serving layer and the product manifests use.
    assert iceberg_identifier("gold.sales.daily_metrics") == ("sales", "daily_metrics")
    assert iceberg_identifier("stream.sales.orders_windowed") == ("sales", "orders_windowed")


def test_internal_tiers_keep_the_tier() -> None:
    # Otherwise Bronze and Silver would collide on the same name.
    assert iceberg_identifier("bronze.sales.orders") == ("sales", "bronze_orders")
    assert iceberg_identifier("silver.sales.orders") == ("sales", "silver_orders")


def test_identifier_rejects_a_malformed_logical_name() -> None:
    with pytest.raises(ValueError, match="must be '<tier>.<domain>.<table>'"):
        iceberg_identifier("sales.orders")


# ---------------------------------------------------------------------------
# The port contract
# ---------------------------------------------------------------------------
def _orders(rows: int = 3, start: int = 0) -> list[dict[str, Any]]:
    return [
        {
            "order_id": f"O-{index}",
            "status": "confirmed",
            "total_amount": 10.0 + index,
            "order_date": "2026-10-18",
        }
        for index in range(start, start + rows)
    ]


def test_write_creates_the_table_and_returns_the_row_count(adapter: IcebergLakehouseStorageAdapter) -> None:
    assert adapter.write_table("gold.sales.daily_metrics", _orders()) == 3
    assert adapter.table_exists("gold.sales.daily_metrics")
    assert adapter.get_table_schema("gold.sales.daily_metrics") == [
        "order_id",
        "status",
        "total_amount",
        "order_date",
    ]


def test_read_returns_the_rows_that_were_written(adapter: IcebergLakehouseStorageAdapter) -> None:
    adapter.write_table("gold.sales.daily_metrics", _orders())
    rows = adapter.read_table("gold.sales.daily_metrics")
    assert len(rows) == 3
    assert rows[0]["order_id"] == "O-0"
    assert rows[0]["total_amount"] == 10.0


def test_read_strips_the_internal_columns(adapter: IcebergLakehouseStorageAdapter) -> None:
    adapter.write_table("gold.sales.daily_metrics", _orders())
    row = adapter.read_table("gold.sales.daily_metrics")[0]
    assert "_content_hash" not in row
    assert "_row_seq" not in row


def test_re_writing_the_same_batch_writes_nothing(adapter: IcebergLakehouseStorageAdapter) -> None:
    """Re-executing identical content must not duplicate rows."""
    batch = _orders()
    assert adapter.write_table("gold.sales.daily_metrics", batch) == 3
    assert adapter.write_table("gold.sales.daily_metrics", batch) == 0
    assert len(adapter.read_table("gold.sales.daily_metrics")) == 3


def test_a_different_batch_is_appended(adapter: IcebergLakehouseStorageAdapter) -> None:
    adapter.write_table("gold.sales.daily_metrics", _orders(3))
    other = _orders(5, start=100)
    assert adapter.write_table("gold.sales.daily_metrics", other) == 5
    assert len(adapter.read_table("gold.sales.daily_metrics")) == 8


def test_overwrite_replaces_the_live_content(adapter: IcebergLakehouseStorageAdapter) -> None:
    adapter.write_table("gold.sales.daily_metrics", _orders(3))
    assert adapter.write_table("gold.sales.daily_metrics", _orders(2), mode="overwrite") == 2
    assert len(adapter.read_table("gold.sales.daily_metrics")) == 2


def test_the_previous_snapshot_is_still_readable(adapter: IcebergLakehouseStorageAdapter) -> None:
    """The prior snapshot stays readable, that is time travel."""
    adapter.write_table("gold.sales.daily_metrics", _orders(3))
    first = adapter.list_versions("gold.sales.daily_metrics")[0]["snapshot_version"]
    adapter.write_table("gold.sales.daily_metrics", _orders(2), mode="overwrite")
    assert len(adapter.read_table("gold.sales.daily_metrics", version=first)) == 3


def test_list_versions_reports_every_snapshot(adapter: IcebergLakehouseStorageAdapter) -> None:
    adapter.write_table("gold.sales.daily_metrics", _orders(3))
    adapter.write_table("gold.sales.daily_metrics", _orders(4, start=100))
    versions = adapter.list_versions("gold.sales.daily_metrics")
    assert len(versions) == 2  # one snapshot per write, as Iceberg does
    assert versions == sorted(versions, key=lambda item: item["snapshot_version"])
    assert set(versions[0]) == {"snapshot_version", "row_count", "created_at"}
    assert versions[0]["row_count"] == 3
    # The second snapshot holds the table AFTER the append: 3 + 4 rows.
    assert versions[1]["row_count"] == 7


def test_rows_come_back_in_insertion_order(adapter: IcebergLakehouseStorageAdapter) -> None:
    """The port promises oldest write first, and limit applies after that."""
    adapter.write_table("gold.sales.daily_metrics", _orders(3))
    adapter.write_table("gold.sales.daily_metrics", _orders(5, start=100))
    rows = adapter.read_table("gold.sales.daily_metrics")
    assert [row["order_id"] for row in rows] == [f"O-{index}" for index in range(3)] + [
        f"O-{index}" for index in range(100, 105)
    ]


def test_limit_is_applied_after_the_ordering(adapter: IcebergLakehouseStorageAdapter) -> None:
    adapter.write_table("gold.sales.daily_metrics", _orders(5))
    rows = adapter.read_table("gold.sales.daily_metrics", limit=2)
    assert [row["order_id"] for row in rows] == ["O-0", "O-1"]


def test_reading_a_missing_table_returns_nothing(adapter: IcebergLakehouseStorageAdapter) -> None:
    assert adapter.read_table("gold.sales.daily_metrics") == []
    assert adapter.table_exists("gold.sales.daily_metrics") is False
    assert adapter.list_versions("gold.sales.daily_metrics") == []


def test_an_unsupported_write_mode_is_rejected(adapter: IcebergLakehouseStorageAdapter) -> None:
    with pytest.raises(ValueError, match="Unsupported write mode"):
        adapter.write_table("gold.sales.daily_metrics", _orders(), mode="upsert")


def test_overwrite_with_an_empty_set_clears_the_table(adapter: IcebergLakehouseStorageAdapter) -> None:
    """A replayed empty log must clear the table, not leave stale rows."""
    adapter.write_table("gold.sales.daily_metrics", _orders(3))
    assert adapter.write_table("gold.sales.daily_metrics", [], mode="overwrite") == 0
    assert adapter.read_table("gold.sales.daily_metrics") == []


def test_writing_to_a_table_created_outside_the_adapter_evolves_it(
    adapter: IcebergLakehouseStorageAdapter,
) -> None:
    """The catalogue registers published tables with the business schema only.

    The adapter evolves them to add the internal audit columns.
    """
    catalog = adapter._catalog  # noqa: SLF001 - the double is the test's own
    catalog.namespaces.add("sales")
    catalog.tables["sales.daily_metrics"] = _FakeTable(
        "sales.daily_metrics",
        _FakeSchema(
            [
                _FakeField("order_id", "string"),
                _FakeField("status", "string"),
                _FakeField("total_amount", "double"),
                _FakeField("order_date", "string"),
            ]
        ),
    )

    assert adapter.write_table("gold.sales.daily_metrics", _orders(2)) == 2
    names = {field.name for field in catalog.tables["sales.daily_metrics"].schema().fields}
    assert {"_content_hash", "_row_seq"} <= names
    assert len(adapter.read_table("gold.sales.daily_metrics")) == 2
