"""Iceberg-backed lakehouse storage.

Second implementation of LakehouseStoragePort: Iceberg tables in a Polaris
REST catalog with data files in MinIO.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, date, datetime
from typing import Any

from src.domain.naming import iceberg_identifier

logger = logging.getLogger(__name__)

#: Columns the adapter owns. They are stripped on read, like the SQLite
#: adapter's internal versioning columns, so the application core never sees
#: them. The business audit columns (``_ingested_at``, ``_source``,
#: ``_batch_id``) are NOT internal and are kept.
INTERNAL_COLUMNS: tuple[str, ...] = ("_content_hash", "_row_seq")


class IcebergLakehouseStorageAdapter:
    """Lakehouse storage backed by Iceberg tables in a Polaris REST catalog.

    The catalog is injected so connection details stay in the composition root.
    """

    def __init__(self, catalog: Any, namespace_resolver: Any = None) -> None:
        self._catalog = catalog
        self._resolve = namespace_resolver or iceberg_identifier

    # Writing
    def write_table(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        mode: str = "append",
    ) -> int:
        """Writes records with content-hash idempotency and versioning.

        Append inserts only rows whose hash is new; overwrite replaces live
        content while keeping the previous snapshot readable.
        """
        if mode not in ("append", "overwrite"):
            raise ValueError(f"Unsupported write mode '{mode}'. Expected 'append' or 'overwrite'.")
        if not records and mode == "append":
            return 0

        namespace, table_id = self._resolve(table_name)
        self._ensure_namespace(namespace)

        if not records:
            # Overwrite with an empty set reconciles the table to empty;
            # a missing table stays missing.
            if not self.table_exists(table_name):
                return 0
            table = self._load(namespace, table_id)
            self._overwrite_with_empty(table)
            logger.info("IcebergStorage: overwrite of '%s' with empty set, table cleared", table_name)
            return 0

        hashes = [self._content_hash(record) for record in records]
        existing_hashes = self._existing_hashes(table_name)

        if mode == "append":
            fresh = [
                (record, digest)
                for record, digest in zip(records, hashes, strict=True)
                if digest not in existing_hashes
            ]
            if not fresh:
                logger.info(
                    "IcebergStorage: append of '%s' wrote nothing, all %d rows already present",
                    table_name,
                    len(records),
                )
                return 0
            rows = [record for record, _ in fresh]
            row_hashes = [digest for _, digest in fresh]
        else:
            rows = list(records)
            row_hashes = hashes

        table = self._ensure_table(table_name, namespace, table_id, rows)
        sequence = self._next_sequence(table)
        payload = self._to_arrow(table, rows, row_hashes, sequence)

        if mode == "append":
            table.append(payload)
        else:
            table.overwrite(payload)
        logger.info(
            "IcebergStorage: wrote %d row(s) to %s.%s (mode=%s)",
            len(rows),
            namespace,
            table_id,
            mode,
        )
        return len(rows)

    # Reading
    def read_table(
        self,
        table_name: str,
        limit: int | None = None,
        version: int | None = None,
    ) -> list[dict[str, Any]]:
        """Reads live rows (``version=None``) or the state at ``version``.

        Rows are ordered by the sequence column; internal columns are stripped.
        """
        namespace, table_id = self._resolve(table_name)
        if not self._table_exists(namespace, table_id):
            return []

        table = self._load(namespace, table_id)
        scan = table.scan()
        if version is not None:
            scan = scan.snapshot_id(int(version))
        arrow_table = scan.to_arrow()

        columns = [name for name in arrow_table.column_names if name not in INTERNAL_COLUMNS]
        ordered = self._sorted_by_sequence(arrow_table)

        results: list[dict[str, Any]] = []
        for row in ordered.to_pylist():
            record = {key: value for key, value in row.items() if key not in INTERNAL_COLUMNS}
            for key, value in record.items():
                if isinstance(value, str) and (value.startswith("{") or value.startswith("[")):
                    try:
                        record[key] = json.loads(value)
                    except (ValueError, TypeError):
                        pass
            results.append(record)
            if limit is not None and len(results) >= int(limit):
                break
        _ = columns  # the column order is carried by the arrow table itself
        return results

    # Metadata
    def table_exists(self, table_name: str) -> bool:
        """Checks whether the table exists in the catalog."""
        namespace, table_id = self._resolve(table_name)
        return self._table_exists(namespace, table_id)

    def get_table_schema(self, table_name: str) -> list[str]:
        """Returns the business column names of the table, in order."""
        namespace, table_id = self._resolve(table_name)
        if not self._table_exists(namespace, table_id):
            return []
        table = self._load(namespace, table_id)
        return [field.name for field in table.schema().fields if field.name not in INTERNAL_COLUMNS]

    def list_versions(self, table_name: str) -> list[dict[str, Any]]:
        """Lists every snapshot available for time travel, oldest first."""
        namespace, table_id = self._resolve(table_name)
        if not self._table_exists(namespace, table_id):
            return []
        table = self._load(namespace, table_id)
        versions: list[dict[str, Any]] = []
        for snapshot in table.metadata.snapshots:
            summary = snapshot.summary or {}
            versions.append(
                {
                    "snapshot_version": int(snapshot.snapshot_id),
                    "row_count": int(summary.get("total-records", 0)),
                    "created_at": _isoformat(snapshot.timestamp_ms),
                }
            )
        versions.sort(key=lambda item: item["snapshot_version"])
        return versions

    # Internals
    @staticmethod
    def _content_hash(record: dict[str, Any]) -> str:
        """Hashes a record's business content, ignoring adapter columns."""
        payload = {key: value for key, value in record.items() if not key.startswith("_")}
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _existing_hashes(self, table_name: str) -> set[str]:
        """The content hashes already present in the table."""
        namespace, table_id = self._resolve(table_name)
        if not self._table_exists(namespace, table_id):
            return set()
        table = self._load(namespace, table_id)
        names = [field.name for field in table.schema().fields]
        if "_content_hash" not in names:
            return set()
        arrow_table = table.scan().to_arrow()
        return {str(value) for value in arrow_table.column("_content_hash").to_pylist() if value}

    def _ensure_namespace(self, namespace: str) -> None:
        """Creates the namespace if the catalog does not have it yet."""
        if not self._catalog.namespace_exists(namespace):
            self._catalog.create_namespace(namespace)
            logger.info("IcebergStorage: created namespace %s", namespace)

    def _table_exists(self, namespace: str, table_id: str) -> bool:
        return bool(self._catalog.table_exists(f"{namespace}.{table_id}"))

    def _load(self, namespace: str, table_id: str) -> Any:
        return self._catalog.load_table(f"{namespace}.{table_id}")

    def _ensure_table(self, table_name: str, namespace: str, table_id: str, rows: list[dict[str, Any]]) -> Any:
        """Loads the table, creating it with the row schema when it is missing."""
        if self._table_exists(namespace, table_id):
            table = self._load(namespace, table_id)
            self._ensure_columns(table, rows)
            return table
        table = self._catalog.create_table(
            f"{namespace}.{table_id}",
            schema=self._arrow_schema(rows),
            properties={"format-version": "2", "write.format.default": "parquet"},
        )
        logger.info("IcebergStorage: created table %s.%s", namespace, table_id)
        return table

    @staticmethod
    def _arrow_schema(rows: list[dict[str, Any]]) -> Any:
        """Builds the arrow schema of a table from the first batch of rows."""
        import pyarrow as pa

        columns: list[str] = []
        seen: set[str] = set()
        for row in rows:
            for key in row:
                if key not in seen and key not in INTERNAL_COLUMNS:
                    seen.add(key)
                    columns.append(key)
        fields = [pa.field(name, _arrow_type(rows, name)) for name in columns]
        fields.append(pa.field("_content_hash", pa.string()))
        fields.append(pa.field("_row_seq", pa.int64()))
        return pa.schema(fields)

    @staticmethod
    def _ensure_columns(table: Any, rows: list[dict[str, Any]]) -> None:
        """Adds the business and internal columns a new batch introduced.

        Tables created outside this adapter carry the business schema only.
        """
        import pyarrow as pa

        existing = {field.name for field in table.schema().fields}
        with table.update_schema() as update:
            for row in rows:
                for key in row:
                    if key in existing or key in INTERNAL_COLUMNS:
                        continue
                    update.add_column(key, _iceberg_type(_arrow_type(rows, key)))
                    existing.add(key)
            for name, type_name in (("_content_hash", "string"), ("_row_seq", "long")):
                if name not in existing:
                    update.add_column(name, _iceberg_type_name_to_type(type_name))
                    existing.add(name)
            if existing - {field.name for field in table.schema().fields}:
                update.commit()
            else:
                # Nothing to evolve: abandon the pending update.
                update._table = table  # noqa: SLF001 - drop the pending change
        _ = pa

    @staticmethod
    def _next_sequence(table: Any) -> int:
        """The next value of the monotonic sequence column."""
        arrow_table = table.scan().to_arrow()
        if "_row_seq" not in arrow_table.column_names:
            return 1
        values = [int(value) for value in arrow_table.column("_row_seq").to_pylist() if value is not None]
        return (max(values) + 1) if values else 1

    @staticmethod
    def _to_arrow(table: Any, rows: list[dict[str, Any]], hashes: list[str], sequence: int) -> Any:
        """Builds the arrow table to write, with internal columns filled.

        The table's declared schema wins; values are cast to it.
        """
        import pyarrow as pa

        fields = IcebergLakehouseStorageAdapter._table_fields(table, rows)
        if not fields:
            fields = _schema_fields(IcebergLakehouseStorageAdapter._arrow_schema(rows))

        columns: dict[str, list[Any]] = {}
        for field in fields:
            if field.name == "_content_hash":
                columns[field.name] = list(hashes)
            elif field.name == "_row_seq":
                columns[field.name] = [sequence + index for index in range(len(rows))]
            else:
                columns[field.name] = [
                    _arrow_value(row.get(field.name), field.type) for row in rows
                ]
        return pa.Table.from_pydict(columns, schema=pa.schema(list(fields)))

    @staticmethod
    def _table_fields(table: Any, rows: list[dict[str, Any]]) -> list[Any]:
        """The arrow fields of an Iceberg table, in the table's own order."""
        import pyarrow as pa

        try:
            fields = []
            for field in _schema_fields(table.schema()):
                if field.name in INTERNAL_COLUMNS:
                    arrow_type = pa.string() if field.name == "_content_hash" else pa.int64()
                else:
                    iceberg_type = getattr(field, "field_type", None)
                    if iceberg_type is None:
                        # A double (or a table without a typed schema): infer it.
                        arrow_type = _arrow_type(rows, field.name)
                    else:
                        arrow_type = _iceberg_to_arrow_type(iceberg_type)
                # A required Iceberg column needs a required arrow field.
                required = bool(getattr(field, "required", False))
                fields.append(pa.field(field.name, arrow_type, nullable=not required))
            return fields
        except Exception:  # noqa: BLE001 - a double without a real schema
            return []


    @staticmethod
    def _sorted_by_sequence(arrow_table: Any) -> Any:
        """Sorts by the sequence column, so the output is insertion ordered."""
        if "_row_seq" not in arrow_table.column_names:
            return arrow_table
        indices = sorted(
            range(arrow_table.num_rows),
            key=lambda index: (arrow_table.column("_row_seq")[index].as_py() or 0, index),
        )
        return arrow_table.take(indices)

    @staticmethod
    def _overwrite_with_empty(table: Any) -> None:
        """Replaces the live content with nothing, keeping the old snapshot."""
        import pyarrow as pa

        schema = table.schema()
        empty = pa.Table.from_pydict({field.name: [] for field in schema}, schema=schema)
        table.overwrite(empty)


def _arrow_type(rows: list[dict[str, Any]], name: str) -> Any:
    """Infers the arrow type of a column from the Python types it holds."""
    import pyarrow as pa

    has_text = False
    has_real = False
    has_integer = False
    for row in rows:
        value = row.get(name)
        if value is None:
            continue
        if isinstance(value, bool):
            has_integer = True
        elif isinstance(value, int):
            has_integer = True
        elif isinstance(value, float):
            has_real = True
        else:
            has_text = True
            break
    if has_text:
        return pa.string()
    if has_real:
        return pa.float64()
    if has_integer:
        return pa.int64()
    return pa.string()


#: The PyIceberg class of each Iceberg type name.
_ICEBERG_TYPE_CLASSES = {
    "string": "StringType",
    "boolean": "BooleanType",
    "double": "DoubleType",
    "long": "LongType",
}


def _schema_fields(schema: Any) -> list[Any]:
    """Returns the fields of a schema as a list."""
    fields = getattr(schema, "fields", [])
    return list(fields() if callable(fields) else fields)


def _iceberg_type_name_to_type(name: str) -> Any:
    """The Iceberg type object of an Iceberg type name."""
    try:
        from pyiceberg import types as iceberg_types
    except ImportError:
        return name
    return getattr(iceberg_types, _ICEBERG_TYPE_CLASSES[name])()


def _iceberg_type(arrow_type: Any) -> Any:
    """Returns the Iceberg type of an arrow type."""
    import pyarrow as pa

    name = "string"
    if pa.types.is_boolean(arrow_type):
        name = "boolean"
    elif pa.types.is_floating(arrow_type):
        name = "double"
    elif pa.types.is_integer(arrow_type):
        name = "long"

    try:
        from pyiceberg import types as iceberg_types
    except ImportError:
        return name
    return getattr(iceberg_types, _ICEBERG_TYPE_CLASSES[name])()


def _iceberg_to_arrow_type(field_type: Any) -> Any:
    """The arrow type that matches an Iceberg type."""
    import pyarrow as pa

    try:
        from pyiceberg import types as iceberg_types
    except ImportError:
        return pa.string()

    if isinstance(field_type, iceberg_types.BooleanType):
        return pa.bool_()
    if isinstance(field_type, iceberg_types.IntegerType):
        return pa.int32()
    if isinstance(field_type, iceberg_types.LongType):
        return pa.int64()
    if isinstance(field_type, (iceberg_types.FloatType, iceberg_types.DoubleType)):
        return pa.float64()
    if isinstance(field_type, iceberg_types.DateType):
        return pa.date32()
    if isinstance(field_type, iceberg_types.TimestampType):
        return pa.timestamp("us")
    if isinstance(field_type, iceberg_types.DecimalType):
        return pa.decimal128(field_type.precision, field_type.scale)
    return pa.string()


def _iceberg_type_name(arrow_type: Any) -> str:
    """The Iceberg type NAME of an arrow type."""
    import pyarrow as pa

    if pa.types.is_boolean(arrow_type):
        return "boolean"
    if pa.types.is_floating(arrow_type):
        return "double"
    if pa.types.is_integer(arrow_type):
        return "long"
    return "string"


def _arrow_value(value: Any, arrow_type: Any) -> Any:
    """Maps a Python value to what the arrow column expects."""
    import pyarrow as pa

    if value is None:
        return None
    if pa.types.is_string(arrow_type):
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)
    if pa.types.is_boolean(arrow_type):
        return bool(value)
    if pa.types.is_floating(arrow_type):
        return float(value)
    if pa.types.is_integer(arrow_type):
        return int(value)
    if pa.types.is_date(arrow_type):
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value)[:10])
    if pa.types.is_timestamp(arrow_type):
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value))
    return str(value)


def _isoformat(timestamp_ms: int | None) -> str:
    """Formats a snapshot timestamp the way the SQLite adapter does."""
    if timestamp_ms is None:
        return datetime.now(UTC).isoformat()
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).isoformat()
