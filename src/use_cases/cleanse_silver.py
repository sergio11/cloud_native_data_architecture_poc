"""Modern Data Architecture POC - Silver cleansing and quality gate use case.

Deduplicates, casts and quality-gates Bronze records before promoting to Silver.
"""

from __future__ import annotations

import logging
from typing import Any, cast

from src.domain.contracts import resolve_contract_field_types
from src.domain.naming import bronze_table, silver_table
from src.ports.inbound import CleanseSilverPort, CleanseSilverResult
from src.ports.outbound import (
    ContractCatalogPort,
    LakehouseTableReader,
    LakehouseTableWriter,
    LineagePublisherPort,
    QualityEnginePort,
)
from src.use_cases.gate_support import enforce_quality_gate

logger = logging.getLogger("use_case.cleanse_silver")

#: Truthy/falsy string tokens accepted for boolean fields during Silver typing.
_TRUE_TOKENS: frozenset[str] = frozenset({"true", "t", "1", "yes", "y"})
_FALSE_TOKENS: frozenset[str] = frozenset({"false", "f", "0", "no", "n"})


def _resolve_entity_field_types(catalog: ContractCatalogPort | None, entity: str) -> dict[str, str]:
    """Returns canonical type families for ``entity`` from its data contract.

    Returns {} when no contract is found or readable; typing is then skipped
    and the quality gate still guards promotion.
    """
    if catalog is None:
        return {}
    try:
        contracts = catalog.list_contracts()
    except Exception as exc:
        logger.warning("Silver typing: contract catalog unreadable (%s); skipping type casts", exc)
        return {}
    segment = f"data-products/{entity}/"
    for path, data in contracts:
        if segment not in str(path).replace("\\", "/"):
            continue
        try:
            return resolve_contract_field_types(data)
        except Exception as exc:
            logger.warning("Silver typing: cannot parse contract %s (%s); skipping type casts", path, exc)
            return {}
    return {}


def _cast_value(value: Any, family: str) -> tuple[Any, bool]:
    """Casts ``value`` to the contract ``family``; returns (result, ok).

    Unparseable values keep the original value and report ``ok=False``.
    """
    if value is None:
        return None, True
    if family == "integer":
        if isinstance(value, bool):
            return int(value), True
        if isinstance(value, int):
            return value, True
        if isinstance(value, float):
            if value.is_integer():
                return int(value), True
            return value, False
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return value, False
            try:
                return int(text), True
            except ValueError:
                try:
                    as_float = float(text)
                except ValueError:
                    return value, False
                if as_float.is_integer():
                    return int(as_float), True
                return value, False
        return value, False
    if family == "number":
        if isinstance(value, bool):
            return value, False
        if isinstance(value, int):
            return float(value), True
        if isinstance(value, float):
            return value, True
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return value, False
            try:
                return float(text), True
            except ValueError:
                return value, False
        return value, False
    if family == "boolean":
        if isinstance(value, bool):
            return value, True
        if isinstance(value, (int, float)):
            if value == 1:
                return True, True
            if value == 0:
                return False, True
            return value, False
        if isinstance(value, str):
            token = value.strip().lower()
            if token in _TRUE_TOKENS:
                return True, True
            if token in _FALSE_TOKENS:
                return False, True
            return value, False
        return value, False
    if family in ("string", ""):
        if isinstance(value, str):
            return value, True
        if isinstance(value, (dict, list)):
            return value, True
        return str(value), True
    return value, True


class CleanseSilverUseCase(CleanseSilverPort):
    """Orchestrates Bronze-to-Silver cleansing, deduplication, and quality gate enforcement."""

    def __init__(
        self,
        storage_reader: LakehouseTableReader,
        storage_writer: LakehouseTableWriter,
        quality_engine: QualityEnginePort,
        lineage: LineagePublisherPort,
        catalog: ContractCatalogPort,
    ) -> None:
        self._reader = storage_reader
        self._writer = storage_writer
        self._quality = quality_engine
        self._lineage = lineage
        self._catalog = catalog

    def execute(
        self,
        entity: str,
        suite_path: str,
        quality_threshold: float | None = None,
    ) -> CleanseSilverResult:
        source_table = bronze_table(entity)
        target_table = silver_table(entity)
        logger.info("Starting Silver transformation from '%s' to '%s'", source_table, target_table)

        # 1. Read raw records from Bronze
        raw_records = self._reader.read_table(source_table)
        if not raw_records:
            logger.warning("No records found in '%s'. Skipping promotion.", source_table)
            # EMPTY mirrors the SUCCESS contract keys.
            empty_result: dict[str, Any] = {
                "entity": entity,
                "target_table": target_table,
                "rows_promoted": 0,
                "quality_pass_rate": 0.0,
                "status": "EMPTY",
                "dropped_null_id": 0,
                "cast_failures": 0,
            }
            return cast(CleanseSilverResult, empty_result)

        # 2. Deduplicate by primary key 'id': latest snapshot wins.
        # Rows with a NULL id are dropped and counted.
        latest: dict[object, dict[str, Any]] = {}
        dropped_null_id = 0
        for r in raw_records:
            record_id = r.get("id")
            if record_id is None:
                dropped_null_id += 1
                continue
            # Filter out internal audit columns
            clean_row = {k: v for k, v in r.items() if not k.startswith("_")}
            latest[record_id] = clean_row
        deduped: list[dict[str, Any]] = list(latest.values())

        logger.info("Deduplication: %d raw -> %d unique records", len(raw_records), len(deduped))
        if dropped_null_id:
            logger.warning(
                "Dropped %d Bronze record(s) with NULL 'id' before Silver promotion of '%s'",
                dropped_null_id,
                source_table,
            )

        # 2b. Explicit typing: cast each field to its contract schema type.
        # Unparseable values keep the original and bump ``cast_failures``.
        field_families = _resolve_entity_field_types(self._catalog, entity)
        typed: list[dict[str, Any]] = []
        cast_failures = 0
        for row in deduped:
            cast_row = dict(row)
            for field_name, family in field_families.items():
                if field_name not in cast_row:
                    continue
                casted, ok = _cast_value(cast_row[field_name], family)
                if ok:
                    cast_row[field_name] = casted
                else:
                    cast_failures += 1
                    logger.warning(
                        "Silver type cast failed for field '%s' value %r (expected %s); keeping original",
                        field_name,
                        cast_row[field_name],
                        family or "unknown",
                    )
            typed.append(cast_row)
        if cast_failures:
            logger.warning(
                "Silver type casting finished with %d failure(s) for '%s'; original values kept (fail-open)",
                cast_failures,
                target_table,
            )

        # 3. Quality gate (fail-closed): a missing suite propagates.
        quality_pass_rate = enforce_quality_gate(
            quality_engine=self._quality,
            lineage=self._lineage,
            catalog=self._catalog,
            records=typed,
            suite_path=suite_path,
            table_name=target_table,
            job_name=f"silver_clean_{entity}",
            inputs=[source_table],
            entity=entity,
            quality_threshold=quality_threshold,
        )
        logger.info("Quality gate PASSED (Pass rate: %.2f)", quality_pass_rate)

        # 4. Write conformed records to Silver tier
        written_count = self._writer.write_table(target_table, typed, mode="overwrite")
        logger.info("Successfully wrote %d records to '%s'", written_count, target_table)

        # 5. Emit OpenLineage metadata
        self._lineage.emit_job_event(
            job_name=f"silver_clean_{entity}",
            inputs=[source_table],
            outputs=[target_table],
            status="COMPLETE",
        )

        return cast(
            CleanseSilverResult,
            {
                "entity": entity,
                "target_table": target_table,
                "rows_promoted": written_count,
                "quality_pass_rate": quality_pass_rate,
                "status": "SUCCESS",
                "dropped_null_id": dropped_null_id,
                "cast_failures": cast_failures,
            },
        )
