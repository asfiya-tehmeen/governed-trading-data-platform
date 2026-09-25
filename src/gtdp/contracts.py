"""Data contracts: load the YAML definitions in contracts/ and validate records.

The same contract drives ingestion validation, the BigQuery raw table schema,
and the schema-drift check, so there is one definition of each raw table.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from functools import cache
from pathlib import Path
from typing import Any

import yaml

from gtdp.config import PROJECT_ROOT

CONTRACTS_DIR = Path(os.environ.get("GTDP_CONTRACTS_DIR", PROJECT_ROOT / "contracts"))

SUPPORTED_TYPES = {"STRING", "INT64", "FLOAT64", "NUMERIC", "BOOL", "TIMESTAMP"}


def _is_timestamp(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _is_numeric(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        try:
            return Decimal(value).is_finite()
        except InvalidOperation:
            return False
    return False


_TYPE_CHECKS = {
    "STRING": lambda v: isinstance(v, str),
    "INT64": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "FLOAT64": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "NUMERIC": _is_numeric,
    "BOOL": lambda v: isinstance(v, bool),
    "TIMESTAMP": _is_timestamp,
}


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    required: bool = False
    description: str = ""
    allowed_values: tuple[Any, ...] | None = None
    minimum: float | None = None
    exclusive_minimum: bool = False
    pii: bool = False

    def validate(self, value: Any) -> list[str]:
        if value is None:
            return [f"{self.name}: required field is null or missing"] if self.required else []
        if not _TYPE_CHECKS[self.type](value):
            return [f"{self.name}: expected {self.type}, got {type(value).__name__} {value!r}"]
        errors = []
        if self.allowed_values is not None and value not in self.allowed_values:
            errors.append(f"{self.name}: {value!r} not in {list(self.allowed_values)}")
        if self.minimum is not None:
            number = Decimal(str(value))
            too_small = number <= self.minimum if self.exclusive_minimum else number < self.minimum
            if too_small:
                op = ">" if self.exclusive_minimum else ">="
                errors.append(f"{self.name}: {value!r} must be {op} {self.minimum}")
        return errors


@dataclass(frozen=True)
class Contract:
    name: str
    table: str
    version: int
    owner: str
    description: str
    primary_key: tuple[str, ...]
    columns: tuple[Column, ...]
    freshness: dict[str, Any] = field(default_factory=dict)
    volume: dict[str, Any] = field(default_factory=dict)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def pii_columns(self) -> list[str]:
        return [c.name for c in self.columns if c.pii]

    def validate(self, record: dict[str, Any]) -> list[str]:
        """Return a list of contract violations; an empty list means the record is valid."""
        errors = [
            f"{key}: unexpected field (not in contract {self.name} v{self.version})"
            for key in record
            if key not in self.column_names
        ]
        for column in self.columns:
            errors.extend(column.validate(record.get(column.name)))
        return errors

    def bigquery_schema(self) -> list[dict[str, str]]:
        """Schema in the JSON form accepted by the BigQuery API."""
        return [
            {
                "name": c.name,
                "type": c.type,
                "mode": "REQUIRED" if c.required else "NULLABLE",
                "description": c.description.strip(),
            }
            for c in self.columns
        ]


def _parse(raw: dict[str, Any]) -> Contract:
    columns = []
    for col in raw["columns"]:
        if col["type"] not in SUPPORTED_TYPES:
            raise ValueError(f"{raw['name']}.{col['name']}: unsupported type {col['type']}")
        allowed = col.get("allowed_values")
        columns.append(
            Column(
                name=col["name"],
                type=col["type"],
                required=col.get("required", False),
                description=col.get("description", ""),
                allowed_values=tuple(allowed) if allowed is not None else None,
                minimum=col.get("minimum"),
                exclusive_minimum=col.get("exclusive_minimum", False),
                pii=col.get("pii", False),
            )
        )
    return Contract(
        name=raw["name"],
        table=raw["table"],
        version=raw["version"],
        owner=raw["owner"],
        description=raw.get("description", "").strip(),
        primary_key=tuple(raw["primary_key"]),
        columns=tuple(columns),
        freshness=raw.get("freshness", {}),
        volume=raw.get("volume", {}),
    )


@cache
def load_contract(table: str) -> Contract:
    """Load the contract for a raw table, e.g. load_contract("trades")."""
    path = CONTRACTS_DIR / f"raw_{table}.yml"
    with path.open(encoding="utf-8") as f:
        return _parse(yaml.safe_load(f))


def load_all() -> dict[str, Contract]:
    return {
        contract.table: contract
        for contract in (
            load_contract(p.stem.removeprefix("raw_"))
            for p in sorted(CONTRACTS_DIR.glob("raw_*.yml"))
        )
    }
