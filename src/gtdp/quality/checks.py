"""Data quality check logic.

These functions are pure: they take already-measured values and return a
CheckResult. The BigQuery queries that measure them live in runner.py, which
keeps the decision logic unit-testable without a warehouse.
"""

from __future__ import annotations

import statistics
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from gtdp.contracts import Contract


class Status(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclass(frozen=True)
class CheckResult:
    check: str
    table: str
    status: Status
    message: str
    observed: float | None = None
    threshold: float | None = None
    checked_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def as_row(self) -> dict:
        row = asdict(self)
        row["status"] = str(self.status)
        return row


# --- Freshness ---------------------------------------------------------------


def check_freshness(
    table: str,
    latest: datetime | None,
    now: datetime,
    warn_after_minutes: float,
    error_after_minutes: float,
) -> CheckResult:
    if latest is None:
        return CheckResult("freshness", table, Status.FAIL, "table has no rows")
    age = (now - latest).total_seconds() / 60
    if age > error_after_minutes:
        status = Status.FAIL
    elif age > warn_after_minutes:
        status = Status.WARN
    else:
        status = Status.PASS
    return CheckResult(
        "freshness",
        table,
        status,
        f"newest row is {age:.1f} min old "
        f"(warn > {warn_after_minutes}, error > {error_after_minutes})",
        observed=round(age, 2),
        threshold=error_after_minutes,
    )


# --- Completeness ------------------------------------------------------------


def check_completeness(table: str, key: str, expected: set[str], observed: set[str]) -> CheckResult:
    missing = sorted(expected - observed)
    if missing:
        return CheckResult(
            "completeness",
            table,
            Status.FAIL,
            f"no recent rows for {key} {missing}",
            observed=len(expected) - len(missing),
            threshold=len(expected),
        )
    return CheckResult(
        "completeness",
        table,
        Status.PASS,
        f"all {len(expected)} expected {key} values present",
        observed=len(expected),
        threshold=len(expected),
    )


# --- Duplicates --------------------------------------------------------------


def check_duplicates(table: str, duplicate_keys: int, conflicting_keys: int) -> CheckResult:
    """Exact re-deliveries are expected (at-least-once) and removed in staging, so
    they only warn. The same key with *different* content means we cannot know
    which version is true, so that fails."""
    if conflicting_keys:
        return CheckResult(
            "duplicates",
            table,
            Status.FAIL,
            f"{conflicting_keys} keys have conflicting duplicate rows",
            observed=conflicting_keys,
            threshold=0,
        )
    if duplicate_keys:
        return CheckResult(
            "duplicates",
            table,
            Status.WARN,
            f"{duplicate_keys} keys re-delivered with identical content (deduplicated in staging)",
            observed=duplicate_keys,
            threshold=0,
        )
    return CheckResult(
        "duplicates", table, Status.PASS, "no duplicate keys", observed=0, threshold=0
    )


# --- Volume anomaly ----------------------------------------------------------


def check_volume_anomaly(
    table: str,
    current: int,
    history: list[int],
    z_threshold: float = 3.0,
    min_history: int = 24,
) -> CheckResult:
    """Compare the latest complete period's row count with the trailing baseline."""
    if len(history) < min_history:
        return CheckResult(
            "volume_anomaly",
            table,
            Status.PASS,
            f"only {len(history)} periods of history (need {min_history}); not evaluated",
            observed=current,
        )
    mean = statistics.fmean(history)
    stdev = statistics.pstdev(history)
    if stdev == 0:
        z = 0.0 if current == mean else float("inf")
    else:
        z = (current - mean) / stdev
    status = Status.FAIL if abs(z) > z_threshold else Status.PASS
    return CheckResult(
        "volume_anomaly",
        table,
        status,
        f"{current} rows vs baseline mean {mean:.1f} (sd {stdev:.1f}), z = {z:.2f}",
        observed=round(z, 3) if z != float("inf") else None,
        threshold=z_threshold,
    )


# --- Schema drift ------------------------------------------------------------


@dataclass(frozen=True)
class SchemaDrift:
    missing: list[str]
    unexpected: list[str]
    type_changed: list[tuple[str, str, str]]  # (column, contract type, actual type)

    @property
    def has_drift(self) -> bool:
        return bool(self.missing or self.unexpected or self.type_changed)


def diff_schema(contract: Contract, actual: dict[str, str]) -> SchemaDrift:
    """Compare a live table schema ({column: BigQuery type}) with its contract."""
    expected = {c.name: c.type for c in contract.columns}
    return SchemaDrift(
        missing=sorted(set(expected) - set(actual)),
        unexpected=sorted(set(actual) - set(expected)),
        type_changed=sorted(
            (name, expected[name], actual[name])
            for name in set(expected) & set(actual)
            if expected[name] != actual[name]
        ),
    )


def check_schema_drift(contract: Contract, actual: dict[str, str]) -> CheckResult:
    drift = diff_schema(contract, actual)
    if not drift.has_drift:
        return CheckResult(
            "schema_drift",
            contract.table,
            Status.PASS,
            f"matches contract {contract.name} v{contract.version}",
        )
    parts = []
    if drift.missing:
        parts.append(f"missing columns {drift.missing}")
    if drift.unexpected:
        parts.append(f"unexpected columns {drift.unexpected}")
    if drift.type_changed:
        parts.append(
            "type changes " + ", ".join(f"{c}: {a} -> {b}" for c, a, b in drift.type_changed)
        )
    return CheckResult(
        "schema_drift",
        contract.table,
        Status.FAIL,
        f"differs from contract {contract.name} v{contract.version}: " + "; ".join(parts),
    )
