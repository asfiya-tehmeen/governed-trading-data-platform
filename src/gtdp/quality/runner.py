"""Run the quality checks against BigQuery, store results in ops.dq_results, and alert.

A run that produces any FAIL raises ChecksFailed, so the Airflow task fails and
nothing downstream (dbt build, reports) runs on bad data.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from gtdp.alerts import send_alert
from gtdp.config import TOPICS, Settings
from gtdp.contracts import load_contract
from gtdp.quality.checks import (
    CheckResult,
    Status,
    check_completeness,
    check_duplicates,
    check_freshness,
    check_schema_drift,
    check_volume_anomaly,
)

log = logging.getLogger(__name__)

DQ_RESULTS_TABLE = "dq_results"
DQ_RESULTS_SCHEMA = [
    {"name": "check", "type": "STRING", "mode": "REQUIRED"},
    {"name": "table", "type": "STRING", "mode": "REQUIRED"},
    {"name": "status", "type": "STRING", "mode": "REQUIRED"},
    {"name": "message", "type": "STRING", "mode": "REQUIRED"},
    {"name": "observed", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "threshold", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "checked_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

CHECK_GROUPS = ("schema-drift", "freshness", "completeness", "duplicates", "volume", "all")


class ChecksFailed(RuntimeError):
    pass


class CheckRunner:
    def __init__(self, settings: Settings):
        from google.cloud import bigquery

        self.settings = settings
        self.project = settings.require_project()
        self.bq = bigquery.Client(project=self.project, location=settings.bq_location)
        self.raw = f"`{self.project}.{settings.raw_dataset}`"

    def _query(self, sql: str) -> list:
        return list(self.bq.query(sql).result())

    # --- measurements ---

    def schema_drift(self) -> list[CheckResult]:
        rows = self._query(
            f"select table_name, column_name, data_type from {self.raw}.INFORMATION_SCHEMA.COLUMNS"
        )
        results = []
        for topic in TOPICS:
            contract = load_contract(topic)
            actual = {r.column_name: r.data_type for r in rows if r.table_name == contract.table}
            results.append(check_schema_drift(contract, actual))
        return results

    def freshness(self) -> list[CheckResult]:
        now = datetime.now(UTC)
        results = []
        for topic in TOPICS:
            contract = load_contract(topic)
            f = contract.freshness
            [row] = self._query(
                f"select max({f['timestamp_column']}) as latest from {self.raw}.{contract.table}"
            )
            results.append(
                check_freshness(
                    contract.table,
                    row.latest,
                    now,
                    f["warn_after_minutes"],
                    f["error_after_minutes"],
                )
            )
        return results

    def completeness(self, window_minutes: int = 15) -> list[CheckResult]:
        rows = self._query(
            f"select distinct symbol from {self.raw}.ticks "
            "where ingested_at >= "
            f"timestamp_sub(current_timestamp(), interval {window_minutes} minute)"
        )
        return [
            check_completeness(
                "ticks", "symbol", set(self.settings.deriv_symbols), {r.symbol for r in rows}
            )
        ]

    def duplicates(self) -> list[CheckResult]:
        [row] = self._query(
            f"""
            with keys as (
                select
                    trade_id,
                    count(*) as n,
                    count(distinct format('%t', (client_id, symbol, side, quantity, price,
                                                 commission, executed_at))) as versions
                from {self.raw}.trades
                where ingested_at >= timestamp_sub(current_timestamp(), interval 1 day)
                group by trade_id
            )
            select countif(n > 1) as duplicate_keys, countif(versions > 1) as conflicting_keys
            from keys
            """
        )
        return [check_duplicates("trades", row.duplicate_keys, row.conflicting_keys)]

    def volume(self) -> list[CheckResult]:
        contract = load_contract("trades")
        lookback = contract.volume.get("lookback_hours", 168)
        rows = self._query(
            f"""
            select timestamp_trunc(ingested_at, hour) as hour, count(*) as n
            from {self.raw}.trades
            where ingested_at >= timestamp_sub(timestamp_trunc(current_timestamp(), hour),
                                               interval {lookback + 1} hour)
              and ingested_at < timestamp_trunc(current_timestamp(), hour)
            group by hour
            """
        )
        counts = {r.hour: r.n for r in rows}
        last_complete = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
            hours=1
        )
        if not counts:
            return [check_volume_anomaly("trades", 0, [])]
        # Start from the first hour with data so a new deployment isn't one long "outage".
        first = min(counts)
        hours = []
        h = first
        while h < last_complete:
            hours.append(counts.get(h, 0))
            h += timedelta(hours=1)
        return [
            check_volume_anomaly(
                "trades",
                counts.get(last_complete, 0),
                hours,
                z_threshold=contract.volume.get("z_threshold", 3.0),
            )
        ]

    # --- orchestration ---

    def run(self, group: str = "all") -> list[CheckResult]:
        groups = {
            "schema-drift": self.schema_drift,
            "freshness": self.freshness,
            "completeness": self.completeness,
            "duplicates": self.duplicates,
            "volume": self.volume,
        }
        selected = groups.values() if group == "all" else [groups[group]]
        results = [r for fn in selected for r in fn()]

        for r in results:
            log.log(
                logging.ERROR if r.status == Status.FAIL else logging.INFO,
                "[%s] %s.%s: %s",
                r.status,
                r.table,
                r.check,
                r.message,
            )

        errors = self.bq.insert_rows_json(
            f"{self.project}.{self.settings.ops_dataset}.{DQ_RESULTS_TABLE}",
            [r.as_row() for r in results],
        )
        if errors:
            log.error("Could not record check results: %s", errors)

        failed = [r for r in results if r.status == Status.FAIL]
        if failed:
            detail = "\n".join(f"{r.table}.{r.check}: {r.message}" for r in failed)
            send_alert(f"{len(failed)} data quality check(s) failed", detail, self.settings)
            raise ChecksFailed(detail)
        return results
