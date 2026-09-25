"""Pub/Sub -> contract validation -> BigQuery.

Every message is checked against its table's data contract. Valid rows go to
raw.<table>; anything else goes to raw.dead_letter with the reasons, so a bad
producer is visible and never lands in the tables reports are built from.

Messages are acked only after their rows are written, so a BigQuery failure
leads to redelivery (at-least-once). Staging deduplicates on the primary key.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from gtdp.alerts import send_alert
from gtdp.config import TOPICS, Settings
from gtdp.contracts import Contract, load_contract

log = logging.getLogger(__name__)

DEAD_LETTER_TABLE = "dead_letter"
DEAD_LETTER_SCHEMA = [
    {"name": "target_table", "type": "STRING", "mode": "REQUIRED"},
    {"name": "contract_version", "type": "INT64", "mode": "REQUIRED"},
    {"name": "message_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "payload", "type": "STRING", "mode": "NULLABLE"},
    {"name": "errors", "type": "STRING", "mode": "REQUIRED", "description": "JSON list"},
    {"name": "received_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]


@dataclass
class Routed:
    rows: list[dict[str, Any]]
    row_ids: list[str]
    dead_letters: list[dict[str, Any]]


def route_messages(
    contract: Contract,
    messages: list[tuple[str | None, bytes]],
    received_at: datetime | None = None,
) -> Routed:
    """Split (message_id, payload) pairs into valid rows and dead letters."""
    ingested_at = (received_at or datetime.now(UTC)).isoformat()
    routed = Routed(rows=[], row_ids=[], dead_letters=[])

    for message_id, payload in messages:
        text = payload.decode("utf-8", errors="replace")
        try:
            record = json.loads(text)
        except json.JSONDecodeError as exc:
            errors = [f"payload is not valid JSON: {exc}"]
        else:
            if isinstance(record, dict):
                record = {**record, "ingested_at": ingested_at}
                errors = contract.validate(record)
            else:
                errors = [f"payload is a JSON {type(record).__name__}, expected an object"]

        if errors:
            routed.dead_letters.append(
                {
                    "target_table": contract.table,
                    "contract_version": contract.version,
                    "message_id": message_id,
                    "payload": text,
                    "errors": json.dumps(errors),
                    "received_at": ingested_at,
                }
            )
        else:
            routed.rows.append(record)
            routed.row_ids.append("|".join(str(record[k]) for k in contract.primary_key))

    return routed


class Loader:
    def __init__(self, settings: Settings):
        from google.cloud import bigquery, pubsub_v1

        self.settings = settings
        self.project = settings.require_project()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.bq = bigquery.Client(project=self.project, location=settings.bq_location)
        self.contracts = {topic: load_contract(topic) for topic in TOPICS}

    def _table(self, name: str) -> str:
        return f"{self.project}.{self.settings.raw_dataset}.{name}"

    def pull_once(self, topic: str, max_messages: int = 500) -> int:
        subscription = self.subscriber.subscription_path(
            self.project, self.settings.subscription_id(topic)
        )
        response = self.subscriber.pull(
            request={"subscription": subscription, "max_messages": max_messages},
            timeout=30,
        )
        if not response.received_messages:
            return 0

        received = response.received_messages
        contract = self.contracts[topic]
        routed = route_messages(
            contract, [(m.message.message_id, m.message.data) for m in received]
        )

        insert_errors = []
        if routed.rows:
            insert_errors += self.bq.insert_rows_json(
                self._table(contract.table), routed.rows, row_ids=routed.row_ids
            )
        if routed.dead_letters:
            log.warning(
                "%d %s messages failed contract validation", len(routed.dead_letters), topic
            )
            insert_errors += self.bq.insert_rows_json(
                self._table(DEAD_LETTER_TABLE), routed.dead_letters
            )

        if insert_errors:
            # Don't ack: the messages will be redelivered once the problem is fixed.
            send_alert(
                f"BigQuery insert failed for {topic}",
                json.dumps(insert_errors[:5], indent=2, default=str),
                self.settings,
            )
            return 0

        self.subscriber.acknowledge(
            request={"subscription": subscription, "ack_ids": [m.ack_id for m in received]}
        )
        log.info(
            "%s: loaded %d, dead-lettered %d", topic, len(routed.rows), len(routed.dead_letters)
        )
        return len(received)

    def run(self, idle_sleep_s: float = 5.0) -> None:
        while True:
            loaded = sum(self.pull_once(topic) for topic in TOPICS)
            if loaded == 0:
                time.sleep(idle_sleep_s)
