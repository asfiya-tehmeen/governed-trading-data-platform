"""Create Pub/Sub topics/subscriptions and BigQuery datasets/tables. Safe to re-run."""

from __future__ import annotations

import logging

from gtdp.config import TOPICS, Settings
from gtdp.contracts import load_contract
from gtdp.ingestion.loader import DEAD_LETTER_SCHEMA, DEAD_LETTER_TABLE
from gtdp.quality.runner import DQ_RESULTS_SCHEMA, DQ_RESULTS_TABLE

log = logging.getLogger(__name__)


def bootstrap_pubsub(settings: Settings) -> None:
    from google.api_core.exceptions import AlreadyExists
    from google.cloud import pubsub_v1

    project = settings.require_project()
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    for topic in TOPICS:
        topic_path = publisher.topic_path(project, settings.topic_id(topic))
        sub_path = subscriber.subscription_path(project, settings.subscription_id(topic))
        try:
            publisher.create_topic(name=topic_path)
            log.info("Created topic %s", topic_path)
        except AlreadyExists:
            pass
        try:
            subscriber.create_subscription(name=sub_path, topic=topic_path, ack_deadline_seconds=60)
            log.info("Created subscription %s", sub_path)
        except AlreadyExists:
            pass


def bootstrap_bigquery(settings: Settings) -> None:
    from google.cloud import bigquery

    project = settings.require_project()
    client = bigquery.Client(project=project, location=settings.bq_location)

    for dataset in (settings.raw_dataset, settings.ops_dataset):
        ds = bigquery.Dataset(f"{project}.{dataset}")
        ds.location = settings.bq_location
        client.create_dataset(ds, exists_ok=True)

    def schema(fields: list[dict]) -> list:
        return [bigquery.SchemaField.from_api_repr(f) for f in fields]

    tables = [
        (
            settings.raw_dataset,
            load_contract(t).table,
            schema(load_contract(t).bigquery_schema()),
            "ingested_at",
        )
        for t in TOPICS
    ]
    tables += [
        (settings.raw_dataset, DEAD_LETTER_TABLE, schema(DEAD_LETTER_SCHEMA), "received_at"),
        (settings.ops_dataset, DQ_RESULTS_TABLE, schema(DQ_RESULTS_SCHEMA), "checked_at"),
    ]
    for dataset, name, fields, partition_col in tables:
        table = bigquery.Table(f"{project}.{dataset}.{name}", schema=fields)
        table.time_partitioning = bigquery.TimePartitioning(field=partition_col)
        client.create_table(table, exists_ok=True)
        log.info("Ensured table %s.%s", dataset, name)
