"""Where producers send events: Pub/Sub in the real pipeline, JSONL files locally."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from gtdp.config import Settings

log = logging.getLogger(__name__)


class Sink(Protocol):
    def publish(self, topic: str, record: dict[str, Any], key: str) -> None: ...

    def flush(self) -> None: ...


class JsonlSink:
    """Appends each record to data/landing/<topic>/<YYYY-MM-DD>.jsonl."""

    def __init__(self, landing_dir: Path):
        self.landing_dir = landing_dir

    def publish(self, topic: str, record: dict[str, Any], key: str) -> None:
        day = datetime.now(UTC).strftime("%Y-%m-%d")
        path = self.landing_dir / topic / f"{day}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")

    def flush(self) -> None:
        pass


class PubSubSink:
    """Publishes JSON messages. The record key goes in a message attribute for tracing."""

    def __init__(self, settings: Settings):
        from google.cloud import pubsub_v1

        self.settings = settings
        self.project = settings.require_project()
        self.publisher = pubsub_v1.PublisherClient()
        self._futures: list[Any] = []

    def publish(self, topic: str, record: dict[str, Any], key: str) -> None:
        topic_path = self.publisher.topic_path(self.project, self.settings.topic_id(topic))
        data = json.dumps(record, default=str).encode("utf-8")
        self._futures.append(self.publisher.publish(topic_path, data, key=key))
        if len(self._futures) >= 500:
            self.flush()

    def flush(self) -> None:
        for future in self._futures:
            future.result(timeout=60)
        self._futures.clear()


def make_sink(settings: Settings) -> Sink:
    if settings.sink == "pubsub":
        return PubSubSink(settings)
    if settings.sink == "local":
        log.info("Writing events to %s", settings.landing_dir)
        return JsonlSink(settings.landing_dir)
    raise ValueError(f"Unknown GTDP_SINK {settings.sink!r} (expected 'local' or 'pubsub')")
