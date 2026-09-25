"""Alerting. Always logs; also POSTs to ALERT_WEBHOOK_URL (Slack-compatible) when set."""

from __future__ import annotations

import json
import logging
import urllib.request

from gtdp.config import Settings

log = logging.getLogger(__name__)


def send_alert(title: str, detail: str, settings: Settings | None = None) -> None:
    settings = settings or Settings.from_env()
    log.error("ALERT: %s\n%s", title, detail)
    if not settings.alert_webhook_url:
        return
    body = json.dumps({"text": f":rotating_light: *{title}*\n```{detail}```"}).encode("utf-8")
    request = urllib.request.Request(
        settings.alert_webhook_url, data=body, headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(request, timeout=10)
    except OSError:
        # An alerting outage must not hide the original failure.
        log.exception("Failed to deliver alert to webhook")


def airflow_failure_callback(context: dict) -> None:
    """on_failure_callback for Airflow tasks."""
    ti = context["task_instance"]
    send_alert(
        f"Airflow task failed: {ti.dag_id}.{ti.task_id}",
        f"run {context['run_id']}\nlog: {ti.log_url}",
    )
