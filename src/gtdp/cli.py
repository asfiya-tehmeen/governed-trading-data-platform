"""Command line entry point: `python -m gtdp <command>`."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from dotenv import load_dotenv

from gtdp.config import PROJECT_ROOT, Settings


def cmd_stream(args: argparse.Namespace, settings: Settings) -> None:
    """Stream live Deriv ticks and simulated trades to the configured sink."""
    from gtdp.ingestion.deriv_stream import stream_ticks
    from gtdp.ingestion.trade_simulator import TradeSimulator
    from gtdp.sinks import make_sink

    sink = make_sink(settings)
    sim = TradeSimulator(
        num_clients=settings.sim_num_clients,
        trade_probability=settings.sim_trade_probability,
        seed=settings.sim_seed,
    )
    for client in sim.client_records():
        sink.publish("clients", client, key=client["client_id"])
    sink.flush()

    counts = {"ticks": 0, "trades": 0}

    def on_tick(tick: dict) -> None:
        sink.publish("ticks", tick, key=tick["tick_id"])
        counts["ticks"] += 1
        for trade in sim.on_tick(tick):
            sink.publish("trades", trade, key=trade["trade_id"])
            counts["trades"] += 1

    try:
        asyncio.run(
            stream_ticks(settings.deriv_ws_url, settings.deriv_symbols, on_tick, args.duration)
        )
    finally:
        sink.flush()
        logging.info("Published %d ticks and %d trades", counts["ticks"], counts["trades"])


def cmd_load(args: argparse.Namespace, settings: Settings) -> None:
    from gtdp.ingestion.loader import Loader

    Loader(settings).run()


def cmd_bootstrap(args: argparse.Namespace, settings: Settings) -> None:
    from gtdp.bootstrap import bootstrap_bigquery, bootstrap_pubsub

    if args.target in ("pubsub", "all"):
        bootstrap_pubsub(settings)
    if args.target in ("bigquery", "all"):
        bootstrap_bigquery(settings)


def cmd_check(args: argparse.Namespace, settings: Settings) -> None:
    from gtdp.quality.runner import CheckRunner, ChecksFailed

    try:
        CheckRunner(settings).run(args.group)
    except ChecksFailed:
        sys.exit(1)


def main(argv: list[str] | None = None) -> None:
    from gtdp.quality.runner import CHECK_GROUPS

    parser = argparse.ArgumentParser(prog="gtdp", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("stream", help="stream Deriv ticks + simulated trades")
    p.add_argument("--duration", type=float, default=None, help="stop after N seconds")
    p.set_defaults(func=cmd_stream)

    p = sub.add_parser("load", help="load Pub/Sub messages into BigQuery")
    p.set_defaults(func=cmd_load)

    p = sub.add_parser("bootstrap", help="create topics, subscriptions, datasets and tables")
    p.add_argument("target", nargs="?", choices=["pubsub", "bigquery", "all"], default="all")
    p.set_defaults(func=cmd_bootstrap)

    p = sub.add_parser("check", help="run data quality checks")
    p.add_argument("group", nargs="?", choices=CHECK_GROUPS, default="all")
    p.set_defaults(func=cmd_check)

    args = parser.parse_args(argv)
    load_dotenv(PROJECT_ROOT / ".env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args.func(args, Settings.from_env())
