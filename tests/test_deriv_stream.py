import pytest

from gtdp.contracts import load_contract
from gtdp.ingestion.deriv_stream import DerivAPIError, parse_tick

TICK_MESSAGE = {
    "echo_req": {"ticks": "R_100", "subscribe": 1},
    "msg_type": "tick",
    "subscription": {"id": "sub-1"},
    "tick": {
        "ask": 1234.57,
        "bid": 1234.55,
        "epoch": 1790000000,
        "id": "tick-1",
        "pip_size": 2,
        "quote": 1234.56,
        "symbol": "R_100",
    },
}


def test_parse_tick():
    assert parse_tick(TICK_MESSAGE) == {
        "tick_id": "R_100:1790000000",
        "subscription_id": "tick-1",
        "symbol": "R_100",
        "quote": 1234.56,
        "bid": 1234.55,
        "ask": 1234.57,
        "pip_size": 2,
        "epoch": 1790000000,
        "quoted_at": "2026-09-21T14:13:20+00:00",
    }


def test_parsed_tick_matches_contract():
    record = {**parse_tick(TICK_MESSAGE), "ingested_at": "2026-09-21T14:13:21+00:00"}
    assert load_contract("ticks").validate(record) == []


def test_non_tick_messages_are_ignored():
    assert parse_tick({"msg_type": "ping", "ping": "pong"}) is None


def test_api_errors_raise():
    with pytest.raises(DerivAPIError, match="InvalidSymbol"):
        parse_tick({"error": {"code": "InvalidSymbol", "message": "Symbol X is invalid"}})


def test_ticks_on_one_subscription_get_distinct_ids():
    """Deriv repeats the subscription id on every tick; our key must not."""
    later = {**TICK_MESSAGE, "tick": {**TICK_MESSAGE["tick"], "epoch": 1790000002}}
    assert parse_tick(TICK_MESSAGE)["tick_id"] != parse_tick(later)["tick_id"]
