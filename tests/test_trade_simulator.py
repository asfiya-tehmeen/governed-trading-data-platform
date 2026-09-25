from datetime import UTC, datetime
from decimal import Decimal

from gtdp.contracts import load_contract
from gtdp.ingestion.trade_simulator import TradeSimulator

NOW = datetime(2026, 9, 25, tzinfo=UTC)
TICK = {
    "tick_id": "R_100:1790000000",
    "subscription_id": "sub-1",
    "symbol": "R_100",
    "quote": 1234.56,
    "bid": 1234.55,
    "ask": 1234.57,
    "pip_size": 2,
    "epoch": 1790000000,
    "quoted_at": "2026-09-21T14:13:20+00:00",
}
INGESTED = {"ingested_at": "2026-09-25T00:00:00+00:00"}


def make_trades(n_ticks=500):
    sim = TradeSimulator(num_clients=20, trade_probability=0.5, seed=42, now=NOW)
    return sim, [t for _ in range(n_ticks) for t in sim.on_tick(TICK)]


def test_same_seed_is_deterministic():
    _, a = make_trades()
    _, b = make_trades()
    assert a == b


def test_clients_match_contract():
    sim, _ = make_trades()
    contract = load_contract("clients")
    for client in sim.client_records():
        assert contract.validate({**client, **INGESTED}) == []
    assert len({c["client_id"] for c in sim.client_records()}) == 20


def test_trades_match_contract_and_are_unique():
    _, trades = make_trades()
    contract = load_contract("trades")
    assert trades
    for trade in trades:
        assert contract.validate({**trade, **INGESTED}) == []
    assert len({t["trade_id"] for t in trades}) == len(trades)


def test_trades_are_priced_at_bid_or_ask():
    _, trades = make_trades()
    for trade in trades:
        assert trade["price"] == ("1234.57" if trade["side"] == "BUY" else "1234.55")


def test_commission_is_five_bps_of_notional():
    _, trades = make_trades()
    for t in trades:
        notional = Decimal(t["price"]) * Decimal(t["quantity"])
        assert abs(Decimal(t["commission"]) - notional * Decimal("0.0005")) <= Decimal("0.005")


def test_zero_probability_produces_no_trades():
    sim = TradeSimulator(num_clients=5, trade_probability=0.0, seed=1, now=NOW)
    assert sim.on_tick(TICK) == []
