import pytest

from gtdp.contracts import load_all, load_contract

VALID_TRADE = {
    "trade_id": "t-1",
    "client_id": "CL00001",
    "symbol": "R_100",
    "side": "BUY",
    "quantity": "1.50",
    "price": "1234.56",
    "commission": "0.93",
    "executed_at": "2026-09-25T10:00:00+00:00",
    "tick_id": "abc",
    "source": "simulator",
    "ingested_at": "2026-09-25T10:00:01+00:00",
}


@pytest.fixture
def trades():
    return load_contract("trades")


def test_all_contracts_load():
    contracts = load_all()
    assert set(contracts) == {"ticks", "trades", "clients"}
    for contract in contracts.values():
        assert set(contract.primary_key) <= set(contract.column_names)
        assert "ingested_at" in contract.column_names


def test_valid_trade_passes(trades):
    assert trades.validate(VALID_TRADE) == []


def test_optional_field_may_be_missing(trades):
    record = {k: v for k, v in VALID_TRADE.items() if k != "tick_id"}
    assert trades.validate(record) == []


@pytest.mark.parametrize(
    ("change", "expected_error"),
    [
        ({"side": "HOLD"}, "side: 'HOLD' not in"),
        ({"quantity": "0"}, "quantity: '0' must be > 0"),
        ({"quantity": "-1"}, "quantity: '-1' must be > 0"),
        ({"commission": "-0.01"}, "commission: '-0.01' must be >= 0"),
        ({"price": "abc"}, "price: expected NUMERIC"),
        ({"executed_at": "yesterday"}, "executed_at: expected TIMESTAMP"),
        ({"client_id": None}, "client_id: required field is null"),
        ({"px": "1.0"}, "px: unexpected field"),
    ],
)
def test_invalid_trade_is_rejected(trades, change, expected_error):
    errors = trades.validate({**VALID_TRADE, **change})
    assert any(expected_error in e for e in errors), errors


def test_renamed_field_is_both_missing_and_unexpected(trades):
    record = dict(VALID_TRADE)
    record["px"] = record.pop("price")
    errors = trades.validate(record)
    assert any(e.startswith("px: unexpected field") for e in errors)
    assert any(e.startswith("price: required field") for e in errors)


def test_bool_is_not_an_int():
    ticks = load_contract("ticks")
    errors = ticks.validate({"pip_size": True})
    assert any("pip_size: expected INT64" in e for e in errors)


def test_bigquery_schema_modes(trades):
    schema = {f["name"]: f for f in trades.bigquery_schema()}
    assert schema["trade_id"]["mode"] == "REQUIRED"
    assert schema["tick_id"]["mode"] == "NULLABLE"
    assert schema["price"]["type"] == "NUMERIC"


def test_pii_columns_are_flagged():
    assert set(load_contract("clients").pii_columns) == {"full_name", "email"}
