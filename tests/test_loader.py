import json
from datetime import UTC, datetime

from gtdp.contracts import load_contract
from gtdp.ingestion.loader import route_messages
from tests.test_contracts import VALID_TRADE

RECEIVED = datetime(2026, 9, 25, 12, 0, tzinfo=UTC)


def payload(record):
    return json.dumps({k: v for k, v in record.items() if k != "ingested_at"}).encode()


def test_valid_and_invalid_messages_are_split():
    bad = {**VALID_TRADE, "side": "HOLD"}
    routed = route_messages(
        load_contract("trades"),
        [("m1", payload(VALID_TRADE)), ("m2", payload(bad)), ("m3", b"not json"), ("m4", b"[1]")],
        received_at=RECEIVED,
    )
    assert len(routed.rows) == 1
    assert routed.rows[0]["ingested_at"] == RECEIVED.isoformat()
    assert routed.row_ids == ["t-1"]

    errors = {d["message_id"]: json.loads(d["errors"]) for d in routed.dead_letters}
    assert set(errors) == {"m2", "m3", "m4"}
    assert any("side" in e for e in errors["m2"])
    assert "not valid JSON" in errors["m3"][0]
    assert "expected an object" in errors["m4"][0]


def test_dead_letter_keeps_original_payload_and_contract_version():
    routed = route_messages(load_contract("trades"), [("m1", b'{"trade_id": "x"}')], RECEIVED)
    [dead] = routed.dead_letters
    assert dead["payload"] == '{"trade_id": "x"}'
    assert dead["target_table"] == "trades"
    assert dead["contract_version"] == 1


def test_composite_primary_key_row_id():
    client = {
        "client_id": "CL00001",
        "full_name": "A B",
        "email": "a@example.com",
        "country": "GB",
        "account_currency": "USD",
        "risk_tier": "LOW",
        "opened_at": "2024-01-01T00:00:00+00:00",
        "updated_at": "2026-09-25T00:00:00+00:00",
    }
    routed = route_messages(load_contract("clients"), [("m1", json.dumps(client).encode())])
    assert routed.dead_letters == []
    assert routed.row_ids == ["CL00001|2026-09-25T00:00:00+00:00"]
