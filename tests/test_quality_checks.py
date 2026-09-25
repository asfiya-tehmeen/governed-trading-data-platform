from datetime import UTC, datetime, timedelta

from gtdp.contracts import load_contract
from gtdp.quality.checks import (
    Status,
    check_completeness,
    check_duplicates,
    check_freshness,
    check_schema_drift,
    check_volume_anomaly,
    diff_schema,
)

NOW = datetime(2026, 9, 25, 12, 0, tzinfo=UTC)


def test_freshness_thresholds():
    def status(minutes_old):
        return check_freshness("t", NOW - timedelta(minutes=minutes_old), NOW, 10, 30).status

    assert status(5) == Status.PASS
    assert status(15) == Status.WARN
    assert status(31) == Status.FAIL


def test_freshness_fails_on_empty_table():
    assert check_freshness("t", None, NOW, 10, 30).status == Status.FAIL


def test_completeness_names_missing_keys():
    result = check_completeness("ticks", "symbol", {"R_10", "R_100"}, {"R_10"})
    assert result.status == Status.FAIL
    assert "R_100" in result.message
    assert check_completeness("ticks", "symbol", {"R_10"}, {"R_10", "R_25"}).status == Status.PASS


def test_duplicates():
    assert check_duplicates("trades", 0, 0).status == Status.PASS
    assert check_duplicates("trades", 3, 0).status == Status.WARN
    assert check_duplicates("trades", 3, 1).status == Status.FAIL


def test_volume_anomaly():
    history = [100, 104, 96, 101, 99, 98, 102, 100] * 4
    assert check_volume_anomaly("trades", 103, history).status == Status.PASS
    assert check_volume_anomaly("trades", 0, history).status == Status.FAIL
    assert check_volume_anomaly("trades", 300, history).status == Status.FAIL


def test_volume_anomaly_needs_history():
    result = check_volume_anomaly("trades", 0, [100] * 5)
    assert result.status == Status.PASS
    assert "not evaluated" in result.message


def test_volume_anomaly_flat_history():
    assert check_volume_anomaly("trades", 50, [50] * 30).status == Status.PASS
    assert check_volume_anomaly("trades", 0, [50] * 30).status == Status.FAIL


def _live_schema(contract):
    return {c.name: c.type for c in contract.columns}


def test_schema_drift_passes_when_schema_matches():
    trades = load_contract("trades")
    assert check_schema_drift(trades, _live_schema(trades)).status == Status.PASS


def test_schema_drift_detects_rename_and_type_change():
    trades = load_contract("trades")
    actual = _live_schema(trades)
    actual["px"] = actual.pop("price")
    actual["quantity"] = "FLOAT64"

    drift = diff_schema(trades, actual)
    assert drift.missing == ["price"]
    assert drift.unexpected == ["px"]
    assert drift.type_changed == [("quantity", "NUMERIC", "FLOAT64")]

    result = check_schema_drift(trades, actual)
    assert result.status == Status.FAIL
    assert "price" in result.message and "px" in result.message
