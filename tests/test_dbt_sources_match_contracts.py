"""The dbt sources and the data contracts must describe the same columns."""

import pytest
import yaml

from gtdp.config import PROJECT_ROOT
from gtdp.contracts import load_all

SOURCES = PROJECT_ROOT / "dbt" / "models" / "staging" / "_sources.yml"
TABLES = sorted(load_all())


def _dbt_tables():
    doc = yaml.safe_load(SOURCES.read_text(encoding="utf-8"))
    [raw] = [s for s in doc["sources"] if s["name"] == "raw"]
    return {t["name"]: t for t in raw["tables"]}


@pytest.mark.parametrize("table", TABLES)
def test_source_columns_match_contract(table):
    contract = load_all()[table]
    assert [c["name"] for c in _dbt_tables()[table]["columns"]] == contract.column_names


@pytest.mark.parametrize("table", TABLES)
def test_source_freshness_matches_contract(table):
    contract = load_all()[table]
    freshness = _dbt_tables()[table]["config"]["freshness"]
    minutes = {"minute": 1, "hour": 60, "day": 1440}
    for level in ("warn", "error"):
        spec = freshness[f"{level}_after"]
        expected = contract.freshness[f"{level}_after_minutes"]
        assert spec["count"] * minutes[spec["period"]] == expected


@pytest.mark.parametrize("table", TABLES)
def test_pii_columns_are_tagged_in_dbt(table):
    contract = load_all()[table]
    columns = _dbt_tables()[table]["columns"]
    dbt_pii = {c["name"] for c in columns if c.get("config", {}).get("meta", {}).get("pii")}
    assert dbt_pii == set(contract.pii_columns)
