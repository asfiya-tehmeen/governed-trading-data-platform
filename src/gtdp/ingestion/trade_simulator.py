"""Synthetic clients and trades, priced off live Deriv ticks.

Real client trades are not available, so each incoming tick may trigger a few
simulated executions at that tick's bid/ask. Prices are therefore real market
prices; only who traded and how much is synthetic.
"""

from __future__ import annotations

import random
import uuid
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

FIRST_NAMES = [
    "Amara",
    "Bilal",
    "Chen",
    "Dana",
    "Elif",
    "Farah",
    "Goran",
    "Hana",
    "Ivan",
    "Jia",
    "Kofi",
    "Lina",
    "Mateo",
    "Nadia",
    "Omar",
    "Priya",
    "Quinn",
    "Rosa",
    "Sami",
    "Tariq",
]
LAST_NAMES = [
    "Abiodun",
    "Bauer",
    "Costa",
    "Dubois",
    "Eriksen",
    "Fernandes",
    "Gupta",
    "Haddad",
    "Ivanova",
    "Jensen",
    "Khan",
    "Lim",
    "Moreau",
    "Nowak",
    "Okafor",
    "Petrov",
]
COUNTRIES = ["GB", "DE", "FR", "MT", "AE", "MY", "ZA", "BR", "IN", "SG"]
CURRENCIES = ["USD", "EUR", "GBP"]

# Quantity range per risk tier: higher-risk clients trade larger size.
TIER_QUANTITY = {"LOW": (0.1, 2.0), "MEDIUM": (0.5, 5.0), "HIGH": (1.0, 20.0)}
TIER_WEIGHTS = {"LOW": 0.5, "MEDIUM": 0.35, "HIGH": 0.15}

COMMISSION_RATE = Decimal("0.0005")
CENT = Decimal("0.01")


def _to_decimal(value: float, places: int) -> Decimal:
    return Decimal(str(value)).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


class TradeSimulator:
    def __init__(
        self,
        num_clients: int = 50,
        trade_probability: float = 0.3,
        seed: int | None = None,
        now: datetime | None = None,
    ):
        self.rng = random.Random(seed)
        self.trade_probability = trade_probability
        self.clients = [self._make_client(i, now or datetime.now(UTC)) for i in range(num_clients)]

    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _make_client(self, index: int, now: datetime) -> dict[str, Any]:
        first, last = self.rng.choice(FIRST_NAMES), self.rng.choice(LAST_NAMES)
        tiers, weights = zip(*TIER_WEIGHTS.items(), strict=True)
        opened_at = now - timedelta(days=self.rng.randint(30, 1500))
        return {
            "client_id": f"CL{index + 1:05d}",
            "full_name": f"{first} {last}",
            "email": f"{first}.{last}{index + 1}@example.com".lower(),
            "country": self.rng.choice(COUNTRIES),
            "account_currency": self.rng.choice(CURRENCIES),
            "risk_tier": self.rng.choices(tiers, weights)[0],
            "opened_at": opened_at.isoformat(),
            "updated_at": now.isoformat(),
        }

    def client_records(self) -> list[dict[str, Any]]:
        return [dict(c) for c in self.clients]

    def on_tick(self, tick: dict[str, Any]) -> list[dict[str, Any]]:
        """Maybe generate trades for this tick. Usually 0, sometimes 1-3."""
        trades = []
        while self.rng.random() < self.trade_probability and len(trades) < 3:
            trades.append(self._make_trade(tick))
        return trades

    def _make_trade(self, tick: dict[str, Any]) -> dict[str, Any]:
        client = self.rng.choice(self.clients)
        side = self.rng.choice(["BUY", "SELL"])
        # Buy at the ask, sell at the bid; fall back to the mid quote.
        raw_price = tick.get("ask" if side == "BUY" else "bid") or tick["quote"]
        price = _to_decimal(raw_price, tick["pip_size"])
        low, high = TIER_QUANTITY[client["risk_tier"]]
        quantity = _to_decimal(self.rng.uniform(low, high), 2)
        commission = (price * quantity * COMMISSION_RATE).quantize(CENT, rounding=ROUND_HALF_UP)
        return {
            "trade_id": self._uuid(),
            "client_id": client["client_id"],
            "symbol": tick["symbol"],
            "side": side,
            "quantity": str(quantity),
            "price": str(price),
            "commission": str(commission),
            "executed_at": tick["quoted_at"],
            "tick_id": tick["tick_id"],
            "source": "simulator",
        }
