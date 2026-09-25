"""Runtime settings, read from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

TOPICS = ("ticks", "trades", "clients")

# Deriv's public market-data socket; no app_id or login needed.
DERIV_PUBLIC_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    sink: str
    landing_dir: Path
    gcp_project: str | None
    bq_location: str
    raw_dataset: str
    ops_dataset: str
    topic_prefix: str
    deriv_ws_url: str
    deriv_symbols: tuple[str, ...]
    sim_num_clients: int
    sim_trade_probability: float
    sim_seed: int | None
    alert_webhook_url: str | None

    def topic_id(self, topic: str) -> str:
        return f"{self.topic_prefix}-{topic}"

    def subscription_id(self, topic: str) -> str:
        return f"{self.topic_prefix}-{topic}-bq-loader"

    @classmethod
    def from_env(cls) -> Settings:
        seed = _env("SIM_SEED")
        return cls(
            sink=_env("GTDP_SINK", "local"),
            landing_dir=Path(_env("GTDP_LANDING_DIR", str(PROJECT_ROOT / "data" / "landing"))),
            gcp_project=_env("GCP_PROJECT"),
            bq_location=_env("BQ_LOCATION", "EU"),
            raw_dataset=_env("BQ_RAW_DATASET", "raw"),
            ops_dataset=_env("BQ_OPS_DATASET", "ops"),
            topic_prefix=_env("GTDP_TOPIC_PREFIX", "gtdp"),
            deriv_ws_url=_env("DERIV_WS_URL", DERIV_PUBLIC_WS_URL),
            deriv_symbols=tuple(
                s.strip() for s in _env("DERIV_SYMBOLS", "R_10,R_25,R_50,R_75,R_100").split(",")
            ),
            sim_num_clients=int(_env("SIM_NUM_CLIENTS", "50")),
            sim_trade_probability=float(_env("SIM_TRADE_PROBABILITY", "0.3")),
            sim_seed=int(seed) if seed is not None else None,
            alert_webhook_url=_env("ALERT_WEBHOOK_URL"),
        )

    def require_project(self) -> str:
        if not self.gcp_project:
            raise RuntimeError("GCP_PROJECT is not set")
        return self.gcp_project
