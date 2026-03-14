from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict

import yaml


@dataclass
class Config:
    raw: Dict[str, Any]

    @property
    def bucket(self) -> str:
        return self.raw["bucket"]

    @property
    def region(self) -> str:
        return self.raw.get("region", "us-east-1")

    @property
    def api(self) -> Dict[str, Any]:
        return self.raw["api"]

    @property
    def seasons(self) -> Dict[str, Any]:
        return self.raw["seasons"]

    @property
    def endpoints(self) -> Dict[str, Any]:
        return self.raw["endpoints"]

    @property
    def s3_layout(self) -> Dict[str, str]:
        return self.raw["s3_layout"]


def load_config(path: str = "config.yaml") -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    bucket = raw.get("bucket")
    if bucket != "hoops-edge":
        raise ValueError("bucket must be 'hoops-edge' per requirements")
    api_cfg = raw.setdefault("api", {})
    overrides = {
        "rolling_window_days": os.getenv("CBBD_ETL_ROLLING_WINDOW_DAYS"),
        "max_concurrency": os.getenv("CBBD_ETL_MAX_CONCURRENCY"),
        "rate_limit_per_sec": os.getenv("CBBD_ETL_RATE_LIMIT_PER_SEC"),
        "fanout_batch_size": os.getenv("CBBD_ETL_FANOUT_BATCH_SIZE"),
    }
    for key, value in overrides.items():
        if value:
            api_cfg[key] = int(value)
    return Config(raw)


def get_api_token() -> str:
    token = os.getenv("CBBD_API_KEY") or os.getenv("BEARER_TOKEN")
    if not token:
        raise RuntimeError("Missing API token; set CBBD_API_KEY or BEARER_TOKEN")
    return token
