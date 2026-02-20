from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class EndpointSpec:
    name: str
    path: str
    type: str
    season_param: Optional[str] = None
    date_param: Optional[str] = None
    start_date_param: Optional[str] = None
    end_date_param: Optional[str] = None


def build_extractor(spec: Dict[str, Any]) -> EndpointSpec:
    return EndpointSpec(
        name=spec["name"],
        path=spec["path"],
        type=spec["type"],
        season_param=spec.get("season_param"),
        date_param=spec.get("date_param"),
        start_date_param=spec.get("start_date_param"),
        end_date_param=spec.get("end_date_param"),
    )
