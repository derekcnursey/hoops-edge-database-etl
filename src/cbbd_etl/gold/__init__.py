"""Gold layer transforms for the cbbd_etl pipeline.

This package transforms silver-layer star-schema tables into analytics-ready
gold datasets optimized for college basketball analysis, prediction modeling,
and betting research.

Each module exposes a ``build(cfg, season)`` function that reads silver tables
from S3, applies transforms, and returns a ``pyarrow.Table``.
"""

from __future__ import annotations

from .team_power_rankings import build as build_team_power_rankings
from .game_predictions_features import build as build_game_predictions_features
from .player_season_impact import build as build_player_season_impact
from .market_lines_analysis import build as build_market_lines_analysis
from .team_season_summary import build as build_team_season_summary

GOLD_TRANSFORMS = {
    "team_power_rankings": build_team_power_rankings,
    "game_predictions_features": build_game_predictions_features,
    "player_season_impact": build_player_season_impact,
    "market_lines_analysis": build_market_lines_analysis,
    "team_season_summary": build_team_season_summary,
}

__all__ = [
    "GOLD_TRANSFORMS",
    "build_team_power_rankings",
    "build_game_predictions_features",
    "build_player_season_impact",
    "build_market_lines_analysis",
    "build_team_season_summary",
]
