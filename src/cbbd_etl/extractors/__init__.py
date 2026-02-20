from importlib import import_module
from typing import Dict


ENDPOINT_MODULES = [
    "conferences",
    "conferences_history",
    "draft_picks",
    "draft_positions",
    "draft_teams",
    "games",
    "games_media",
    "games_players",
    "games_teams",
    "lines",
    "lines_providers",
    "lineups_game",
    "lineups_team",
    "plays_types",
    "plays_game",
    "plays_date",
    "plays_player",
    "plays_team",
    "plays_tournament",
    "substitutions_game",
    "substitutions_player",
    "substitutions_team",
    "rankings",
    "ratings_adjusted",
    "ratings_srs",
    "recruiting_players",
    "stats_player_shooting_season",
    "stats_player_season",
    "stats_team_shooting_season",
    "stats_team_season",
    "teams",
    "teams_roster",
    "venues",
]


def build_registry(config_endpoints: Dict):
    registry = {}
    for mod_name in ENDPOINT_MODULES:
        mod = import_module(f"cbbd_etl.extractors.{mod_name}")
        spec = mod.get_spec(config_endpoints)
        registry[spec.name] = spec
    return registry
