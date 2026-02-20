from .base import build_extractor

ENDPOINT_NAME = "stats_player_shooting_season"

def get_spec(config_endpoints):
    spec = config_endpoints[ENDPOINT_NAME].copy()
    spec["name"] = ENDPOINT_NAME
    return build_extractor(spec)
