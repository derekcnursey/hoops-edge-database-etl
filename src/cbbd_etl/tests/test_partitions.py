from cbbd_etl.orchestrate import _bronze_partition, _silver_partition


class Dummy:
    def __init__(self, t):
        self.type = t


def test_bronze_partition_snapshot():
    assert _bronze_partition(Dummy("snapshot"), None, None, "2026-01-28") == "asof=2026-01-28"


def test_bronze_partition_season():
    assert _bronze_partition(Dummy("season"), 2024, None, "2026-01-28") == "season=2024/asof=2026-01-28"


def test_silver_partition_dim():
    assert _silver_partition("dim_teams", None, None, "2026-01-28") == "asof=2026-01-28"


def test_silver_partition_date():
    assert _silver_partition("fct_games", None, "2026-01-28", "2026-01-28") == "season=2026/date=2026-01-28"
