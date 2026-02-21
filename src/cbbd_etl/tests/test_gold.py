"""Tests for gold-layer transform logic.

Uses synthetic PyArrow tables as fixtures to test each gold module's
build function in isolation, mocking S3 reads via monkeypatching.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch
import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from cbbd_etl.config import Config
from cbbd_etl.normalize import normalize_records, TABLE_SPECS


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _make_config() -> Config:
    """Create a minimal Config for testing."""
    return Config({
        "bucket": "hoops-edge",
        "region": "us-east-1",
        "api": {"base_url": "https://test", "timeout_seconds": 10},
        "seasons": {"start": 2024, "end": 2024},
        "endpoints": {},
        "s3_layout": {
            "raw_prefix": "raw",
            "bronze_prefix": "bronze",
            "silver_prefix": "silver",
            "gold_prefix": "gold",
            "ref_prefix": "ref",
            "meta_prefix": "meta",
            "deadletter_prefix": "deadletter",
            "tmp_prefix": "tmp",
            "athena_prefix": "athena",
        },
    })


def _table_to_s3_bytes(table: pa.Table) -> bytes:
    """Serialize a PyArrow table to Parquet bytes (mimics S3 object body)."""
    sink = io.BytesIO()
    pq.write_table(table, sink)
    sink.seek(0)
    return sink.read()


def _make_dim_teams() -> pa.Table:
    """Synthetic dim_teams."""
    return pa.table({
        "teamId": pa.array([1, 2, 3], type=pa.int64()),
        "school": pa.array(["Duke", "UNC", "Kentucky"], type=pa.string()),
        "conference": pa.array(["ACC", "ACC", "SEC"], type=pa.string()),
    })


def _make_ratings_adjusted(season: int = 2024) -> pa.Table:
    """Synthetic fct_ratings_adjusted."""
    return pa.table({
        "teamid": pa.array([1, 2, 3], type=pa.int64()),
        "season": pa.array([season, season, season], type=pa.int32()),
        "team": pa.array(["Duke", "UNC", "Kentucky"], type=pa.string()),
        "conference": pa.array(["ACC", "ACC", "SEC"], type=pa.string()),
        "offenserating": pa.array([115.0, 110.0, 112.0], type=pa.float64()),
        "defenserating": pa.array([95.0, 98.0, 97.0], type=pa.float64()),
        "netrating": pa.array([20.0, 12.0, 15.0], type=pa.float64()),
        "ranking_offense": pa.array([5, 20, 10], type=pa.int64()),
        "ranking_defense": pa.array([3, 15, 8], type=pa.int64()),
        "ranking_net": pa.array([1, 10, 5], type=pa.int64()),
    })


def _make_ratings_srs(season: int = 2024) -> pa.Table:
    """Synthetic fct_ratings_srs."""
    return pa.table({
        "teamId": pa.array([1, 2, 3], type=pa.int64()),
        "season": pa.array([season, season, season], type=pa.int32()),
        "rating": pa.array([25.0, 15.0, 18.0], type=pa.float64()),
    })


def _make_rankings(season: int = 2024) -> pa.Table:
    """Synthetic fct_rankings."""
    return pa.table({
        "season": pa.array([season, season, season, season], type=pa.int32()),
        "pollDate": pa.array(["2024-03-01", "2024-03-01", "2024-02-15", "2024-03-01"], type=pa.string()),
        "pollType": pa.array(["AP Top 25", "AP Top 25", "AP Top 25", "Coaches Poll"], type=pa.string()),
        "teamId": pa.array([1, 2, 1, 1], type=pa.int64()),
        "week": pa.array([15, 15, 13, 15], type=pa.int64()),
        "ranking": pa.array([1, 5, 2, 1], type=pa.int64()),
        "points": pa.array([1500, 1200, 1400, 1480], type=pa.int64()),
        "firstPlaceVotes": pa.array([50, 5, 40, 48], type=pa.int64()),
    })


def _make_pbp_rollup(season: int = 2024) -> pa.Table:
    """Synthetic fct_pbp_team_daily_rollup."""
    return pa.table({
        "teamid": pa.array([1, 2, 3], type=pa.int64()),
        "games_played": pa.array([30, 28, 32], type=pa.int64()),
        "team_points_total": pa.array([2400.0, 2100.0, 2300.0], type=pa.float64()),
        "opp_points_total": pa.array([1900.0, 1950.0, 2000.0], type=pa.float64()),
        "team_possessions": pa.array([2000.0, 1800.0, 2100.0], type=pa.float64()),
        "opp_possessions": pa.array([2000.0, 1800.0, 2100.0], type=pa.float64()),
        "game_minutes_total": pa.array([1200.0, 1120.0, 1280.0], type=pa.float64()),
        "team_points_per_game": pa.array([80.0, 75.0, 71.875], type=pa.float64()),
        "opp_points_per_game": pa.array([63.33, 69.64, 62.5], type=pa.float64()),
        "pace": pa.array([66.67, 64.29, 65.63], type=pa.float64()),
        "team_efg_pct": pa.array([0.55, 0.50, 0.52], type=pa.float64()),
        "team_tov_ratio": pa.array([0.15, 0.18, 0.16], type=pa.float64()),
        "team_oreb_pct": pa.array([0.30, 0.28, 0.32], type=pa.float64()),
        "team_ft_rate": pa.array([0.35, 0.30, 0.33], type=pa.float64()),
        "opp_efg_pct": pa.array([0.45, 0.48, 0.47], type=pa.float64()),
        "opp_tov_ratio": pa.array([0.20, 0.17, 0.19], type=pa.float64()),
        "opp_oreb_pct": pa.array([0.25, 0.27, 0.24], type=pa.float64()),
        "opp_ft_rate": pa.array([0.28, 0.32, 0.30], type=pa.float64()),
    })


def _make_pbp_rollup_adj(season: int = 2024) -> pa.Table:
    """Synthetic fct_pbp_team_daily_rollup_adj."""
    return pa.table({
        "teamid": pa.array([1, 2, 3], type=pa.int64()),
        "adj_off_eff": pa.array([118.0, 108.0, 112.0], type=pa.float64()),
        "adj_def_eff": pa.array([92.0, 100.0, 96.0], type=pa.float64()),
        "adj_net_eff": pa.array([26.0, 8.0, 16.0], type=pa.float64()),
    })


def _make_games(season: int = 2024) -> pa.Table:
    """Synthetic fct_games."""
    return pa.table({
        "gameId": pa.array([100, 101, 102], type=pa.int64()),
        "season": pa.array([season, season, season], type=pa.int32()),
        "startDate": pa.array(["2024-01-10", "2024-01-15", "2024-02-01"], type=pa.string()),
        "homeTeamId": pa.array([1, 2, 3], type=pa.int64()),
        "awayTeamId": pa.array([2, 3, 1], type=pa.int64()),
        "homeScore": pa.array([85, 70, 75], type=pa.int64()),
        "awayScore": pa.array([78, 80, 72], type=pa.int64()),
    })


def _make_lines(season: int = 2024) -> pa.Table:
    """Synthetic fct_lines."""
    return pa.table({
        "gameId": pa.array([100, 101, 102], type=pa.int64()),
        "provider": pa.array(["Bovada", "Bovada", "Bovada"], type=pa.string()),
        "spread": pa.array([-5.5, 3.0, -2.0], type=pa.float64()),
        "overUnder": pa.array([150.0, 145.0, 148.0], type=pa.float64()),
        "homeMoneyline": pa.array([-200.0, 150.0, -130.0], type=pa.float64()),
        "awayMoneyline": pa.array([170.0, -180.0, 110.0], type=pa.float64()),
    })


def _make_player_stats(season: int = 2024) -> pa.Table:
    """Synthetic fct_player_season_stats."""
    return pa.table({
        "playerId": pa.array([10, 20, 30], type=pa.int64()),
        "season": pa.array([season, season, season], type=pa.int32()),
        "team": pa.array(["Duke", "UNC", "Kentucky"], type=pa.string()),
        "conference": pa.array(["ACC", "ACC", "SEC"], type=pa.string()),
        "games": pa.array([30, 28, 32], type=pa.int64()),
        "minutes": pa.array([900.0, 800.0, 960.0], type=pa.float64()),
        "points": pa.array([600.0, 400.0, 480.0], type=pa.float64()),
        "rebounds": pa.array([180.0, 250.0, 300.0], type=pa.float64()),
        "assists": pa.array([150.0, 100.0, 60.0], type=pa.float64()),
        "steals": pa.array([45.0, 30.0, 28.0], type=pa.float64()),
        "blocks": pa.array([10.0, 60.0, 80.0], type=pa.float64()),
        "turnovers": pa.array([75.0, 50.0, 40.0], type=pa.float64()),
        "fieldGoalsMade": pa.array([200.0, 150.0, 180.0], type=pa.float64()),
        "fieldGoalsAttempted": pa.array([420.0, 340.0, 400.0], type=pa.float64()),
        "threePointFieldGoalsMade": pa.array([80.0, 40.0, 30.0], type=pa.float64()),
        "threePointFieldGoalsAttempted": pa.array([200.0, 120.0, 100.0], type=pa.float64()),
        "freeThrowsMade": pa.array([120.0, 60.0, 90.0], type=pa.float64()),
        "freeThrowsAttempted": pa.array([150.0, 80.0, 120.0], type=pa.float64()),
    })


def _make_recruiting(season: int = 2024) -> pa.Table:
    """Synthetic fct_recruiting_players."""
    return pa.table({
        "playerId": pa.array([10, 20], type=pa.int64()),
        "season": pa.array([season, season], type=pa.int32()),
        "ranking": pa.array([15, 50], type=pa.int64()),
        "stars": pa.array([5, 4], type=pa.int64()),
        "rating": pa.array([0.99, 0.95], type=pa.float64()),
        "committedTo": pa.array(["Duke", "UNC"], type=pa.string()),
    })


# ---------------------------------------------------------------------------
# Helper to mock S3IO
# ---------------------------------------------------------------------------

class MockS3IO:
    """Mock S3IO that returns predefined tables based on key prefix."""

    def __init__(self, table_data: Dict[str, pa.Table]) -> None:
        self._data = table_data

    def list_keys(self, prefix: str) -> List[str]:
        """Return fake keys for tables matching the prefix."""
        for name in self._data:
            if name in prefix:
                return [f"{prefix}part-00000000.parquet"]
        return []

    def get_object_bytes(self, key: str) -> bytes:
        """Return serialized Parquet for the matched table."""
        for name, table in self._data.items():
            if name in key:
                return _table_to_s3_bytes(table)
        raise KeyError(f"No mock data for key: {key}")


def _patch_s3io(table_data: Dict[str, pa.Table]):
    """Create a patch context that replaces S3IO with MockS3IO."""
    mock_instance = MockS3IO(table_data)

    def fake_init(self, bucket, region):
        self.bucket = bucket
        self.region = region
        self._mock = mock_instance

    def fake_list_keys(self, prefix):
        return mock_instance.list_keys(prefix)

    def fake_get_object_bytes(self, key):
        return mock_instance.get_object_bytes(key)

    return [
        patch.object(
            __import__("cbbd_etl.s3_io", fromlist=["S3IO"]).S3IO,
            "__init__",
            fake_init,
        ),
        patch.object(
            __import__("cbbd_etl.s3_io", fromlist=["S3IO"]).S3IO,
            "list_keys",
            fake_list_keys,
        ),
        patch.object(
            __import__("cbbd_etl.s3_io", fromlist=["S3IO"]).S3IO,
            "get_object_bytes",
            fake_get_object_bytes,
        ),
    ]


# ---------------------------------------------------------------------------
# Tests: team_power_rankings
# ---------------------------------------------------------------------------

class TestTeamPowerRankings:
    """Tests for the team_power_rankings gold transform."""

    def test_basic_build(self):
        """Build with all source tables populated."""
        from cbbd_etl.gold.team_power_rankings import build

        table_data = {
            "fct_ratings_adjusted": _make_ratings_adjusted(),
            "fct_ratings_srs": _make_ratings_srs(),
            "fct_rankings": _make_rankings(),
            "fct_pbp_team_daily_rollup": _make_pbp_rollup(),
            "fct_pbp_team_daily_rollup_adj": _make_pbp_rollup_adj(),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            assert result.num_rows == 3
            cols = result.column_names
            assert "teamId" in cols
            assert "season" in cols
            assert "composite_rank" in cols
            assert "adj_net_rating" in cols
            assert "srs_rating" in cols
            assert "ap_rank" in cols
            assert "pbp_off_eff" in cols

            # Check Duke is present
            tids = result.column("teamId").to_pylist()
            assert 1 in tids

            # Check season column
            seasons = result.column("season").to_pylist()
            assert all(s == 2024 for s in seasons)
        finally:
            for p in patches:
                p.stop()

    def test_empty_season(self):
        """Build with no ratings data returns empty table."""
        from cbbd_etl.gold.team_power_rankings import build

        table_data = {
            "fct_ratings_adjusted": pa.table({}),
            "fct_ratings_srs": pa.table({}),
            "fct_rankings": pa.table({}),
            "fct_pbp_team_daily_rollup": pa.table({}),
            "fct_pbp_team_daily_rollup_adj": pa.table({}),
            "dim_teams": pa.table({}),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            assert result.num_rows == 0
        finally:
            for p in patches:
                p.stop()

    def test_missing_srs(self):
        """Build succeeds when SRS data is missing."""
        from cbbd_etl.gold.team_power_rankings import build

        table_data = {
            "fct_ratings_adjusted": _make_ratings_adjusted(),
            "fct_ratings_srs": pa.table({}),
            "fct_rankings": pa.table({}),
            "fct_pbp_team_daily_rollup": pa.table({}),
            "fct_pbp_team_daily_rollup_adj": pa.table({}),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            assert result.num_rows == 3
            srs = result.column("srs_rating").to_pylist()
            assert all(v is None for v in srs)
        finally:
            for p in patches:
                p.stop()

    def test_composite_rank_computed(self):
        """Composite rank is non-null when sources are available."""
        from cbbd_etl.gold.team_power_rankings import build

        table_data = {
            "fct_ratings_adjusted": _make_ratings_adjusted(),
            "fct_ratings_srs": _make_ratings_srs(),
            "fct_rankings": pa.table({}),
            "fct_pbp_team_daily_rollup": _make_pbp_rollup(),
            "fct_pbp_team_daily_rollup_adj": _make_pbp_rollup_adj(),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            composites = result.column("composite_rank").to_pylist()
            assert any(c is not None for c in composites)
            # Duke (teamId=1) should have the highest composite rank
            tids = result.column("teamId").to_pylist()
            duke_idx = tids.index(1)
            unc_idx = tids.index(2)
            assert composites[duke_idx] > composites[unc_idx]
        finally:
            for p in patches:
                p.stop()


# ---------------------------------------------------------------------------
# Tests: game_predictions_features
# ---------------------------------------------------------------------------

class TestGamePredictionsFeatures:
    """Tests for the game_predictions_features gold transform."""

    def test_basic_build(self):
        """Build produces two rows per game (home and away)."""
        from cbbd_etl.gold.game_predictions_features import build

        table_data = {
            "fct_games": _make_games(),
            "fct_ratings_adjusted": _make_ratings_adjusted(),
            "fct_ratings_srs": _make_ratings_srs(),
            "fct_lines": _make_lines(),
            "fct_pbp_team_daily_rollup": _make_pbp_rollup(),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            # 3 games * 2 sides = 6 rows
            assert result.num_rows == 6

            is_home = result.column("is_home").to_pylist()
            assert sum(1 for h in is_home if h is True) == 3
            assert sum(1 for h in is_home if h is False) == 3
        finally:
            for p in patches:
                p.stop()

    def test_labels_computed(self):
        """team_score, opp_score, team_win are correctly derived."""
        from cbbd_etl.gold.game_predictions_features import build

        table_data = {
            "fct_games": _make_games(),
            "fct_ratings_adjusted": pa.table({}),
            "fct_ratings_srs": pa.table({}),
            "fct_lines": pa.table({}),
            "fct_pbp_team_daily_rollup": pa.table({}),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            gids = result.column("gameId").to_pylist()
            tids = result.column("teamId").to_pylist()
            is_homes = result.column("is_home").to_pylist()
            t_scores = result.column("team_score").to_pylist()
            wins = result.column("team_win").to_pylist()

            # Game 100: home(1) 85 - away(2) 78 -> home wins
            for i in range(len(gids)):
                if gids[i] == 100 and is_homes[i] is True:
                    assert t_scores[i] == 85
                    assert wins[i] is True
                elif gids[i] == 100 and is_homes[i] is False:
                    assert t_scores[i] == 78
                    assert wins[i] is False
        finally:
            for p in patches:
                p.stop()

    def test_conference_game_detection(self):
        """is_conference_game is True when both teams share a conference."""
        from cbbd_etl.gold.game_predictions_features import build

        table_data = {
            "fct_games": _make_games(),
            "fct_ratings_adjusted": pa.table({}),
            "fct_ratings_srs": pa.table({}),
            "fct_lines": pa.table({}),
            "fct_pbp_team_daily_rollup": pa.table({}),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            gids = result.column("gameId").to_pylist()
            conf_games = result.column("is_conference_game").to_pylist()

            # Game 100: Duke (ACC) vs UNC (ACC) -> conference game
            for i in range(len(gids)):
                if gids[i] == 100:
                    assert conf_games[i] is True
                # Game 101: UNC (ACC) vs Kentucky (SEC) -> not conference
                elif gids[i] == 101:
                    assert conf_games[i] is False
        finally:
            for p in patches:
                p.stop()

    def test_empty_games(self):
        """Returns empty table when no games exist."""
        from cbbd_etl.gold.game_predictions_features import build

        table_data = {
            "fct_games": pa.table({}),
            "fct_ratings_adjusted": pa.table({}),
            "fct_ratings_srs": pa.table({}),
            "fct_lines": pa.table({}),
            "fct_pbp_team_daily_rollup": pa.table({}),
            "dim_teams": pa.table({}),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            assert result.num_rows == 0
        finally:
            for p in patches:
                p.stop()

    def test_spread_flipped_for_away(self):
        """Spread is negated for the away team side."""
        from cbbd_etl.gold.game_predictions_features import build

        table_data = {
            "fct_games": _make_games(),
            "fct_ratings_adjusted": pa.table({}),
            "fct_ratings_srs": pa.table({}),
            "fct_lines": _make_lines(),
            "fct_pbp_team_daily_rollup": pa.table({}),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            gids = result.column("gameId").to_pylist()
            spreads = result.column("spread").to_pylist()
            is_homes = result.column("is_home").to_pylist()

            for i in range(len(gids)):
                if gids[i] == 100 and is_homes[i] is True:
                    assert spreads[i] == -5.5  # home spread
                elif gids[i] == 100 and is_homes[i] is False:
                    assert spreads[i] == 5.5  # away = negated
        finally:
            for p in patches:
                p.stop()


# ---------------------------------------------------------------------------
# Tests: player_season_impact
# ---------------------------------------------------------------------------

class TestPlayerSeasonImpact:
    """Tests for the player_season_impact gold transform."""

    def test_basic_build(self):
        """Build produces one row per player with derived metrics."""
        from cbbd_etl.gold.player_season_impact import build

        table_data = {
            "fct_player_season_stats": _make_player_stats(),
            "fct_recruiting_players": _make_recruiting(),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            assert result.num_rows == 3
            cols = result.column_names
            assert "playerId" in cols
            assert "ppg" in cols
            assert "efg_pct" in cols
            assert "true_shooting" in cols
            assert "per_40_pts" in cols
        finally:
            for p in patches:
                p.stop()

    def test_derived_metrics(self):
        """PPG, FG%, and per-40 stats are computed correctly."""
        from cbbd_etl.gold.player_season_impact import build

        table_data = {
            "fct_player_season_stats": _make_player_stats(),
            "fct_recruiting_players": pa.table({}),
            "dim_teams": pa.table({}),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            pids = result.column("playerId").to_pylist()
            ppgs = result.column("ppg").to_pylist()
            fg_pcts = result.column("fg_pct").to_pylist()
            per40 = result.column("per_40_pts").to_pylist()

            # Player 10: 600 pts / 30 games = 20.0 ppg
            idx = pids.index(10)
            assert abs(ppgs[idx] - 20.0) < 0.01
            # FG%: 200/420 = 0.476
            assert abs(fg_pcts[idx] - (200 / 420)) < 0.01
            # Per-40: 600/900 * 40 = 26.67
            assert abs(per40[idx] - (600 / 900 * 40)) < 0.1
        finally:
            for p in patches:
                p.stop()

    def test_recruiting_enrichment(self):
        """Recruiting rank and stars are joined from recruiting data."""
        from cbbd_etl.gold.player_season_impact import build

        table_data = {
            "fct_player_season_stats": _make_player_stats(),
            "fct_recruiting_players": _make_recruiting(),
            "dim_teams": pa.table({}),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            pids = result.column("playerId").to_pylist()
            ranks = result.column("recruiting_rank").to_pylist()
            stars = result.column("recruiting_stars").to_pylist()

            # Player 10 has recruiting rank 15, 5 stars
            idx = pids.index(10)
            assert ranks[idx] == 15
            assert stars[idx] == 5

            # Player 30 has no recruiting data
            idx = pids.index(30)
            assert ranks[idx] is None
            assert stars[idx] is None
        finally:
            for p in patches:
                p.stop()

    def test_empty_stats(self):
        """Returns empty table with no player stats."""
        from cbbd_etl.gold.player_season_impact import build

        table_data = {
            "fct_player_season_stats": pa.table({}),
            "fct_recruiting_players": pa.table({}),
            "dim_teams": pa.table({}),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            assert result.num_rows == 0
        finally:
            for p in patches:
                p.stop()

    def test_zero_minutes_handling(self):
        """Per-40 stats are None when minutes are zero."""
        from cbbd_etl.gold.player_season_impact import build

        stats = pa.table({
            "playerId": pa.array([99], type=pa.int64()),
            "season": pa.array([2024], type=pa.int32()),
            "team": pa.array(["Test"], type=pa.string()),
            "conference": pa.array(["Test"], type=pa.string()),
            "games": pa.array([5], type=pa.int64()),
            "minutes": pa.array([0.0], type=pa.float64()),
            "points": pa.array([0.0], type=pa.float64()),
            "rebounds": pa.array([0.0], type=pa.float64()),
            "assists": pa.array([0.0], type=pa.float64()),
            "steals": pa.array([0.0], type=pa.float64()),
            "blocks": pa.array([0.0], type=pa.float64()),
            "turnovers": pa.array([0.0], type=pa.float64()),
            "fieldGoalsMade": pa.array([0.0], type=pa.float64()),
            "fieldGoalsAttempted": pa.array([0.0], type=pa.float64()),
            "threePointFieldGoalsMade": pa.array([0.0], type=pa.float64()),
            "threePointFieldGoalsAttempted": pa.array([0.0], type=pa.float64()),
            "freeThrowsMade": pa.array([0.0], type=pa.float64()),
            "freeThrowsAttempted": pa.array([0.0], type=pa.float64()),
        })
        table_data = {
            "fct_player_season_stats": stats,
            "fct_recruiting_players": pa.table({}),
            "dim_teams": pa.table({}),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            assert result.num_rows == 1
            per40 = result.column("per_40_pts").to_pylist()
            assert per40[0] is None
        finally:
            for p in patches:
                p.stop()


# ---------------------------------------------------------------------------
# Tests: market_lines_analysis
# ---------------------------------------------------------------------------

class TestMarketLinesAnalysis:
    """Tests for the market_lines_analysis gold transform."""

    def test_basic_build(self):
        """Build produces one row per game-provider with derived ATS metrics."""
        from cbbd_etl.gold.market_lines_analysis import build

        table_data = {
            "fct_lines": _make_lines(),
            "fct_games": _make_games(),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            assert result.num_rows == 3
            cols = result.column_names
            assert "home_covered" in cols
            assert "over_hit" in cols
            assert "ats_margin" in cols
            assert "spread_error" in cols
        finally:
            for p in patches:
                p.stop()

    def test_ats_calculations(self):
        """ATS margin and home_covered are computed correctly."""
        from cbbd_etl.gold.market_lines_analysis import build

        table_data = {
            "fct_lines": _make_lines(),
            "fct_games": _make_games(),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            gids = result.column("gameId").to_pylist()
            ats_margins = result.column("ats_margin").to_pylist()
            covered = result.column("home_covered").to_pylist()
            over_hit = result.column("over_hit").to_pylist()

            # Game 100: home 85-78, spread -5.5
            # home_margin = 7, ats_margin = 7 + (-5.5) = 1.5 -> covered
            idx = gids.index(100)
            assert abs(ats_margins[idx] - 1.5) < 0.01
            assert covered[idx] is True

            # Game 100: total = 163, OU = 150 -> over hit
            assert over_hit[idx] is True

            # Game 101: home 70-80, spread 3.0
            # home_margin = -10, ats_margin = -10 + 3.0 = -7.0 -> not covered
            idx = gids.index(101)
            assert abs(ats_margins[idx] - (-7.0)) < 0.01
            assert covered[idx] is False
        finally:
            for p in patches:
                p.stop()

    def test_empty_lines(self):
        """Returns empty table when no lines exist."""
        from cbbd_etl.gold.market_lines_analysis import build

        table_data = {
            "fct_lines": pa.table({}),
            "fct_games": _make_games(),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            assert result.num_rows == 0
        finally:
            for p in patches:
                p.stop()

    def test_team_names_enriched(self):
        """Home and away team names come from dim_teams."""
        from cbbd_etl.gold.market_lines_analysis import build

        table_data = {
            "fct_lines": _make_lines(),
            "fct_games": _make_games(),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            gids = result.column("gameId").to_pylist()
            homes = result.column("home_team").to_pylist()
            aways = result.column("away_team").to_pylist()

            # Game 100: Duke (home) vs UNC (away)
            idx = gids.index(100)
            assert homes[idx] == "Duke"
            assert aways[idx] == "UNC"
        finally:
            for p in patches:
                p.stop()


# ---------------------------------------------------------------------------
# Tests: team_season_summary
# ---------------------------------------------------------------------------

class TestTeamSeasonSummary:
    """Tests for the team_season_summary gold transform."""

    def test_basic_build(self):
        """Build produces one row per team with record and stats."""
        from cbbd_etl.gold.team_season_summary import build

        table_data = {
            "dim_teams": _make_dim_teams(),
            "fct_games": _make_games(),
            "fct_ratings_adjusted": _make_ratings_adjusted(),
            "fct_ratings_srs": _make_ratings_srs(),
            "fct_pbp_team_daily_rollup": _make_pbp_rollup(),
            "fct_recruiting_players": _make_recruiting(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            assert result.num_rows == 3
            cols = result.column_names
            assert "wins" in cols
            assert "losses" in cols
            assert "win_pct" in cols
            assert "conf_wins" in cols
            assert "adj_net_rating" in cols
        finally:
            for p in patches:
                p.stop()

    def test_win_loss_record(self):
        """W/L record is computed correctly from game scores."""
        from cbbd_etl.gold.team_season_summary import build

        table_data = {
            "dim_teams": _make_dim_teams(),
            "fct_games": _make_games(),
            "fct_ratings_adjusted": _make_ratings_adjusted(),
            "fct_ratings_srs": pa.table({}),
            "fct_pbp_team_daily_rollup": pa.table({}),
            "fct_recruiting_players": pa.table({}),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            tids = result.column("teamId").to_pylist()
            wins = result.column("wins").to_pylist()
            losses = result.column("losses").to_pylist()

            # Game 100: Duke(1) 85 > UNC(2) 78 -> Duke wins
            # Game 101: UNC(2) 70 < Kentucky(3) 80 -> Kentucky wins
            # Game 102: Kentucky(3) 75 > Duke(1) 72 -> Kentucky wins
            duke_idx = tids.index(1)
            unc_idx = tids.index(2)
            ky_idx = tids.index(3)

            assert wins[duke_idx] == 1
            assert losses[duke_idx] == 1
            assert wins[unc_idx] == 0
            assert losses[unc_idx] == 2
            assert wins[ky_idx] == 2
            assert losses[ky_idx] == 0
        finally:
            for p in patches:
                p.stop()

    def test_conference_record(self):
        """Conference W/L is computed for games between same-conference teams."""
        from cbbd_etl.gold.team_season_summary import build

        table_data = {
            "dim_teams": _make_dim_teams(),
            "fct_games": _make_games(),
            "fct_ratings_adjusted": _make_ratings_adjusted(),
            "fct_ratings_srs": pa.table({}),
            "fct_pbp_team_daily_rollup": pa.table({}),
            "fct_recruiting_players": pa.table({}),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            tids = result.column("teamId").to_pylist()
            cw = result.column("conf_wins").to_pylist()
            cl = result.column("conf_losses").to_pylist()

            # Game 100: Duke(ACC) vs UNC(ACC) -> conference game, Duke wins
            duke_idx = tids.index(1)
            unc_idx = tids.index(2)
            ky_idx = tids.index(3)

            assert cw[duke_idx] == 1  # beat UNC
            assert cl[duke_idx] == 0
            assert cw[unc_idx] == 0
            assert cl[unc_idx] == 1  # lost to Duke
            # Kentucky is SEC, no conference games
            assert cw[ky_idx] == 0
            assert cl[ky_idx] == 0
        finally:
            for p in patches:
                p.stop()

    def test_win_pct_zero_games(self):
        """Win pct is None when a team has 0 games."""
        from cbbd_etl.gold.team_season_summary import build

        # Use ratings as spine with no games
        table_data = {
            "dim_teams": _make_dim_teams(),
            "fct_games": pa.table({}),
            "fct_ratings_adjusted": _make_ratings_adjusted(),
            "fct_ratings_srs": pa.table({}),
            "fct_pbp_team_daily_rollup": pa.table({}),
            "fct_recruiting_players": pa.table({}),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            win_pcts = result.column("win_pct").to_pylist()
            # All should be None since no games
            assert all(w is None for w in win_pcts)
        finally:
            for p in patches:
                p.stop()


# ---------------------------------------------------------------------------
# Tests: TABLE_SPECS
# ---------------------------------------------------------------------------

class TestGoldTableSpecs:
    """Verify gold TABLE_SPECS are defined and consistent."""

    def test_all_gold_tables_have_specs(self):
        """Every gold table in GOLD_TRANSFORMS has a corresponding TABLE_SPEC."""
        from cbbd_etl.gold import GOLD_TRANSFORMS

        for name in GOLD_TRANSFORMS:
            assert name in TABLE_SPECS, f"Missing TABLE_SPEC for gold table: {name}"

    def test_primary_keys_defined(self):
        """Each gold TABLE_SPEC has non-empty primary keys."""
        gold_tables = [
            "team_power_rankings",
            "game_predictions_features",
            "player_season_impact",
            "market_lines_analysis",
            "team_season_summary",
        ]
        for name in gold_tables:
            spec = TABLE_SPECS[name]
            assert len(spec.primary_keys) > 0, f"No primary keys for {name}"

    def test_normalize_records_works(self):
        """normalize_records works with gold table specs (round-trip test)."""
        records = [
            {"teamId": 1, "season": 2024, "team": "Duke", "wins": 30, "losses": 5},
        ]
        result = normalize_records("team_season_summary", records)
        assert result.num_rows == 1
        assert result.column("teamId").to_pylist()[0] == 1


# ---------------------------------------------------------------------------
# Tests: _io_helpers
# ---------------------------------------------------------------------------

class TestIOHelpers:
    """Tests for gold layer I/O helper functions."""

    def test_safe_divide(self):
        """safe_divide handles normal, zero, and None cases."""
        from cbbd_etl.gold._io_helpers import safe_divide

        result = safe_divide([10.0, 0.0, None, 5.0], [2.0, 0.0, 3.0, None])
        assert result[0] == 5.0
        assert result[1] is None  # 0/0
        assert result[2] is None  # None numerator
        assert result[3] is None  # None denominator

    def test_safe_divide_with_scale(self):
        """safe_divide applies scale factor."""
        from cbbd_etl.gold._io_helpers import safe_divide

        result = safe_divide([10.0], [50.0], scale=100.0)
        assert abs(result[0] - 20.0) < 0.01

    def test_pydict_get_missing_column(self):
        """pydict_get returns Nones for missing columns."""
        from cbbd_etl.gold._io_helpers import pydict_get

        table = pa.table({"a": [1, 2, 3]})
        result = pydict_get(table, "b")
        assert result == [None, None, None]

    def test_pydict_get_existing_column(self):
        """pydict_get returns column values for existing columns."""
        from cbbd_etl.gold._io_helpers import pydict_get

        table = pa.table({"a": [1, 2, 3]})
        result = pydict_get(table, "a")
        assert result == [1, 2, 3]

    def test_pydict_get_first_finds_second_candidate(self):
        """pydict_get_first returns data from the first matching column."""
        from cbbd_etl.gold._io_helpers import pydict_get_first

        table = pa.table({"offensiveRating": [115.0, 110.0]})
        result = pydict_get_first(table, ["offenserating", "offensiveRating"])
        assert result == [115.0, 110.0]

    def test_pydict_get_first_all_missing(self):
        """pydict_get_first returns Nones when no candidates match."""
        from cbbd_etl.gold._io_helpers import pydict_get_first

        table = pa.table({"x": [1, 2]})
        result = pydict_get_first(table, ["a", "b", "c"])
        assert result == [None, None]

    def test_filter_by_season(self):
        """filter_by_season filters rows by season column value."""
        from cbbd_etl.gold._io_helpers import filter_by_season

        table = pa.table({
            "teamId": pa.array([1, 2, 3, 4], type=pa.int64()),
            "season": pa.array([2024, 2025, 2024, 2025], type=pa.int32()),
            "rating": pa.array([10.0, 20.0, 30.0, 40.0], type=pa.float64()),
        })
        result = filter_by_season(table, 2024)
        assert result.num_rows == 2
        assert result.column("teamId").to_pylist() == [1, 3]

    def test_dedup_by(self):
        """dedup_by removes duplicate rows by key columns."""
        from cbbd_etl.gold._io_helpers import dedup_by

        table = pa.table({
            "gameId": pa.array([100, 100, 101], type=pa.int64()),
            "homeTeamId": pa.array([1, 1, 2], type=pa.int64()),
            "homeScore": pa.array([85, 85, 70], type=pa.int64()),
        })
        result = dedup_by(table, ["gameId"])
        assert result.num_rows == 2
        assert result.column("gameId").to_pylist() == [100, 101]


# ---------------------------------------------------------------------------
# Tests: real-data column name patterns
# ---------------------------------------------------------------------------

class TestRealDataColumnNames:
    """Tests verifying gold transforms work with real S3 column names."""

    def test_ratings_adjusted_real_columns(self):
        """team_power_rankings works with offensiveRating/defensiveRating."""
        from cbbd_etl.gold.team_power_rankings import build

        # Use real-world column names: offensiveRating, defensiveRating
        real_adj = pa.table({
            "teamid": pa.array([1, 2], type=pa.int64()),
            "season": pa.array([2024, 2024], type=pa.int32()),
            "team": pa.array(["Duke", "UNC"], type=pa.string()),
            "conference": pa.array(["ACC", "ACC"], type=pa.string()),
            "offensiveRating": pa.array([115.0, 110.0], type=pa.float64()),
            "defensiveRating": pa.array([95.0, 98.0], type=pa.float64()),
            "netrating": pa.array([20.0, 12.0], type=pa.float64()),
            "ranking_offense": pa.array([5, 20], type=pa.int64()),
            "ranking_defense": pa.array([3, 15], type=pa.int64()),
            "ranking_net": pa.array([1, 10], type=pa.int64()),
        })
        table_data = {
            "fct_ratings_adjusted": real_adj,
            "fct_ratings_srs": pa.table({}),
            "fct_rankings": pa.table({}),
            "fct_pbp_team_daily_rollup": pa.table({}),
            "fct_pbp_team_daily_rollup_adj": pa.table({}),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            assert result.num_rows == 2
            offs = result.column("adj_off_rating").to_pylist()
            defs = result.column("adj_def_rating").to_pylist()
            assert 115.0 in offs
            assert 95.0 in defs
        finally:
            for p in patches:
                p.stop()

    def test_games_homepoints_columns(self):
        """game_predictions_features works with homePoints/awayPoints."""
        from cbbd_etl.gold.game_predictions_features import build

        real_games = pa.table({
            "gameId": pa.array([100], type=pa.int64()),
            "season": pa.array([2024], type=pa.int32()),
            "startDate": pa.array(["2024-01-10"], type=pa.string()),
            "homeTeamId": pa.array([1], type=pa.int64()),
            "awayTeamId": pa.array([2], type=pa.int64()),
            "homePoints": pa.array([85], type=pa.int64()),
            "awayPoints": pa.array([78], type=pa.int64()),
        })
        table_data = {
            "fct_games": real_games,
            "fct_ratings_adjusted": pa.table({}),
            "fct_ratings_srs": pa.table({}),
            "fct_lines": pa.table({}),
            "fct_pbp_team_daily_rollup": pa.table({}),
            "dim_teams": _make_dim_teams(),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            assert result.num_rows == 2
            scores = result.column("team_score").to_pylist()
            assert 85 in scores
            assert 78 in scores
        finally:
            for p in patches:
                p.stop()

    def test_player_stats_string_dict_fields(self):
        """player_season_impact parses string dict fields from real data."""
        from cbbd_etl.gold.player_season_impact import build

        real_stats = pa.table({
            "athleteId": pa.array([747], type=pa.int64()),
            "season": pa.array([2024], type=pa.int32()),
            "team": pa.array(["Abilene Christian"], type=pa.string()),
            "conference": pa.array(["WAC"], type=pa.string()),
            "games": pa.array([31], type=pa.int64()),
            "minutes": pa.array([985], type=pa.int64()),
            "points": pa.array([482], type=pa.int64()),
            "rebounds": pa.array(["{'offensive': 31, 'defensive': 110, 'total': 141}"], type=pa.string()),
            "assists": pa.array([36], type=pa.int64()),
            "steals": pa.array([43], type=pa.int64()),
            "blocks": pa.array([7], type=pa.int64()),
            "turnovers": pa.array([72], type=pa.int64()),
            "fieldGoals": pa.array(["{'made': 175, 'attempted': 367, 'pct': 47.7}"], type=pa.string()),
            "freeThrows": pa.array(["{'made': 115, 'attempted': 158, 'pct': 72.8}"], type=pa.string()),
            "threePointFieldGoals": pa.array(["{'made': 17, 'attempted': 49, 'pct': 34.7}"], type=pa.string()),
        })
        table_data = {
            "fct_player_season_stats": real_stats,
            "fct_recruiting_players": pa.table({}),
            "dim_teams": pa.table({}),
        }
        patches = _patch_s3io(table_data)
        for p in patches:
            p.start()
        try:
            result = build(_make_config(), 2024)
            assert result.num_rows == 1
            # Check parsed shooting stats
            fgm = result.column("fgm").to_pylist()[0]
            fga = result.column("fga").to_pylist()[0]
            assert fgm == 175.0
            assert fga == 367.0
            # Check rebounds parsed from string dict
            reb = result.column("rebounds").to_pylist()[0]
            assert reb == 141.0
            # Check ppg
            ppg = result.column("ppg").to_pylist()[0]
            assert abs(ppg - 482 / 31) < 0.1
        finally:
            for p in patches:
                p.stop()
