"""Tests for the adjusted efficiencies orchestration module."""

import pytest

from cbbd_etl.gold.adjusted_efficiencies import _parse_team_stats


# ---------------------------------------------------------------------------
# _parse_team_stats
# ---------------------------------------------------------------------------


def test_parse_team_stats_normal():
    """Verify dict extraction from a real teamStats string."""
    stats = '{"possessions": 68, "points": {"total": 75}, "fieldGoals": {"made": 28}}'
    poss, pts = _parse_team_stats(stats)
    assert poss == 68.0
    assert pts == 75.0


def test_parse_team_stats_flat_points():
    """Handle case where points is a flat number."""
    stats = '{"possessions": 70, "points": 80}'
    poss, pts = _parse_team_stats(stats)
    assert poss == 70.0
    assert pts == 80.0


def test_parse_team_stats_none():
    """None input returns (None, None)."""
    assert _parse_team_stats(None) == (None, None)


def test_parse_team_stats_empty_string():
    """Empty string returns (None, None)."""
    assert _parse_team_stats("") == (None, None)


def test_parse_team_stats_missing_keys():
    """Dict without possessions or points returns None values."""
    stats = '{"fieldGoals": {"made": 28}}'
    poss, pts = _parse_team_stats(stats)
    assert poss is None
    assert pts is None


def test_parse_team_stats_dict_input():
    """Direct dict input (not string) should work."""
    stats = {"possessions": 65, "points": {"total": 70}}
    poss, pts = _parse_team_stats(stats)
    assert poss == 65.0
    assert pts == 70.0


def test_parse_team_stats_malformed():
    """Malformed string returns (None, None)."""
    assert _parse_team_stats("not a dict at all") == (None, None)
