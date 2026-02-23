"""Tests for the iterative ratings math engine."""

import pytest

from cbbd_etl.gold.iterative_ratings import (
    GameObs,
    compute_barthag,
    exponential_decay_weight,
    solve_ratings,
)


# ---------------------------------------------------------------------------
# exponential_decay_weight
# ---------------------------------------------------------------------------


def test_exponential_decay_weight_zero():
    assert exponential_decay_weight(0) == 1.0


def test_exponential_decay_weight_one_half_life():
    assert abs(exponential_decay_weight(30) - 0.5) < 1e-9


def test_exponential_decay_weight_two_half_lives():
    assert abs(exponential_decay_weight(60) - 0.25) < 1e-9


def test_exponential_decay_weight_negative():
    assert exponential_decay_weight(-5) == 1.0


def test_exponential_decay_weight_custom_half_life():
    assert abs(exponential_decay_weight(10, half_life=10.0) - 0.5) < 1e-9


# ---------------------------------------------------------------------------
# compute_barthag
# ---------------------------------------------------------------------------


def test_barthag_equal():
    """Equal OE and DE should produce 0.5."""
    assert abs(compute_barthag(100.0, 100.0) - 0.5) < 1e-9


def test_barthag_dominant():
    """High OE, low DE should produce >0.95."""
    result = compute_barthag(120.0, 90.0)
    assert result > 0.95


def test_barthag_weak():
    """Low OE, high DE should produce <0.05."""
    result = compute_barthag(85.0, 115.0)
    assert result < 0.05


def test_barthag_zero():
    """Zero inputs should return 0.5."""
    assert compute_barthag(0.0, 0.0) == 0.5


# ---------------------------------------------------------------------------
# solve_ratings — two teams
# ---------------------------------------------------------------------------


def _make_game(
    gid, team, opp, t_pts, t_poss, o_pts, o_poss, home=True, neutral=False
):
    return GameObs(
        game_id=gid,
        team_id=team,
        opp_id=opp,
        team_pts=t_pts,
        team_poss=t_poss,
        opp_pts=o_pts,
        opp_poss=o_poss,
        is_home=home,
        is_neutral=neutral,
        game_date="2025-01-01",
        weight=1.0,
    )


def test_solve_two_teams():
    """A beats B at home — A should get higher adj_oe than B."""
    games = [
        _make_game(1, 1, 2, 80, 70, 60, 70, home=True, neutral=False),
        _make_game(1, 2, 1, 60, 70, 80, 70, home=False, neutral=False),
    ]
    result = solve_ratings(games)
    assert 1 in result
    assert 2 in result
    assert result[1]["adj_oe"] > result[2]["adj_oe"]
    assert result[1]["adj_de"] < result[2]["adj_de"]


def test_solve_convergence_four_teams():
    """4-team round robin should converge in <100 iterations."""
    games = []
    gid = 0
    # Team 1 beats everyone, team 4 loses to everyone
    matchups = [
        (1, 2, 80, 65), (1, 3, 85, 70), (1, 4, 90, 55),
        (2, 3, 75, 70), (2, 4, 80, 60),
        (3, 4, 75, 65),
    ]
    for t1, t2, s1, s2 in matchups:
        gid += 1
        games.append(_make_game(gid, t1, t2, s1, 70, s2, 70, home=True, neutral=True))
        games.append(_make_game(gid, t2, t1, s2, 70, s1, 70, home=False, neutral=True))

    result = solve_ratings(games)
    assert len(result) == 4
    # Team 1 should be best
    assert result[1]["adj_oe"] > result[4]["adj_oe"]
    assert result[1]["adj_de"] < result[4]["adj_de"]
    # All teams should have games_played
    for tid in [1, 2, 3, 4]:
        assert result[tid]["games_played"] > 0


def test_home_court_neutral():
    """Neutral site games should have no HCA applied — symmetric results."""
    games = [
        _make_game(1, 1, 2, 75, 70, 75, 70, home=True, neutral=True),
        _make_game(1, 2, 1, 75, 70, 75, 70, home=False, neutral=True),
    ]
    result = solve_ratings(games)
    # Identical scores on neutral court — ratings should be equal
    assert abs(result[1]["adj_oe"] - result[2]["adj_oe"]) < 0.01
    assert abs(result[1]["adj_de"] - result[2]["adj_de"]) < 0.01


def test_solve_empty():
    """Empty games list should return empty dict."""
    assert solve_ratings([]) == {}


def test_solve_with_prior():
    """Warm-start with prior should still converge."""
    games = [
        _make_game(1, 1, 2, 80, 70, 60, 70, home=True, neutral=True),
        _make_game(1, 2, 1, 60, 70, 80, 70, home=False, neutral=True),
    ]
    prior = {1: (110.0, 90.0), 2: (90.0, 110.0)}
    result = solve_ratings(games, prior=prior)
    assert result[1]["adj_oe"] > result[2]["adj_oe"]


def test_natural_average_emerges():
    """League average adj_oe/adj_de should match the data-driven league_avg, not 100.0."""
    games = []
    gid = 0
    # These scores on 70 possessions give a league_avg of ~103.57 pts/100 poss
    matchups = [
        (1, 2, 80, 65), (1, 3, 85, 70), (1, 4, 90, 55),
        (2, 3, 75, 70), (2, 4, 80, 60),
        (3, 4, 75, 65),
    ]
    for t1, t2, s1, s2 in matchups:
        gid += 1
        games.append(_make_game(gid, t1, t2, s1, 70, s2, 70, home=True, neutral=True))
        games.append(_make_game(gid, t2, t1, s2, 70, s1, 70, home=False, neutral=True))

    result = solve_ratings(games)
    total_gp = sum(v["games_played"] for v in result.values())
    avg_oe = sum(v["adj_oe"] * v["games_played"] for v in result.values()) / total_gp
    avg_de = sum(v["adj_de"] * v["games_played"] for v in result.values()) / total_gp

    # Compute expected league_avg from the test data
    total_pts = sum(s1 + s2 for _, _, s1, s2 in matchups)
    total_poss = len(matchups) * 2 * 70  # each matchup has 2 teams, 70 poss each
    expected_league_avg = total_pts / total_poss * 100.0

    # Average should be near the data-driven value, NOT forced to 100.0
    assert abs(avg_oe - expected_league_avg) < 1.0, f"avg_oe={avg_oe}, expected ~{expected_league_avg}"
    assert abs(avg_de - expected_league_avg) < 1.0, f"avg_de={avg_de}, expected ~{expected_league_avg}"
    assert avg_oe > 100.0, f"avg_oe={avg_oe} should be > 100 for this data"


def test_relative_ordering_preserved():
    """Team 1 > 2 > 3 > 4 by margin (adj_oe - adj_de) in the round robin."""
    games = []
    gid = 0
    matchups = [
        (1, 2, 80, 65), (1, 3, 85, 70), (1, 4, 90, 55),
        (2, 3, 75, 70), (2, 4, 80, 60),
        (3, 4, 75, 65),
    ]
    for t1, t2, s1, s2 in matchups:
        gid += 1
        games.append(_make_game(gid, t1, t2, s1, 70, s2, 70, home=True, neutral=True))
        games.append(_make_game(gid, t2, t1, s2, 70, s1, 70, home=False, neutral=True))

    result = solve_ratings(games)
    margins = {tid: r["adj_oe"] - r["adj_de"] for tid, r in result.items()}
    assert margins[1] > margins[2] > margins[3] > margins[4], f"margins={margins}"


def test_per_game_adjustment_produces_spread():
    """Per-game SOS adjustment should produce meaningful margin separation.

    Team 1 beats everyone convincingly. The solver should give team 1 the
    best margin (adj_oe - adj_de) and the spread should be substantial.
    """
    games = []
    gid = 0
    # Team 1 (elite) plays both strong and weak teams
    games.append(_make_game(gid := gid + 1, 1, 2, 78, 70, 72, 70, neutral=True))
    games.append(_make_game(gid, 2, 1, 72, 70, 78, 70, neutral=True))
    games.append(_make_game(gid := gid + 1, 1, 3, 80, 70, 65, 70, neutral=True))
    games.append(_make_game(gid, 3, 1, 65, 70, 80, 70, neutral=True))
    games.append(_make_game(gid := gid + 1, 1, 4, 95, 70, 55, 70, neutral=True))
    games.append(_make_game(gid, 4, 1, 55, 70, 95, 70, neutral=True))
    games.append(_make_game(gid := gid + 1, 1, 5, 90, 70, 60, 70, neutral=True))
    games.append(_make_game(gid, 5, 1, 60, 70, 90, 70, neutral=True))
    # Strong teams beat each other closely
    games.append(_make_game(gid := gid + 1, 2, 3, 75, 70, 70, 70, neutral=True))
    games.append(_make_game(gid, 3, 2, 70, 70, 75, 70, neutral=True))
    # Weak teams lose to each other closely
    games.append(_make_game(gid := gid + 1, 4, 5, 65, 70, 60, 70, neutral=True))
    games.append(_make_game(gid, 5, 4, 60, 70, 65, 70, neutral=True))

    result = solve_ratings(games)
    margins = {tid: r["adj_oe"] - r["adj_de"] for tid, r in result.items()}
    # Team 1 should be the best
    assert margins[1] == max(margins.values()), f"Team 1 should have best margin: {margins}"
    # Team 1's margin should be substantial (> 20 pts/100 poss)
    assert margins[1] > 20, f"Team 1 margin={margins[1]:.2f} should be > 20"
    # There should be meaningful spread between best and worst
    spread = max(margins.values()) - min(margins.values())
    assert spread > 40, f"Total spread={spread:.2f} should be > 40"
