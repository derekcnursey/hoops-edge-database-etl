-- ============================================================
-- Gold Layer Validation Queries — Run in Athena (cbbd_gold db)
-- ============================================================

-- FIRST: Make sure Athena sees all partitions
MSCK REPAIR TABLE team_power_rankings;
MSCK REPAIR TABLE team_season_summary;
MSCK REPAIR TABLE game_predictions_features;
MSCK REPAIR TABLE market_lines_analysis;
MSCK REPAIR TABLE player_season_impact;


-- ============================================================
-- 1. TEAM POWER RANKINGS — Top 25 for current season
-- ============================================================

-- Top 25 teams this season — does this look right?
SELECT team, conference, blended_score, adj_efficiency, srs, poll_rank
FROM team_power_rankings
WHERE season = 2026
ORDER BY blended_score DESC
LIMIT 25;

-- How many teams per season?
SELECT season, COUNT(*) as teams
FROM team_power_rankings
GROUP BY season
ORDER BY season;

-- Any duplicates?
SELECT season, team, COUNT(*) as dupes
FROM team_power_rankings
GROUP BY season, team
HAVING COUNT(*) > 1;


-- ============================================================
-- 2. TEAM SEASON SUMMARY — Sanity check W-L records
-- ============================================================

-- Blue bloods this season
SELECT team, conference, wins, losses, points_per_game, margin
FROM team_season_summary
WHERE season = 2026
  AND team IN ('Duke', 'Kansas', 'Kentucky', 'North Carolina', 'Houston', 'Auburn', 'Florida', 'Tennessee')
ORDER BY wins DESC;

-- Teams with suspicious records
SELECT team, wins, losses, points_per_game
FROM team_season_summary
WHERE season = 2026
  AND (wins < 0 OR losses < 0 OR wins + losses > 40 OR points_per_game > 120 OR points_per_game < 40)
ORDER BY team;

-- Row counts per season
SELECT season, COUNT(*) as teams
FROM team_season_summary
GROUP BY season
ORDER BY season;


-- ============================================================
-- 3. GAME PREDICTIONS FEATURES — What feeds your model
-- ============================================================

-- Total games this season (should be 2 rows per game)
SELECT season, COUNT(*) as rows, COUNT(DISTINCT "gameId") as games
FROM game_predictions_features
WHERE season = 2026
GROUP BY season;

-- Today's games — compare against hoops-edge.vercel.app
SELECT "gameId", home_team, away_team, home_adj_eff, away_adj_eff, spread
FROM game_predictions_features
WHERE season = 2026
  AND CAST(game_date AS DATE) = CURRENT_DATE
ORDER BY "gameId";

-- How many games have null features? (should be low)
SELECT 
  COUNT(*) as total,
  COUNT(home_adj_eff) as has_home_eff,
  COUNT(away_adj_eff) as has_away_eff,
  COUNT(spread) as has_spread,
  COUNT(home_srs) as has_srs
FROM game_predictions_features
WHERE season = 2026;


-- ============================================================
-- 4. MARKET LINES ANALYSIS — Spread accuracy
-- ============================================================

-- Spread error distribution (mean should be near 0)
SELECT 
  COUNT(*) as games,
  ROUND(AVG(spread_error), 2) as avg_spread_error,
  ROUND(STDDEV(spread_error), 2) as stddev_spread_error,
  ROUND(MIN(spread_error), 2) as min_error,
  ROUND(MAX(spread_error), 2) as max_error
FROM market_lines_analysis
WHERE season = 2025;

-- Home cover rate (should be ~50%)
SELECT 
  COUNT(*) as total,
  SUM(CASE WHEN home_covered = true THEN 1 ELSE 0 END) as home_covers,
  ROUND(AVG(CASE WHEN home_covered = true THEN 1.0 ELSE 0.0 END), 3) as cover_rate
FROM market_lines_analysis
WHERE season = 2025;

-- Biggest upsets (largest spread errors)
SELECT home_team, away_team, spread, actual_margin, spread_error
FROM market_lines_analysis
WHERE season = 2025
ORDER BY ABS(spread_error) DESC
LIMIT 20;


-- ============================================================
-- 5. PLAYER SEASON IMPACT — Star player check
-- ============================================================

-- Top scorers this season
SELECT player_name, team, games, ppg, rpg, apg, true_shooting, usage_rate
FROM player_season_impact
WHERE season = 2026
ORDER BY ppg DESC
LIMIT 25;

-- Suspicious values
SELECT player_name, team, ppg, minutes_per_game, true_shooting
FROM player_season_impact
WHERE season = 2026
  AND (ppg > 40 OR ppg < 0 OR minutes_per_game > 42 OR true_shooting > 1.0 OR true_shooting < 0)
ORDER BY ppg DESC;

-- Players by team (spot check your favorite team)
SELECT player_name, games, ppg, rpg, apg, efg_pct, true_shooting, usage_rate, recruiting_rank, recruiting_stars
FROM player_season_impact
WHERE season = 2026
  AND team = 'Duke'
ORDER BY ppg DESC;

-- Row counts per season
SELECT season, COUNT(*) as players, COUNT(DISTINCT team) as teams
FROM player_season_impact
GROUP BY season
ORDER BY season;


-- ============================================================
-- 6. CROSS-TABLE CONSISTENCY
-- ============================================================

-- Teams in power rankings but missing from season summary
SELECT pr.team, pr.season
FROM team_power_rankings pr
LEFT JOIN team_season_summary ts ON pr.team = ts.team AND pr.season = ts.season
WHERE ts.team IS NULL AND pr.season = 2026;

-- Games in predictions but missing lines
SELECT gp.season, COUNT(DISTINCT gp."gameId") as pred_games, 
       COUNT(DISTINCT ml."gameId") as lines_games
FROM game_predictions_features gp
LEFT JOIN market_lines_analysis ml ON gp."gameId" = ml."gameId"
WHERE gp.season = 2025
GROUP BY gp.season;
