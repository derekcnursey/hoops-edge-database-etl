-- ============================================================
-- Bronze & Silver Layer Validation Queries
-- ============================================================

-- FIRST: Repair all tables so Athena sees partitions
-- Run these in cbbd_bronze database
-- MSCK REPAIR TABLE games;
-- MSCK REPAIR TABLE games_teams;
-- MSCK REPAIR TABLE games_players;
-- MSCK REPAIR TABLE plays_game;
-- MSCK REPAIR TABLE lines;
-- MSCK REPAIR TABLE ratings_adjusted;
-- MSCK REPAIR TABLE ratings_srs;
-- MSCK REPAIR TABLE stats_player_season;
-- MSCK REPAIR TABLE stats_team_season;
-- MSCK REPAIR TABLE rankings;
-- MSCK REPAIR TABLE recruiting_players;
-- MSCK REPAIR TABLE teams;

-- Run these in cbbd_silver database
-- MSCK REPAIR TABLE fct_games;
-- MSCK REPAIR TABLE fct_game_teams;
-- MSCK REPAIR TABLE fct_game_players;
-- MSCK REPAIR TABLE fct_plays;
-- MSCK REPAIR TABLE fct_lines;
-- MSCK REPAIR TABLE fct_ratings_adjusted;
-- MSCK REPAIR TABLE fct_ratings_srs;
-- MSCK REPAIR TABLE fct_player_season_stats;
-- MSCK REPAIR TABLE fct_stats_team_season;
-- MSCK REPAIR TABLE fct_rankings;
-- MSCK REPAIR TABLE fct_recruiting_players;
-- MSCK REPAIR TABLE fct_substitutions;
-- MSCK REPAIR TABLE fct_lineups;
-- MSCK REPAIR TABLE dim_teams;
-- MSCK REPAIR TABLE dim_conferences;
-- MSCK REPAIR TABLE dim_venues;


-- ============================================================
-- BRONZE LAYER (run against cbbd_bronze)
-- ============================================================

-- 1. Row counts per endpoint per season — are we missing seasons?
SELECT 'games' as tbl, season, COUNT(*) as rows FROM games GROUP BY season
UNION ALL
SELECT 'games_teams', season, COUNT(*) FROM games_teams GROUP BY season
UNION ALL
SELECT 'games_players', season, COUNT(*) FROM games_players GROUP BY season
UNION ALL
SELECT 'lines', season, COUNT(*) FROM lines GROUP BY season
UNION ALL
SELECT 'ratings_adjusted', season, COUNT(*) FROM ratings_adjusted GROUP BY season
UNION ALL
SELECT 'rankings', season, COUNT(*) FROM rankings GROUP BY season
UNION ALL
SELECT 'stats_player_season', season, COUNT(*) FROM stats_player_season GROUP BY season
UNION ALL
SELECT 'stats_team_season', season, COUNT(*) FROM stats_team_season GROUP BY season
UNION ALL
SELECT 'recruiting_players', season, COUNT(*) FROM recruiting_players GROUP BY season
ORDER BY tbl, season;

-- 2. Check for empty seasons (ingestion gaps)
SELECT 'games' as tbl, season FROM games GROUP BY season HAVING COUNT(*) < 100
UNION ALL
SELECT 'games_teams', season FROM games_teams GROUP BY season HAVING COUNT(*) < 200
UNION ALL
SELECT 'games_players', season FROM games_players GROUP BY season HAVING COUNT(*) < 1000
ORDER BY tbl, season;

-- 3. Games per season — D1 has ~5,500-6,000 games/season
SELECT season, COUNT(*) as games
FROM games
GROUP BY season
ORDER BY season;

-- 4. Check for duplicate gameIds in games table
SELECT season, "gameId", COUNT(*) as dupes
FROM games
GROUP BY season, "gameId"
HAVING COUNT(*) > 1
ORDER BY dupes DESC
LIMIT 20;


-- ============================================================
-- SILVER LAYER — FACT TABLES (run against cbbd_silver)
-- ============================================================

-- 5. Row counts per silver table per season
SELECT 'fct_games' as tbl, season, COUNT(*) as rows FROM fct_games GROUP BY season
UNION ALL
SELECT 'fct_game_teams', season, COUNT(*) FROM fct_game_teams GROUP BY season
UNION ALL
SELECT 'fct_game_players', season, COUNT(*) FROM fct_game_players GROUP BY season
UNION ALL
SELECT 'fct_plays', season, COUNT(*) FROM fct_plays GROUP BY season
UNION ALL
SELECT 'fct_lines', season, COUNT(*) FROM fct_lines GROUP BY season
UNION ALL
SELECT 'fct_ratings_adjusted', season, COUNT(*) FROM fct_ratings_adjusted GROUP BY season
UNION ALL
SELECT 'fct_ratings_srs', season, COUNT(*) FROM fct_ratings_srs GROUP BY season
UNION ALL
SELECT 'fct_player_season_stats', season, COUNT(*) FROM fct_player_season_stats GROUP BY season
UNION ALL
SELECT 'fct_stats_team_season', season, COUNT(*) FROM fct_stats_team_season GROUP BY season
UNION ALL
SELECT 'fct_rankings', season, COUNT(*) FROM fct_rankings GROUP BY season
UNION ALL
SELECT 'fct_recruiting_players', season, COUNT(*) FROM fct_recruiting_players GROUP BY season
ORDER BY tbl, season;

-- 6. Duplicate check on fct_games
SELECT season, "gameId", COUNT(*) as dupes
FROM fct_games
GROUP BY season, "gameId"
HAVING COUNT(*) > 1
ORDER BY dupes DESC
LIMIT 20;

-- 7. Duplicate check on fct_game_players
SELECT season, "gameId", "playerId", COUNT(*) as dupes
FROM fct_game_players
GROUP BY season, "gameId", "playerId"
HAVING COUNT(*) > 1
ORDER BY dupes DESC
LIMIT 20;


-- ============================================================
-- SILVER LAYER — DATA QUALITY
-- ============================================================

-- 8. Games with missing scores (should be 0 for completed games)
SELECT season, COUNT(*) as missing_scores
FROM fct_games
WHERE ("homeScore" IS NULL OR "awayScore" IS NULL)
  AND season < 2026
GROUP BY season
ORDER BY season;

-- 9. Games missing team box scores
SELECT g.season, COUNT(DISTINCT g."gameId") as total_games,
       COUNT(DISTINCT gt."gameId") as games_with_teams,
       COUNT(DISTINCT g."gameId") - COUNT(DISTINCT gt."gameId") as missing_team_stats
FROM fct_games g
LEFT JOIN fct_game_teams gt ON g."gameId" = gt."gameId"
WHERE g.season = 2025
GROUP BY g.season;

-- 10. Games missing player stats
SELECT g.season, COUNT(DISTINCT g."gameId") as total_games,
       COUNT(DISTINCT gp."gameId") as games_with_players,
       COUNT(DISTINCT g."gameId") - COUNT(DISTINCT gp."gameId") as missing_player_stats
FROM fct_games g
LEFT JOIN fct_game_players gp ON g."gameId" = gp."gameId"
WHERE g.season = 2025
GROUP BY g.season;

-- 11. Play-by-play coverage (the known gap)
SELECT g.season, COUNT(DISTINCT g."gameId") as total_games,
       COUNT(DISTINCT p."gameId") as games_with_pbp,
       COUNT(DISTINCT g."gameId") - COUNT(DISTINCT p."gameId") as missing_pbp,
       ROUND(100.0 * COUNT(DISTINCT p."gameId") / COUNT(DISTINCT g."gameId"), 1) as pbp_pct
FROM fct_games g
LEFT JOIN fct_plays p ON g."gameId" = p."gameId"
GROUP BY g.season
ORDER BY g.season;

-- 12. Lines coverage
SELECT g.season, COUNT(DISTINCT g."gameId") as total_games,
       COUNT(DISTINCT l."gameId") as games_with_lines,
       ROUND(100.0 * COUNT(DISTINCT l."gameId") / COUNT(DISTINCT g."gameId"), 1) as lines_pct
FROM fct_games g
LEFT JOIN fct_lines l ON g."gameId" = l."gameId"
WHERE g.season >= 2015
GROUP BY g.season
ORDER BY g.season;


-- ============================================================
-- SILVER LAYER — DIMENSION TABLES
-- ============================================================

-- 13. Teams dimension
SELECT COUNT(*) as total_teams,
       COUNT(DISTINCT "teamId") as unique_teams,
       COUNT(DISTINCT conference) as conferences
FROM dim_teams;

-- 14. Conferences dimension
SELECT COUNT(*) as total, COUNT(DISTINCT "conferenceId") as unique
FROM dim_conferences;

-- 15. Venues dimension
SELECT COUNT(*) as total, COUNT(DISTINCT "venueId") as unique
FROM dim_venues;


-- ============================================================
-- SILVER LAYER — REFERENTIAL INTEGRITY
-- ============================================================

-- 16. Games referencing teams not in dim_teams
SELECT DISTINCT g."homeTeamId", g."homeTeam"
FROM fct_games g
LEFT JOIN dim_teams t ON g."homeTeamId" = t."teamId"
WHERE t."teamId" IS NULL AND g.season = 2025
LIMIT 20;

-- 17. Players referencing teams not in dim_teams
SELECT DISTINCT gp."teamId", gp.team
FROM fct_game_players gp
LEFT JOIN dim_teams t ON gp."teamId" = t."teamId"
WHERE t."teamId" IS NULL AND gp.season = 2025
LIMIT 20;

-- 18. Ratings referencing teams not in dim_teams
SELECT DISTINCT r."teamId", r.team
FROM fct_ratings_adjusted r
LEFT JOIN dim_teams t ON r."teamId" = t."teamId"
WHERE t."teamId" IS NULL
LIMIT 20;


-- ============================================================
-- CROSS-LAYER CONSISTENCY (bronze vs silver)
-- ============================================================

-- 19. Bronze vs silver game counts (should match)
-- Run bronze count first, then silver, compare manually
-- Bronze (run in cbbd_bronze):
SELECT season, COUNT(*) as bronze_games FROM games GROUP BY season ORDER BY season;
-- Silver (run in cbbd_silver):
SELECT season, COUNT(*) as silver_games FROM fct_games GROUP BY season ORDER BY season;

-- 20. Check for silver tables with stale partitions
-- (partition asof dates much older than latest ingestion)
SELECT 'fct_games' as tbl, MAX(asof) as latest_asof FROM fct_games
UNION ALL
SELECT 'fct_game_teams', MAX(asof) FROM fct_game_teams
UNION ALL
SELECT 'fct_game_players', MAX(asof) FROM fct_game_players
UNION ALL
SELECT 'fct_lines', MAX(asof) FROM fct_lines
UNION ALL
SELECT 'fct_ratings_adjusted', MAX(asof) FROM fct_ratings_adjusted
ORDER BY tbl;
