-- =============================================================================
-- Data Completeness Audit Queries for cbbd_silver
-- Database: cbbd_silver | Workgroup: cbbd
-- Run via: aws athena start-query-execution --work-group cbbd
-- =============================================================================

-- Q1: Record counts per season for all key fact tables
SELECT 'fct_games' AS table_name, season, COUNT(*) AS record_count FROM fct_games GROUP BY season
UNION ALL
SELECT 'fct_plays', season, COUNT(*) FROM fct_plays GROUP BY season
UNION ALL
SELECT 'fct_substitutions', season, COUNT(*) FROM fct_substitutions GROUP BY season
UNION ALL
SELECT 'fct_game_teams', season, COUNT(*) FROM fct_game_teams GROUP BY season
UNION ALL
SELECT 'fct_game_players', season, COUNT(*) FROM fct_game_players GROUP BY season
UNION ALL
SELECT 'fct_lines', season, COUNT(*) FROM fct_lines GROUP BY season
UNION ALL
SELECT 'fct_rankings', season, COUNT(*) FROM fct_rankings GROUP BY season
UNION ALL
SELECT 'fct_ratings_adjusted', season, COUNT(*) FROM fct_ratings_adjusted GROUP BY season
ORDER BY table_name, season;


-- Q2: Plays coverage vs games (the known gap)
SELECT
    g.season,
    COUNT(DISTINCT g."gameId") AS total_games,
    COUNT(DISTINCT p."gameId") AS games_with_plays,
    COUNT(DISTINCT g."gameId") - COUNT(DISTINCT p."gameId") AS games_without_plays,
    ROUND(CAST(COUNT(DISTINCT p."gameId") AS DOUBLE) / NULLIF(COUNT(DISTINCT g."gameId"), 0) * 100, 2) AS coverage_pct
FROM fct_games g
LEFT JOIN (SELECT DISTINCT "gameId" FROM fct_plays) p ON g."gameId" = p."gameId"
GROUP BY g.season
ORDER BY g.season;


-- Q3: Substitutions coverage vs games
SELECT
    g.season,
    COUNT(DISTINCT g."gameId") AS total_games,
    COUNT(DISTINCT s."gameId") AS games_with_subs,
    COUNT(DISTINCT g."gameId") - COUNT(DISTINCT s."gameId") AS games_without_subs,
    ROUND(CAST(COUNT(DISTINCT s."gameId") AS DOUBLE) / NULLIF(COUNT(DISTINCT g."gameId"), 0) * 100, 2) AS coverage_pct
FROM fct_games g
LEFT JOIN (SELECT DISTINCT "gameId" FROM fct_substitutions) s ON g."gameId" = s."gameId"
GROUP BY g.season
ORDER BY g.season;


-- Q4: Lineups coverage vs games
SELECT
    g.season,
    COUNT(DISTINCT g."gameId") AS total_games,
    COUNT(DISTINCT l."gameId") AS games_with_lineups,
    COUNT(DISTINCT g."gameId") - COUNT(DISTINCT l."gameId") AS games_without_lineups,
    ROUND(CAST(COUNT(DISTINCT l."gameId") AS DOUBLE) / NULLIF(COUNT(DISTINCT g."gameId"), 0) * 100, 2) AS coverage_pct
FROM fct_games g
LEFT JOIN (SELECT DISTINCT "gameId" FROM fct_lineups) l ON g."gameId" = l."gameId"
GROUP BY g.season
ORDER BY g.season;


-- Q5: Missing plays — specific gameIds to backfill
SELECT
    g."gameId",
    SUBSTR(CAST(g."startDate" AS VARCHAR), 1, 10) AS game_date,
    g.season
FROM fct_games g
LEFT JOIN fct_plays p ON g."gameId" = p."gameId"
WHERE p."gameId" IS NULL
ORDER BY g.season, g."gameId";


-- Q6: Missing substitutions — specific gameIds
SELECT
    g."gameId",
    SUBSTR(CAST(g."startDate" AS VARCHAR), 1, 10) AS game_date,
    g.season
FROM fct_games g
LEFT JOIN fct_substitutions s ON g."gameId" = s."gameId"
WHERE s."gameId" IS NULL
ORDER BY g.season, g."gameId";


-- Q7: Dimension table sanity check
SELECT 'dim_teams' AS table_name, COUNT(*) AS record_count FROM dim_teams
UNION ALL SELECT 'dim_conferences', COUNT(*) FROM dim_conferences
UNION ALL SELECT 'dim_venues', COUNT(*) FROM dim_venues
UNION ALL SELECT 'dim_lines_providers', COUNT(*) FROM dim_lines_providers
UNION ALL SELECT 'dim_play_types', COUNT(*) FROM dim_play_types;


-- Q8: Duplicate detection (should return empty)
SELECT "gameId", COUNT(*) AS cnt FROM fct_games GROUP BY "gameId" HAVING COUNT(*) > 1 LIMIT 20;
SELECT id, COUNT(*) AS cnt FROM fct_plays GROUP BY id HAVING COUNT(*) > 1 LIMIT 20;
SELECT id, COUNT(*) AS cnt FROM fct_substitutions GROUP BY id HAVING COUNT(*) > 1 LIMIT 20;


-- Q9: Season coverage matrix
SELECT
    g.season,
    CASE WHEN gc > 0 THEN 'Y' ELSE 'N' END AS has_games,
    CASE WHEN COALESCE(pc, 0) > 0 THEN 'Y' ELSE 'N' END AS has_plays,
    CASE WHEN COALESCE(sc, 0) > 0 THEN 'Y' ELSE 'N' END AS has_subs,
    CASE WHEN COALESCE(lc, 0) > 0 THEN 'Y' ELSE 'N' END AS has_lineups,
    CASE WHEN COALESCE(rc, 0) > 0 THEN 'Y' ELSE 'N' END AS has_rankings,
    ROUND(CAST(COALESCE(pc, 0) AS DOUBLE) / NULLIF(gc, 0) * 100, 1) AS plays_coverage_pct
FROM (SELECT season, COUNT(DISTINCT "gameId") AS gc FROM fct_games GROUP BY season) g
LEFT JOIN (SELECT season, COUNT(DISTINCT "gameId") AS pc FROM fct_plays GROUP BY season) p ON g.season = p.season
LEFT JOIN (SELECT season, COUNT(DISTINCT "gameId") AS sc FROM fct_substitutions GROUP BY season) s ON g.season = s.season
LEFT JOIN (SELECT season, COUNT(DISTINCT "gameId") AS lc FROM fct_lineups GROUP BY season) l ON g.season = l.season
LEFT JOIN (SELECT season, COUNT(*) AS rc FROM fct_rankings GROUP BY season) r ON g.season = r.season
ORDER BY g.season;
