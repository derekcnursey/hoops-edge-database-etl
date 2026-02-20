-- 1) Latest team ratings (adjusted)
SELECT teamId, season, MAX(asof) AS asof
FROM cbbd_silver.fct_ratings_adjusted
GROUP BY teamId, season
ORDER BY season DESC
LIMIT 100;

-- 2) Game counts per day
SELECT date, COUNT(*) AS games
FROM cbbd_silver.fct_games
GROUP BY date
ORDER BY date DESC
LIMIT 30;

-- 3) Team season stats by season
SELECT season, teamId, AVG(points) AS avg_points
FROM cbbd_silver.fct_team_season_stats
GROUP BY season, teamId
ORDER BY season DESC
LIMIT 100;

-- 4) Market lines history (spread)
SELECT gameId, providerId, spread, date
FROM cbbd_silver.fct_lines
WHERE spread IS NOT NULL
ORDER BY date DESC
LIMIT 100;

-- 5) Top recruits by rating
SELECT season, playerId, rating
FROM cbbd_silver.fct_recruiting_players
ORDER BY season DESC, rating DESC
LIMIT 50;
