# Gold Layer Design Document

## Overview

The gold layer transforms silver-layer star-schema tables into analytics-ready datasets
optimized for college basketball analysis, prediction modeling, and betting research.
All gold tables live in the `cbbd_gold` Glue database and are written to
`s3://hoops-edge/gold/{table_name}/season={season}/asof={date}/part-{hash8}.parquet`.

Gold transforms are idempotent: re-running for the same season overwrites cleanly.

---

## Table 1: `team_power_rankings`

### Description
Composite team ranking combining adjusted ratings, PBP-derived efficiencies, SRS ratings,
and poll rankings. One row per team per season. This is the "who is good" table.

### Source Silver Tables
- `fct_ratings_adjusted` -- API adjusted offensive/defensive/net ratings and national rankings
- `fct_ratings_srs` -- Simple Rating System (SRS) ratings
- `fct_rankings` -- AP/Coaches poll rankings (latest poll date per season)
- `fct_pbp_team_daily_rollup` -- PBP-derived cumulative season stats (possessions-based efficiency)
- `fct_pbp_team_daily_rollup_adj` -- PBP-derived opponent-adjusted efficiency ratings
- `dim_teams` -- team name, conference enrichment

### Columns

| Column | Type | Description |
|--------|------|-------------|
| teamId | int64 | Primary key (team identifier) |
| season | int32 | Season year (e.g. 2024) |
| team | string | School name |
| conference | string | Conference name |
| adj_off_rating | float64 | API adjusted offensive rating |
| adj_def_rating | float64 | API adjusted defensive rating |
| adj_net_rating | float64 | API adjusted net rating |
| ranking_offense | int64 | National rank for offense (API) |
| ranking_defense | int64 | National rank for defense (API) |
| ranking_net | int64 | National rank overall (API) |
| srs_rating | float64 | Simple Rating System value |
| ap_rank | int64 | Latest AP poll ranking (null if unranked) |
| coaches_rank | int64 | Latest Coaches poll ranking (null if unranked) |
| pbp_off_eff | float64 | PBP-derived raw offensive efficiency (pts/100 poss) |
| pbp_def_eff | float64 | PBP-derived raw defensive efficiency (pts/100 poss) |
| pbp_net_eff | float64 | pbp_off_eff - pbp_def_eff |
| pbp_adj_off_eff | float64 | PBP opponent-adjusted offensive efficiency |
| pbp_adj_def_eff | float64 | PBP opponent-adjusted defensive efficiency |
| pbp_adj_net_eff | float64 | PBP opponent-adjusted net efficiency |
| pbp_pace | float64 | PBP-derived possessions per 40 minutes |
| games_played | int64 | Number of games in the PBP rollup |
| composite_rank | float64 | Blended ranking: average of normalized net ratings from API + PBP-adj + SRS |

### Primary Key
`(teamId, season)`

### Partition Strategy
`season={YYYY}/asof={YYYY-MM-DD}/`

### Transform Logic
1. Read `fct_ratings_adjusted` for the season; select `teamid`, `team`, `conference`, `offenserating`, `defenserating`, `netrating`, `ranking_offense`, `ranking_defense`, `ranking_net`.
2. Read `fct_ratings_srs` for the season; join on `teamId`; bring in `rating` as `srs_rating`.
3. Read `fct_rankings` for the season; filter to latest `pollDate` per `pollType`; pivot AP and Coaches rankings; left join on `teamId`.
4. Read `fct_pbp_team_daily_rollup`; compute `pbp_off_eff = (team_points_total / team_possessions) * 100`, `pbp_def_eff = (opp_points_total / opp_possessions) * 100`, `pbp_net_eff = pbp_off_eff - pbp_def_eff`, `pbp_pace = team_possessions / games_played * (40 / (game_minutes_total / games_played))`.
5. Read `fct_pbp_team_daily_rollup_adj`; join on `teamid`; bring in `adj_off_eff`, `adj_def_eff`, `adj_net_eff` as PBP-adjusted values.
6. Compute `composite_rank` = average of the percentile-normalized values of `adj_net_rating` (API), `pbp_adj_net_eff`, and `srs_rating`. Higher is better.
7. Join `dim_teams` for school/conference if missing.

---

## Table 2: `game_predictions_features`

### Description
Pre-game feature vector for each game: team stats entering the game, rest days,
home/away, conference matchup, and line/spread info. One row per game-team side
(home and away). The primary ML feature table.

### Source Silver Tables
- `fct_games` -- game metadata (date, home/away teams, scores)
- `fct_game_teams` -- per-game team stats, pace
- `fct_lines` -- pre-game lines/spreads
- `fct_ratings_adjusted` -- entering ratings for each team
- `fct_ratings_srs` -- SRS entering ratings
- `fct_pbp_team_daily_rollup` -- season-to-date PBP stats
- `dim_teams` -- team name, conference

### Columns

| Column | Type | Description |
|--------|------|-------------|
| gameId | int64 | Game identifier |
| season | int32 | Season year |
| game_date | string | Game date (YYYY-MM-DD) |
| teamId | int64 | Team on this row's side |
| opponentId | int64 | Opposing team |
| is_home | bool | Whether this team is the home team |
| team_name | string | Team school name |
| team_conference | string | Team conference |
| opp_name | string | Opponent school name |
| opp_conference | string | Opponent conference |
| is_conference_game | bool | Both teams in same conference |
| spread | float64 | Spread for this team (negative = favored) |
| over_under | float64 | Total points line |
| team_moneyline | float64 | Moneyline for this team |
| opp_moneyline | float64 | Moneyline for opponent |
| team_adj_off | float64 | Team adjusted offensive rating entering game |
| team_adj_def | float64 | Team adjusted defensive rating entering game |
| team_adj_net | float64 | Team adjusted net rating entering game |
| opp_adj_off | float64 | Opponent adjusted offensive rating entering game |
| opp_adj_def | float64 | Opponent adjusted defensive rating entering game |
| opp_adj_net | float64 | Opponent adjusted net rating entering game |
| team_srs | float64 | Team SRS rating |
| opp_srs | float64 | Opponent SRS rating |
| team_ppg | float64 | Team points per game (season to date) |
| team_opp_ppg | float64 | Team opponent points per game |
| team_pace | float64 | Team pace (possessions per 40 min) |
| opp_ppg | float64 | Opponent points per game |
| opp_opp_ppg | float64 | Opponent's opponent points per game |
| opp_pace | float64 | Opponent pace |
| team_efg_pct | float64 | Team effective FG% |
| team_tov_ratio | float64 | Team turnover ratio |
| team_oreb_pct | float64 | Team offensive rebound % |
| team_ft_rate | float64 | Team free throw rate |
| opp_efg_pct | float64 | Opponent effective FG% |
| opp_tov_ratio | float64 | Opponent turnover ratio |
| opp_oreb_pct | float64 | Opponent offensive rebound % |
| opp_ft_rate | float64 | Opponent free throw rate |
| team_score | int64 | Actual team score (label, null for future games) |
| opp_score | int64 | Actual opponent score (label) |
| team_win | bool | Whether this team won (label) |

### Primary Key
`(gameId, teamId)`

### Partition Strategy
`season={YYYY}/asof={YYYY-MM-DD}/`

### Transform Logic
1. Read `fct_games` for the season; extract `gameId`, `season`, `startDate`, `homeTeamId`, `awayTeamId`, `homeScore`, `awayScore`.
2. Unpivot into two rows per game (home side, away side), setting `teamId`, `opponentId`, `is_home`.
3. Left join `fct_ratings_adjusted` on team and opponent to get entering adjusted ratings.
4. Left join `fct_ratings_srs` on team and opponent for SRS.
5. Left join `fct_lines` (use first available provider) on `gameId`; compute team-relative spread.
6. Left join `fct_pbp_team_daily_rollup` on `teamId` for season-to-date Four Factors and pace.
7. Left join `dim_teams` for names/conferences; derive `is_conference_game`.
8. Compute labels: `team_score`, `opp_score`, `team_win`.

---

## Table 3: `player_season_impact`

### Description
Player efficiency and impact metrics per season: usage, efficiency, per-40-minute stats,
shooting breakdown, and recruiting rank context. One row per player per season.

### Source Silver Tables
- `fct_player_season_stats` -- season aggregates (points, rebounds, assists, etc.)
- `fct_player_season_shooting` -- shooting splits by zone/range
- `fct_recruiting_players` -- recruiting ranking and stars
- `dim_teams` -- team name enrichment

### Columns

| Column | Type | Description |
|--------|------|-------------|
| playerId | int64 | Player identifier |
| season | int32 | Season year |
| team | string | Team/school name |
| conference | string | Conference |
| games | int64 | Games played |
| minutes | float64 | Total minutes played |
| mpg | float64 | Minutes per game |
| points | float64 | Total points |
| ppg | float64 | Points per game |
| rebounds | float64 | Total rebounds |
| rpg | float64 | Rebounds per game |
| assists | float64 | Total assists |
| apg | float64 | Assists per game |
| steals | float64 | Total steals |
| blocks | float64 | Total blocks |
| turnovers | float64 | Total turnovers |
| fgm | float64 | Field goals made |
| fga | float64 | Field goals attempted |
| fg_pct | float64 | Field goal percentage |
| fg3m | float64 | Three-pointers made |
| fg3a | float64 | Three-pointers attempted |
| fg3_pct | float64 | Three-point percentage |
| ftm | float64 | Free throws made |
| fta | float64 | Free throws attempted |
| ft_pct | float64 | Free throw percentage |
| efg_pct | float64 | Effective field goal percentage: (fgm + 0.5*fg3m) / fga |
| true_shooting | float64 | True shooting percentage: points / (2 * (fga + 0.44*fta)) |
| usage_rate | float64 | Approximate usage rate |
| per_40_pts | float64 | Points per 40 minutes |
| per_40_reb | float64 | Rebounds per 40 minutes |
| per_40_ast | float64 | Assists per 40 minutes |
| ast_to_ratio | float64 | Assist-to-turnover ratio |
| recruiting_rank | int64 | National recruiting rank (null if unavailable) |
| recruiting_stars | int64 | Recruiting star rating |
| recruiting_rating | float64 | Recruiting composite rating |

### Primary Key
`(playerId, season)`

### Partition Strategy
`season={YYYY}/asof={YYYY-MM-DD}/`

### Transform Logic
1. Read `fct_player_season_stats` for the season; extract per-season aggregates.
2. Compute derived columns: `ppg`, `rpg`, `apg`, `fg_pct`, `fg3_pct`, `ft_pct`, `efg_pct`, `true_shooting`, `per_40_*`, `ast_to_ratio`.
3. Compute `usage_rate` approximation using available counting stats.
4. Left join `fct_player_season_shooting` for shooting zone splits (if columns overlap, prefer stats table).
5. Left join `fct_recruiting_players` on `playerId` and `season` for recruiting context.
6. Left join `dim_teams` for team/conference names.

---

## Table 4: `market_lines_analysis`

### Description
Lines/spreads merged with actual game outcomes for against-the-spread (ATS) analysis
and market efficiency research. One row per game per provider.

### Source Silver Tables
- `fct_lines` -- pre-game lines (spread, over/under, moneylines) per provider
- `fct_games` -- game outcomes (scores)
- `dim_teams` -- team enrichment

### Columns

| Column | Type | Description |
|--------|------|-------------|
| gameId | int64 | Game identifier |
| season | int32 | Season year |
| game_date | string | Game date |
| provider | string | Lines provider name |
| home_team | string | Home team name |
| away_team | string | Away team name |
| home_conference | string | Home team conference |
| away_conference | string | Away team conference |
| spread | float64 | Spread (from home team perspective; negative = home favored) |
| over_under | float64 | Total points line |
| home_moneyline | float64 | Home moneyline |
| away_moneyline | float64 | Away moneyline |
| home_score | int64 | Actual home team score |
| away_score | int64 | Actual away team score |
| total_points | int64 | home_score + away_score |
| home_margin | int64 | home_score - away_score |
| home_win | bool | Whether home team won |
| home_covered | bool | Whether home team covered the spread |
| over_hit | bool | Whether total points exceeded over/under |
| ats_margin | float64 | Actual margin + spread (positive = home covered) |
| total_vs_line | float64 | total_points - over_under |
| spread_error | float64 | abs(actual margin - (-spread)): how accurate the spread was |

### Primary Key
`(gameId, provider)`

### Partition Strategy
`season={YYYY}/asof={YYYY-MM-DD}/`

### Transform Logic
1. Read `fct_lines` for the season; extract `gameId`, `provider`, `spread`, `overUnder`, `homeMoneyline`, `awayMoneyline`.
2. Read `fct_games` for the season; extract `gameId`, `season`, `startDate`, `homeTeamId`, `awayTeamId`, `homeScore`, `awayScore`.
3. Inner join on `gameId` (only games with lines).
4. Left join `dim_teams` twice (home and away) for team names and conferences.
5. Compute derived columns:
   - `total_points = homeScore + awayScore`
   - `home_margin = homeScore - awayScore`
   - `home_win = homeScore > awayScore`
   - `home_covered = home_margin + spread > 0` (spread is negative when home is favored)
   - `over_hit = total_points > over_under`
   - `ats_margin = home_margin + spread`
   - `total_vs_line = total_points - over_under`
   - `spread_error = abs(home_margin - (-spread))`

---

## Table 5: `team_season_summary`

### Description
Comprehensive team season profile: win/loss record, conference record,
offensive/defensive rankings, key statistical summaries, recruiting class quality.
One row per team per season.

### Source Silver Tables
- `fct_games` -- game results for W/L record
- `fct_game_teams` -- per-game team stats
- `fct_ratings_adjusted` -- adjusted ratings
- `fct_ratings_srs` -- SRS
- `fct_team_season_stats` -- season aggregate stats
- `fct_recruiting_players` -- recruiting class aggregation
- `dim_teams` -- team name, conference

### Columns

| Column | Type | Description |
|--------|------|-------------|
| teamId | int64 | Team identifier |
| season | int32 | Season year |
| team | string | School name |
| conference | string | Conference |
| wins | int64 | Total wins |
| losses | int64 | Total losses |
| win_pct | float64 | Win percentage |
| conf_wins | int64 | Conference wins |
| conf_losses | int64 | Conference losses |
| conf_win_pct | float64 | Conference win percentage |
| ppg | float64 | Points per game |
| opp_ppg | float64 | Opponent points per game |
| margin | float64 | Average scoring margin |
| adj_off_rating | float64 | Adjusted offensive rating |
| adj_def_rating | float64 | Adjusted defensive rating |
| adj_net_rating | float64 | Adjusted net rating |
| srs_rating | float64 | SRS rating |
| efg_pct | float64 | Effective FG% |
| opp_efg_pct | float64 | Opponent effective FG% |
| tov_ratio | float64 | Turnover ratio |
| opp_tov_ratio | float64 | Opponent turnover ratio |
| oreb_pct | float64 | Offensive rebound % |
| opp_oreb_pct | float64 | Opponent offensive rebound % |
| ft_rate | float64 | Free throw rate |
| opp_ft_rate | float64 | Opponent free throw rate |
| pace | float64 | Tempo (possessions per 40 min) |
| recruiting_avg_rating | float64 | Average recruiting rating of incoming class |
| recruiting_top_star | int64 | Highest star rating in recruiting class |
| recruiting_class_size | int64 | Number of recruits |

### Primary Key
`(teamId, season)`

### Partition Strategy
`season={YYYY}/asof={YYYY-MM-DD}/`

### Transform Logic
1. Read `fct_games` for the season; determine each team's W/L from home/away scores.
2. Join `dim_teams` to determine conference membership; compute conference-only record.
3. Read `fct_team_season_stats` for basic counting stats; compute `ppg`, `opp_ppg`, `margin`.
4. Read `fct_ratings_adjusted` and `fct_ratings_srs` for efficiency metrics.
5. Read `fct_pbp_team_daily_rollup` for Four Factors and pace.
6. Read `fct_recruiting_players` for the season; aggregate by team to get class-level metrics.
7. Left join everything onto the team-season spine.

---

## Cross-Cutting Concerns

### Idempotency
Each gold build writes to `season={YYYY}/asof={YYYY-MM-DD}/` partitions. Re-running
the same season on the same day produces the same partition path and overwrites cleanly.

### Null Handling
Source silver tables may have missing data for certain teams/seasons. All joins are
LEFT joins from the primary spine table to preserve all rows. Derived columns that
divide by zero should return `null`.

### Glue Registration
All gold tables are registered in the `cbbd_gold` Glue database using the same
`GlueCatalog.ensure_table()` pattern used for silver tables.

### File Naming
Parquet files follow the existing convention: `part-{hash8}.parquet` where the hash
is derived from the table name and season for deterministic overwrites.
