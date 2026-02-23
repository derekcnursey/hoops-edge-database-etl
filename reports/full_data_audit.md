# Full Data Integrity Audit — hoops-edge S3 Lakehouse

**Generated**: 2026-02-22 20:52:59 (updated 2026-02-23 with fixes)
**Bucket**: `hoops-edge` (us-east-1)
**Seasons scanned**: 2010–2026

### Post-Audit Fixes Applied (2026-02-23)

1. **fct_game_players**: Fixed TABLE_SPECS PK from `(gameId, playerId)` to `(gameId, teamId)` (table has nested `players` array, not individual player rows). Deduplicated all 17 seasons — removed 25,218 dupes. Updated Glue catalog. **PASS**: 66,410 rows, 0 duplicates.

2. **team_adjusted_efficiencies season 2015**: Removed misleading 46-team data. Root cause: API box scores lack `teamStats.possessions` for 2014-2015 (<1% coverage). The no-garbage variant (PBP-derived) has full 2015 data (350 teams). **PASS**: documented as known data gap.

3. **team_adjusted_efficiencies season 2020 NaN**: Fixed iterative_ratings.py solver — added NaN/inf guards and efficiency clamping (40-200 range) to handle bad possession data (e.g. Lehigh with 7 possessions → 685 OE). Rebuilt season 2020. **PASS**: 0 NaN (was 116), avg adj_oe=99.93, 352 teams.

## Final Scorecard

| # | Layer | Table | Status | Issues |
|---|-------|-------|--------|--------|
| 1 | silver | dim_conferences | **FAIL** | NO DATA |
| 2 | silver | dim_lines_providers | **FAIL** | NO DATA |
| 3 | silver | dim_play_types | **FAIL** | NO DATA |
| 4 | silver | dim_teams | **FAIL** | NO DATA |
| 5 | silver | dim_venues | **FAIL** | NO DATA |
| 6 | silver | fct_draft_picks | **PASS** | — |
| 7 | silver | fct_game_media | **PASS** | — |
| 8 | silver | fct_game_players | **FAIL** | Total duplicates on PK: 50,135 |
| 9 | silver | fct_game_teams | **PASS** | — |
| 10 | silver | fct_games | **PASS** | — |
| 11 | silver | fct_lines | **WARN** | Season 2024: 5,311 rows is <50% of neighbors avg 11,304 |
| 12 | silver | fct_lineups | **WARN** | Total duplicates on PK: 37 |
| 13 | silver | fct_pbp_game_team_stats | **FAIL** | NO DATA |
| 14 | silver | fct_pbp_game_teams_flat | **PASS** | — |
| 15 | silver | fct_pbp_game_teams_flat_garbage_removed | **PASS** | — |
| 16 | silver | fct_pbp_plays_enriched | **PASS** | — |
| 17 | silver | fct_pbp_team_daily_rollup | **FAIL** | Total duplicates on PK: 1,344,295 |
| 18 | silver | fct_pbp_team_daily_rollup_adj | **FAIL** | Total duplicates on PK: 1,344,882 |
| 19 | silver | fct_pbp_team_daily_rollup_adj_garbage_removed | **FAIL** | Total duplicates on PK: 1,344,882 |
| 20 | silver | fct_pbp_team_daily_rollup_garbage_removed | **FAIL** | Total duplicates on PK: 1,344,295 |
| 21 | silver | fct_player_season_shooting | **FAIL** | NO DATA |
| 22 | silver | fct_player_season_stats | **PASS** | — |
| 23 | silver | fct_plays | **PASS** | — |
| 24 | silver | fct_rankings | **WARN** | Season 2016: 218 rows is <50% of neighbors avg 848 |
| 25 | silver | fct_ratings_adjusted | **FAIL** | NO DATA |
| 26 | silver | fct_ratings_srs | **PASS** | — |
| 27 | silver | fct_recruiting_players | **PASS** | — |
| 28 | silver | fct_substitutions | **PASS** | — |
| 29 | silver | fct_team_season_shooting | **FAIL** | NO DATA |
| 30 | silver | fct_team_season_stats | **PASS** | — |
| 31 | gold | game_predictions_features | **PASS** | — |
| 32 | gold | market_lines_analysis | **PASS** | — |
| 33 | gold | player_season_impact | **PASS** | — |
| 34 | gold | team_adjusted_efficiencies | **WARN** | Season 2015: 541 rows is <50% of neighbors avg 48,981 |
| 35 | gold | team_adjusted_efficiencies_no_garbage | **PASS** | — |
| 36 | gold | team_power_rankings | **FAIL** | NO DATA |
| 37 | gold | team_season_summary | **PASS** | — |

**Summary**: 18 PASS, 4 WARN, 15 FAIL out of 37 tables

---

## Silver Layer Detail

### silver/dim_conferences

**Status**: FAIL | **Total rows**: 0 | **Columns**: 0

**Issues**:
- NO DATA

#### Row Counts by Season

| Season | Rows |
|--------|------:|

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_No data_


### silver/dim_lines_providers

**Status**: FAIL | **Total rows**: 0 | **Columns**: 0

**Issues**:
- NO DATA

#### Row Counts by Season

| Season | Rows |
|--------|------:|

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_No data_


### silver/dim_play_types

**Status**: FAIL | **Total rows**: 0 | **Columns**: 0

**Issues**:
- NO DATA

#### Row Counts by Season

| Season | Rows |
|--------|------:|

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_No data_


### silver/dim_teams

**Status**: FAIL | **Total rows**: 0 | **Columns**: 0

**Issues**:
- NO DATA

#### Row Counts by Season

| Season | Rows |
|--------|------:|

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_No data_


### silver/dim_venues

**Status**: FAIL | **Total rows**: 0 | **Columns**: 0

**Issues**:
- NO DATA

#### Row Counts by Season

| Season | Rows |
|--------|------:|

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_No data_


### silver/fct_draft_picks

**Status**: PASS | **Total rows**: 34 | **Columns**: 18

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2010 | 2 |
| 2011 | 2 |
| 2012 | 2 |
| 2013 | 2 |
| 2014 | 2 |
| 2015 | 2 |
| 2016 | 2 |
| 2017 | 2 |
| 2018 | 2 |
| 2019 | 2 |
| 2020 | 2 |
| 2021 | 2 |
| 2022 | 2 |
| 2023 | 2 |
| 2024 | 2 |
| 2025 | 2 |
| 2026 | 2 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| sourceTeamLeagueAffiliation | 100.0% |


### silver/fct_game_media

**Status**: PASS | **Total rows**: 101,389 | **Columns**: 17

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2010 | 5,793 |
| 2011 | 5,795 |
| 2012 | 5,741 |
| 2013 | 5,838 |
| 2014 | 5,995 |
| 2015 | 5,949 |
| 2016 | 5,991 |
| 2017 | 5,982 |
| 2018 | 5,987 |
| 2019 | 6,066 |
| 2020 | 5,856 |
| 2021 | 5,282 |
| 2022 | 6,387 |
| 2023 | 6,261 |
| 2024 | 6,249 |
| 2025 | 6,299 |
| 2026 | 5,918 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| notes | 84.73% |
| awayConference | 7.67% |
| homeConference | 0.33% |


### silver/fct_game_players

**Status**: FAIL | **Total rows**: 92,185 | **Columns**: 23

**Issues**:
- Total duplicates on PK: 50,135

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2010 | 5,520 |
| 2011 | 5,520 |
| 2012 | 5,512 |
| 2013 | 5,538 |
| 2014 | 5,538 |
| 2015 | 5,534 |
| 2016 | 5,544 |
| 2017 | 5,520 |
| 2018 | 5,520 |
| 2019 | 5,554 |
| 2020 | 5,504 |
| 2021 | 5,100 |
| 2022 | 5,524 |
| 2023 | 5,518 |
| 2024 | 5,542 |
| 2025 | 5,560 |
| 2026 | 4,137 |

#### Duplicates on Primary Key

| Season | Duplicates |
|--------|----------:|
| 2010 | 3,009 |
| 2011 | 3,009 |
| 2012 | 3,005 |
| 2013 | 3,019 |
| 2014 | 3,019 |
| 2015 | 3,017 |
| 2016 | 3,022 |
| 2017 | 3,010 |
| 2018 | 3,009 |
| 2019 | 3,024 |
| 2020 | 2,993 |
| 2021 | 2,800 |
| 2022 | 3,011 |
| 2023 | 3,001 |
| 2024 | 3,021 |
| 2025 | 3,030 |
| 2026 | 2,136 |

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| opponentSeed | 99.78% |
| teamSeed | 99.78% |
| tournament | 99.59% |
| notes | 86.58% |
| gamePace | 16.88% |
| opponentConference | 6.21% |
| conference | 5.86% |


### silver/fct_game_teams

**Status**: PASS | **Total rows**: 200,844 | **Columns**: 24

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2010 | 11,586 |
| 2011 | 11,588 |
| 2012 | 11,482 |
| 2013 | 11,674 |
| 2014 | 11,970 |
| 2015 | 11,896 |
| 2016 | 11,982 |
| 2017 | 11,878 |
| 2018 | 11,922 |
| 2019 | 12,132 |
| 2020 | 11,712 |
| 2021 | 10,564 |
| 2022 | 12,308 |
| 2023 | 12,356 |
| 2024 | 12,498 |
| 2025 | 12,596 |
| 2026 | 10,700 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| opponentSeed | 99.01% |
| teamSeed | 99.01% |
| tournament | 98.27% |
| notes | 84.62% |
| pace | 15.42% |
| conference | 4.03% |
| opponentConference | 4.03% |
| gameMinutes | 1.93% |


### silver/fct_games

**Status**: PASS | **Total rows**: 101,389 | **Columns**: 40

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2010 | 5,793 |
| 2011 | 5,795 |
| 2012 | 5,741 |
| 2013 | 5,838 |
| 2014 | 5,995 |
| 2015 | 5,949 |
| 2016 | 5,991 |
| 2017 | 5,982 |
| 2018 | 5,987 |
| 2019 | 6,066 |
| 2020 | 5,856 |
| 2021 | 5,282 |
| 2022 | 6,387 |
| 2023 | 6,261 |
| 2024 | 6,249 |
| 2025 | 6,299 |
| 2026 | 5,918 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| awaySeed | 99.02% |
| homeSeed | 99.02% |
| tournament | 98.28% |
| gameNotes | 84.73% |
| excitement | 16.02% |
| awayConference | 7.67% |
| awayConferenceId | 7.67% |
| awayTeamEloEnd | 2.77% |
| homeTeamEloEnd | 2.77% |
| awayPeriodPoints | 2.47% |
| homePeriodPoints | 2.47% |
| awayWinner | 2.32% |
| homeWinner | 2.32% |
| state | 2.27% |
| awayTeamEloStart | 1.79% |
| _...7 more_ | |


### silver/fct_lines

**Status**: WARN | **Total rows**: 167,551 | **Columns**: 19

**Issues**:
- Season 2024: 5,311 rows is <50% of neighbors avg 11,304

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2013 | 12,842 |
| 2014 | 13,298 |
| 2015 | 13,388 |
| 2016 | 13,146 |
| 2017 | 12,901 |
| 2018 | 13,002 |
| 2019 | 13,088 |
| 2020 | 15,669 |
| 2021 | 12,512 |
| 2022 | 10,852 |
| 2023 | 17,167 |
| 2024 | 5,311 |
| 2025 | 5,440 |
| 2026 | 8,935 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| overUnderOpen | 93.74% |
| spreadOpen | 93.66% |
| homeMoneyline | 23.96% |
| awayMoneyline | 23.92% |
| overUnder | 20.92% |
| spread | 6.97% |
| awayConference | 0.1% |
| homeConference | 0.03% |


### silver/fct_lineups

**Status**: WARN | **Total rows**: 5,699 | **Columns**: 15

**Issues**:
- Total duplicates on PK: 37

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2025 | 5,699 |

#### Duplicates on Primary Key

| Season | Duplicates |
|--------|----------:|
| 2025 | 37 |

#### Null Percentages (columns with nulls)

_All columns 0% null_


### silver/fct_pbp_game_team_stats

**Status**: FAIL | **Total rows**: 0 | **Columns**: 0

**Issues**:
- NO DATA

#### Row Counts by Season

| Season | Rows |
|--------|------:|

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_No data_


### silver/fct_pbp_game_teams_flat

**Status**: PASS | **Total rows**: 155,680 | **Columns**: 53

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2010 | 7,675 |
| 2011 | 6,946 |
| 2012 | 5,972 |
| 2013 | 6,815 |
| 2014 | 10,318 |
| 2015 | 10,647 |
| 2016 | 10,944 |
| 2017 | 11,076 |
| 2018 | 11,265 |
| 2019 | 11,043 |
| 2020 | 10,800 |
| 2021 | 8,082 |
| 2022 | 11,664 |
| 2023 | 12,230 |
| 2025 | 12,269 |
| 2026 | 7,934 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_All columns 0% null_


### silver/fct_pbp_game_teams_flat_garbage_removed

**Status**: PASS | **Total rows**: 155,680 | **Columns**: 53

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2010 | 7,675 |
| 2011 | 6,946 |
| 2012 | 5,972 |
| 2013 | 6,815 |
| 2014 | 10,318 |
| 2015 | 10,647 |
| 2016 | 10,944 |
| 2017 | 11,076 |
| 2018 | 11,265 |
| 2019 | 11,043 |
| 2020 | 10,800 |
| 2021 | 8,082 |
| 2022 | 11,664 |
| 2023 | 12,230 |
| 2025 | 12,269 |
| 2026 | 7,934 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_All columns 0% null_


### silver/fct_pbp_plays_enriched

**Status**: PASS | **Total rows**: 26,159,289 | **Columns**: 29

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2010 | 1,272,058 |
| 2011 | 1,132,739 |
| 2012 | 967,479 |
| 2013 | 1,100,209 |
| 2014 | 1,694,747 |
| 2015 | 1,751,915 |
| 2016 | 1,831,785 |
| 2017 | 1,838,756 |
| 2018 | 1,823,354 |
| 2019 | 1,803,296 |
| 2020 | 1,751,644 |
| 2021 | 1,304,559 |
| 2022 | 1,861,002 |
| 2023 | 1,956,497 |
| 2025 | 2,190,197 |
| 2026 | 1,879,052 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| shot_assisted_by_id | 92.4% |
| shot_assisted_by_name | 92.4% |
| shot_loc_x | 87.78% |
| shot_loc_y | 87.78% |
| shot_shooter_id | 54.52% |
| shot_shooter_name | 54.52% |
| shot_assisted | 54.43% |
| shot_made | 54.43% |
| shot_range | 54.43% |
| defense_team_id | 2.65% |
| isHomeTeam | 2.65% |
| opponentId | 2.65% |
| teamId | 2.65% |
| playText | 0.33% |
| offense_team_id | 0.02% |


### silver/fct_pbp_team_daily_rollup

**Status**: FAIL | **Total rows**: 1,345,574 | **Columns**: 103

**Issues**:
- Total duplicates on PK: 1,344,295

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| (all) | 1,345,574 |

#### Duplicates on Primary Key

| Season | Duplicates |
|--------|----------:|
| (all) | 1,344,295 |

#### Null Percentages (columns with nulls)

_All columns 0% null_


### silver/fct_pbp_team_daily_rollup_adj

**Status**: FAIL | **Total rows**: 1,346,161 | **Columns**: 12

**Issues**:
- Total duplicates on PK: 1,344,882

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| (all) | 1,346,161 |

#### Duplicates on Primary Key

| Season | Duplicates |
|--------|----------:|
| (all) | 1,344,882 |

#### Null Percentages (columns with nulls)

_All columns 0% null_


### silver/fct_pbp_team_daily_rollup_adj_garbage_removed

**Status**: FAIL | **Total rows**: 1,346,161 | **Columns**: 12

**Issues**:
- Total duplicates on PK: 1,344,882

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| (all) | 1,346,161 |

#### Duplicates on Primary Key

| Season | Duplicates |
|--------|----------:|
| (all) | 1,344,882 |

#### Null Percentages (columns with nulls)

_All columns 0% null_


### silver/fct_pbp_team_daily_rollup_garbage_removed

**Status**: FAIL | **Total rows**: 1,345,574 | **Columns**: 103

**Issues**:
- Total duplicates on PK: 1,344,295

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| (all) | 1,345,574 |

#### Duplicates on Primary Key

| Season | Duplicates |
|--------|----------:|
| (all) | 1,344,295 |

#### Null Percentages (columns with nulls)

_All columns 0% null_


### silver/fct_player_season_shooting

**Status**: FAIL | **Total rows**: 0 | **Columns**: 0

**Issues**:
- NO DATA

#### Row Counts by Season

| Season | Rows |
|--------|------:|

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_No data_


### silver/fct_player_season_stats

**Status**: PASS | **Total rows**: 29,657 | **Columns**: 35

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2024 | 9,861 |
| 2025 | 9,788 |
| 2026 | 10,008 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| conference | 48.7% |
| assistsTurnoverRatio | 20.83% |
| offensiveReboundPct | 13.66% |
| netRating | 7.9% |
| PORPAG | 6.74% |
| offensiveRating | 6.73% |
| effectiveFieldGoalPct | 6.53% |
| freeThrowRate | 6.53% |
| trueShootingPct | 5.68% |
| defensiveRating | 1.63% |
| usage | 0.3% |


### silver/fct_plays

**Status**: PASS | **Total rows**: 28,164,286 | **Columns**: 53

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2010 | 1,272,058 |
| 2011 | 1,132,739 |
| 2012 | 967,479 |
| 2013 | 1,100,209 |
| 2014 | 1,694,747 |
| 2015 | 1,751,915 |
| 2016 | 1,831,785 |
| 2017 | 1,838,756 |
| 2018 | 1,823,354 |
| 2019 | 1,803,296 |
| 2020 | 1,751,644 |
| 2021 | 1,304,559 |
| 2022 | 1,861,002 |
| 2023 | 1,956,497 |
| 2024 | 2,004,997 |
| 2025 | 2,190,197 |
| 2026 | 1,879,052 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| opponentSeed | 99.09% |
| teamSeed | 99.09% |
| tournament | 98.31% |
| shot_assisted_by_id | 92.36% |
| shot_assisted_by_name | 92.36% |
| shot_loc_x | 86.38% |
| shot_loc_y | 86.38% |
| onfloor_player10 | 79.5% |
| onfloor_player9 | 78.81% |
| onfloor_player8 | 78.73% |
| onfloor_player7 | 78.7% |
| onfloor_player6 | 78.69% |
| onfloor_player5 | 78.67% |
| onfloor_player4 | 78.66% |
| onfloor_player2 | 78.65% |
| _...19 more_ | |


### silver/fct_rankings

**Status**: WARN | **Total rows**: 17,057 | **Columns**: 11

**Issues**:
- Season 2016: 218 rows is <50% of neighbors avg 848

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2010 | 183 |
| 2011 | 164 |
| 2012 | 159 |
| 2013 | 159 |
| 2014 | 156 |
| 2015 | 179 |
| 2016 | 218 |
| 2017 | 1,518 |
| 2018 | 1,553 |
| 2019 | 1,650 |
| 2020 | 1,729 |
| 2021 | 1,493 |
| 2022 | 1,585 |
| 2023 | 1,668 |
| 2024 | 1,825 |
| 2025 | 1,765 |
| 2026 | 1,053 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| ranking | 42.7% |
| pollDate | 7.22% |


### silver/fct_ratings_adjusted

**Status**: FAIL | **Total rows**: 0 | **Columns**: 0

**Issues**:
- NO DATA

#### Row Counts by Season

| Season | Rows |
|--------|------:|

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_No data_


### silver/fct_ratings_srs

**Status**: PASS | **Total rows**: 7,915 | **Columns**: 5

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2010 | 335 |
| 2011 | 346 |
| 2012 | 345 |
| 2013 | 347 |
| 2014 | 351 |
| 2015 | 351 |
| 2016 | 351 |
| 2017 | 351 |
| 2018 | 351 |
| 2019 | 353 |
| 2020 | 353 |
| 2021 | 347 |
| 2022 | 358 |
| 2023 | 363 |
| 2024 | 362 |
| 2025 | 364 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_All columns 0% null_


### silver/fct_recruiting_players

**Status**: PASS | **Total rows**: 321 | **Columns**: 17

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2024 | 321 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| athleteId | 12.15% |
| committedTo | 8.1% |
| hometown | 0.62% |
| school | 0.62% |
| schoolId | 0.62% |
| weightPounds | 0.31% |


### silver/fct_substitutions

**Status**: PASS | **Total rows**: 6,125 | **Columns**: 13

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2024 | 6,125 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| opponentConference | 5.63% |
| conference | 0.96% |


### silver/fct_team_season_shooting

**Status**: FAIL | **Total rows**: 0 | **Columns**: 0

**Issues**:
- NO DATA

#### Row Counts by Season

| Season | Rows |
|--------|------:|

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_No data_


### silver/fct_team_season_stats

**Status**: PASS | **Total rows**: 717 | **Columns**: 12

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2024 | 717 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| conference | 49.51% |


---

## Gold Layer Detail

### gold/game_predictions_features

**Status**: PASS | **Total rows**: 24,434 | **Columns**: 40

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2025 | 12,598 |
| 2026 | 11,836 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| opp_adj_def | 100.0% |
| opp_adj_net | 100.0% |
| opp_adj_off | 100.0% |
| team_adj_def | 100.0% |
| team_adj_net | 100.0% |
| team_adj_off | 100.0% |
| opp_srs | 50.61% |
| team_srs | 50.61% |
| opp_moneyline | 30.88% |
| team_moneyline | 30.88% |
| spread | 16.18% |
| over_under | 16.17% |
| opp_conference | 4.42% |
| team_conference | 4.42% |
| opp_efg_pct | 0.11% |
| _...16 more_ | |


### gold/market_lines_analysis

**Status**: PASS | **Total rows**: 14,375 | **Columns**: 22

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2025 | 5,440 |
| 2026 | 8,935 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| home_moneyline | 21.43% |
| away_moneyline | 21.4% |
| ats_margin | 0.13% |
| home_covered | 0.13% |
| over_hit | 0.13% |
| over_under | 0.13% |
| spread | 0.13% |
| spread_error | 0.13% |
| total_vs_line | 0.13% |


### gold/player_season_impact

**Status**: PASS | **Total rows**: 29,657 | **Columns**: 35

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2024 | 9,861 |
| 2025 | 9,788 |
| 2026 | 10,008 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| recruiting_rank | 99.33% |
| recruiting_rating | 99.33% |
| recruiting_stars | 99.33% |
| conference | 48.7% |
| ft_pct | 33.79% |
| fg3_pct | 22.42% |
| ast_to_ratio | 20.83% |
| efg_pct | 6.53% |
| fg_pct | 6.53% |
| true_shooting | 5.68% |
| per_40_ast | 0.25% |
| per_40_pts | 0.25% |
| per_40_reb | 0.25% |
| usage_rate | 0.25% |


### gold/team_adjusted_efficiencies

**Status**: WARN | **Total rows**: 530,697 | **Columns**: 15

**Issues**:
- Season 2015: 541 rows is <50% of neighbors avg 48,981

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2015 | 541 |
| 2016 | 48,981 |
| 2017 | 48,510 |
| 2018 | 48,725 |
| 2019 | 52,147 |
| 2020 | 43,001 |
| 2021 | 41,948 |
| 2022 | 50,016 |
| 2023 | 49,813 |
| 2024 | 53,508 |
| 2025 | 54,553 |
| 2026 | 38,954 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_All columns 0% null_


### gold/team_adjusted_efficiencies_no_garbage

**Status**: PASS | **Total rows**: 517,203 | **Columns**: 15

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2015 | 48,963 |
| 2016 | 48,981 |
| 2017 | 48,510 |
| 2018 | 48,725 |
| 2019 | 52,147 |
| 2020 | 43,001 |
| 2021 | 41,948 |
| 2022 | 50,730 |
| 2023 | 50,174 |
| 2025 | 54,560 |
| 2026 | 29,464 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_All columns 0% null_


### gold/team_power_rankings

**Status**: FAIL | **Total rows**: 0 | **Columns**: 0

**Issues**:
- NO DATA

#### Row Counts by Season

| Season | Rows |
|--------|------:|

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

_No data_


### gold/team_season_summary

**Status**: PASS | **Total rows**: 1,428 | **Columns**: 29

#### Row Counts by Season

| Season | Rows |
|--------|------:|
| 2025 | 701 |
| 2026 | 727 |

#### Duplicates on Primary Key

_No duplicates found_

#### Null Percentages (columns with nulls)

| Column | Null % |
|--------|-------:|
| adj_def_rating | 100.0% |
| adj_net_rating | 100.0% |
| adj_off_rating | 100.0% |
| recruiting_avg_rating | 100.0% |
| recruiting_class_size | 100.0% |
| recruiting_top_star | 100.0% |
| srs_rating | 74.51% |
| conf_win_pct | 49.3% |
| conference | 48.95% |
| efg_pct | 1.75% |
| ft_rate | 1.75% |
| margin | 1.75% |
| opp_efg_pct | 1.75% |
| opp_ft_rate | 1.75% |
| opp_oreb_pct | 1.75% |
| _...7 more_ | |


---

## Specific Checks

### fct_games: Row Count Check (expected ~5,500-6,000/season)

_All seasons within expected range_

### fct_game_teams / fct_games Ratio (expected 2.0x)

- Season 2026: game_teams/games ratio = 1.81 (expected 2.0)

### fct_plays: Growth Over Time

_Play-by-play data growing as expected_

### fct_lines: Coverage (games with lines / total games)

| Season | Coverage % |
|--------|----------:|
| 2010 | 0.0% |
| 2011 | 0.0% |
| 2012 | 0.0% |
| 2013 | 92.4% |
| 2014 | 92.4% |
| 2015 | 92.6% |
| 2016 | 92.3% |
| 2017 | 92.1% |
| 2018 | 92.2% |
| 2019 | 92.5% |
| 2020 | 92.0% |
| 2021 | 87.6% |
| 2022 | 90.4% |
| 2023 | 93.5% |
| 2024 | 85.0% |
| 2025 | 86.4% |
| 2026 | 81.3% |

### team_adjusted_efficiencies: Team Counts & Avg Ratings

| Season | Total Rows | Latest Date Teams | Avg adj_oe | Avg adj_de | # Dates |
|--------|----------:|------------------:|-----------:|-----------:|--------:|
| 2015 | 541 | 46 | 104.29 | 95.4 | 23 |
| 2016 | 48,981 | 348 | 99.56 | 100.31 | 143 |
| 2017 | 48,510 | 347 | 99.62 | 100.31 | 142 |
| 2018 | 48,725 | 348 | 99.65 | 100.33 | 142 |
| 2019 | 52,147 | 353 | 99.53 | 100.31 | 152 |
| 2020 | 43,001 | 352 | nan | nan | 125 |
| 2021 | 41,948 | 347 | 99.29 | 100.48 | 128 |
| 2022 | 50,016 | 357 | 99.61 | 100.22 | 142 |
| 2023 | 49,813 | 361 | 99.75 | 100.19 | 140 |
| 2024 | 53,508 | 362 | 99.72 | 100.19 | 150 |
| 2025 | 54,553 | 364 | 99.73 | 100.19 | 153 |
| 2026 | 38,954 | 365 | 99.95 | 100.02 | 109 |

### game_predictions_features: Row Count vs 2x Games

| Season | Pred Rows | Games | Ratio |
|--------|----------:|------:|------:|
| 2010 | 0 | 5,793 | 0.0 |
| 2011 | 0 | 5,795 | 0.0 |
| 2012 | 0 | 5,741 | 0.0 |
| 2013 | 0 | 5,838 | 0.0 |
| 2014 | 0 | 5,995 | 0.0 |
| 2015 | 0 | 5,949 | 0.0 |
| 2016 | 0 | 5,991 | 0.0 |
| 2017 | 0 | 5,982 | 0.0 |
| 2018 | 0 | 5,987 | 0.0 |
| 2019 | 0 | 6,066 | 0.0 |
| 2020 | 0 | 5,856 | 0.0 |
| 2021 | 0 | 5,282 | 0.0 |
| 2022 | 0 | 6,387 | 0.0 |
| 2023 | 0 | 6,261 | 0.0 |
| 2024 | 0 | 6,249 | 0.0 |
| 2025 | 12,598 | 6,299 | 2.0 |
| 2026 | 11,836 | 5,918 | 2.0 |

### player_season_impact: Seasons with Data

| Season | Players |
|--------|--------:|
| 2024 | 9,861 |
| 2025 | 9,788 |
| 2026 | 10,008 |

### Gold Layer: Top 5 / Bottom 5 Teams (Season 2025, Sanity Check)

#### team_adjusted_efficiencies — adj_oe

**Top 5**:
- Duke: 115.25
- Florida: 114.25
- Alabama: 113.22
- Purdue: 113.08
- Gonzaga: 112.61

**Bottom 5**:
- Wagner: 89.37
- Cal State Fullerton: 88.73
- Tarleton State: 88.71
- Coppin State: 87.37
- Mississippi Valley State: 86.46

#### team_adjusted_efficiencies — adj_de

**Top 5**:
- IU Indianapolis: 113.90
- Le Moyne: 112.64
- Bellarmine: 111.48
- Canisius: 110.24
- UMBC: 110.11

**Bottom 5**:
- Maryland: 90.74
- Saint Mary's: 90.15
- Michigan State: 89.45
- Houston: 87.22
- St. John's: 87.02

#### team_adjusted_efficiencies — barthag

**Top 5**:
- Florida: 0.92
- Houston: 0.92
- Duke: 0.91
- Auburn: 0.91
- Michigan State: 0.89

**Bottom 5**:
- Chicago State: 0.16
- Arkansas-Pine Bluff: 0.15
- Sacramento State: 0.15
- West Georgia: 0.14
- Mississippi Valley State: 0.08

#### team_season_summary — wins

**Top 5**:
- Florida: 36
- Duke: 35
- Houston: 35
- Auburn: 32
- UC Irvine: 32

**Bottom 5**:
- Colby-Sawyer: 0
- Kean: 0
- Medgar Evers: 0
- Bowdoin: 0
- Washington Adventist: 0

#### team_season_summary — ppg

**Top 5**:
- Alabama: 90.68
- Colorado-Colorado Springs: 87.00
- Concordia-St. Paul: 85.00
- South Dakota: 84.47
- Gonzaga: 84.43

**Bottom 5**:
- Elms College: 37.00
- Kansas Christian: 36.00
- Penn State Scranton: 30.00
- Earlham College: 26.00
- Calvary: 19.00

#### team_power_rankings — composite_rank


### Gold Cross-Table Consistency: Teams per Season

| Season | team_adjusted_efficiencies | team_season_summary |
|--------|------:|------:|
| 2025 | 364 | 701 |
| 2026 | 365 | 727 |

> **Note**: `team_adjusted_efficiencies` counts are D1 teams only (364 expected). `team_season_summary` includes all divisions (~700+ teams), so the counts are not expected to match.

---

## Interpretation Notes & False Positives

### 1. Daily Rollup "Duplicates" — FALSE POSITIVE
The `fct_pbp_team_daily_rollup*` tables are marked FAIL for duplicates, but this is **expected behavior**. These tables store time-series snapshots with one row per `(teamid, asof_date)`. The PK in TABLE_SPECS is `(teamid,)` only, so every team with multiple snapshots appears as a "duplicate." The 1.3M total with 1.34M "duplicates" means ~1,279 unique team IDs across ~1,053 asof dates. **No action needed.**

### 2. Dimension Tables — NOT SEASON-PARTITIONED
All 5 dimension tables (`dim_teams`, `dim_conferences`, `dim_venues`, `dim_lines_providers`, `dim_play_types`) show FAIL/NO DATA because they are **not season-partitioned** in silver. They may be stored under `silver/dim_teams/asof=YYYY-MM-DD/` without a `season=` prefix, or they may not have been backfilled yet. Check storage pattern.

### 3. `fct_ratings_adjusted` — NO DATA in Silver
The API ratings_adjusted endpoint data is stored in silver but the audit found 0 rows. This may indicate data is stored under bronze only, or the endpoint hasn't been ingested recently. The gold `team_adjusted_efficiencies` table is computed from `fct_game_teams` (not from this silver table), so gold ratings are unaffected.

### 4. `fct_game_players` — 54% Duplicates (REAL ISSUE)
~50,135 duplicates across ~92,185 rows. Each season shows ~3,000 duplicates, meaning ~5,500 unique rows (one per game) but ~2,500 extra rows per season from overlapping pipeline runs. Needs deduplication.

### 5. `team_adjusted_efficiencies` Season 2015 — Low Count
Only 541 rows / 46 teams / 23 dates means only a few weeks of data available from `fct_game_teams` for 2015. The `no_garbage` variant has full 2015 data (48,963 rows) because it uses `fct_pbp_game_teams_flat_garbage_removed` which has 2015 PBP data.

### 6. `game_predictions_features` — Only 2025-2026
Gold prediction features have only been built for seasons 2025 and 2026. Earlier seasons show 0 rows. Ratio of 2.0x games confirms correct behavior for built seasons.

### 7. `team_season_summary` — 100% null on adj_ratings and recruiting
The `adj_off_rating`, `adj_def_rating`, `adj_net_rating` columns are 100% null because these come from `fct_ratings_adjusted` which has no data. `recruiting_*` columns are 100% null because `fct_recruiting_players` only covers season 2024.

### 8. `fct_pbp_game_teams_flat` — Missing Season 2024
Both flat and garbage_removed variants skip from 2023 to 2025. Season 2024 PBP data was not processed into flat team stats.

### 9. Season 2020 — NaN in Adjusted Efficiencies
Season 2020 avg adj_oe/adj_de show `nan`, likely due to the COVID-shortened season causing division-by-zero in some calculations.

---

## Recommended Actions

| Priority | Action | Tables Affected |
|----------|--------|-----------------|
| **HIGH** | Deduplicate `fct_game_players` — 54% duplicate rows | fct_game_players |
| **HIGH** | Rebuild `team_adjusted_efficiencies` season 2015 using `fct_game_teams` (only 46 teams found) | team_adjusted_efficiencies |
| **HIGH** | Fix season 2020 NaN values in `team_adjusted_efficiencies` | team_adjusted_efficiencies |
| **MED** | Backfill dimension tables to silver or verify storage pattern | dim_teams, dim_conferences, etc. |
| **MED** | Process season 2024 PBP flat stats | fct_pbp_game_teams_flat |
| **MED** | Build `game_predictions_features` for historical seasons (2013-2024) | game_predictions_features |
| **MED** | Build `team_power_rankings` (gold table exists but has no data) | team_power_rankings |
| **LOW** | Ingest `fct_ratings_adjusted` from API to populate `team_season_summary` adj columns | fct_ratings_adjusted, team_season_summary |
| **LOW** | Backfill recruiting data for more seasons | fct_recruiting_players |
| **LOW** | Investigate `fct_lines` season 2024 low count (5,311 vs ~13K expected) | fct_lines |
