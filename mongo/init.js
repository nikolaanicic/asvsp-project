// Switch to statsbomb database
db = db.getSiblingDB('statsbomb');

// ========== Batch query result collections ==========
db.createCollection('top_goalscorers');
db.createCollection('pass_completion_running_avg');
db.createCollection('avg_xg_per_player');
db.createCollection('dribbles_with_lag');
db.createCollection('assist_combinations');
db.createCollection('position_conversion_percentile');
db.createCollection('counterpress_rolling_avg');
db.createCollection('possession_duration_diff');
db.createCollection('cross_completion_rank');
db.createCollection('goals_yoy_growth');

// ========== Stream query result collections ==========
db.createCollection('avg_goals_per_10min');
db.createCollection('high_scoring_alerts');
db.createCollection('team_form_30min');

// ========== Indexes for batch collections ==========
// top_goalscorers: rank is unique
db.top_goalscorers.createIndex({ rank: 1 });

// pass_completion_running_avg: team + date for quick filtering
db.pass_completion_running_avg.createIndex({ team_name: 1, match_date: -1 });

// avg_xg_per_player: player name and competition
db.avg_xg_per_player.createIndex({ player_name: 1, competition_name: 1 });

// dribbles_with_lag: player + season
db.dribbles_with_lag.createIndex({ player_name: 1, season_id: 1 });

// assist_combinations: passer, scorer, date
db.assist_combinations.createIndex({ passer: 1, scorer: 1, match_date: -1 });

// position_conversion_percentile: league, percentile
db.position_conversion_percentile.createIndex({ league: 1, percentile_rank: 1 });

// counterpress_rolling_avg: team, date
db.counterpress_rolling_avg.createIndex({ team_name: 1, match_date: -1 });

// possession_duration_diff: diff (for sorting)
db.possession_duration_diff.createIndex({ diff_from_league_avg: -1 });

// cross_completion_rank: season, rank
db.cross_completion_rank.createIndex({ season_id: 1, rank_in_season: 1 });

// goals_yoy_growth: competition, season
db.goals_yoy_growth.createIndex({ competition_name: 1, season_id: 1 });

// ========== Indexes for stream collections ==========
db.avg_goals_per_10min.createIndex({ window: 1, home_team: 1 });
db.high_scoring_alerts.createIndex({ window: 1, match_id: 1 });
db.team_form_30min.createIndex({ window: 1, home_team: 1 });

print('All collections and indexes created successfully.');