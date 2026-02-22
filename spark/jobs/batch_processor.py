#!/usr/bin/env python3
"""
StatsBomb Batch Processor – 10 Queries with Window Functions
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, percent_rank, lag, avg, sum as _sum, count, when
import flatten_utils

MONGO_URI = "mongodb://mongo:27017/statsbomb.results"

def main():
    spark = SparkSession.builder \
        .appName("StatsBomb Batch with Window Functions") \
        .config("spark.mongodb.output.uri", MONGO_URI) \
        .getOrCreate()

    # ------------------------------------------------------------------
    # 1. Read data from HDFS (adjust paths if needed)
    # ------------------------------------------------------------------
    competitions_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/competitions.json")
    matches_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/matches/*/*.json")
    lineups_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/lineups/*.json")
    events_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/events/*.json")

    # ------------------------------------------------------------------
    # 2. Flatten DataFrames using utility functions
    # ------------------------------------------------------------------
    comp_flat = flatten_utils.flatten_competitions(competitions_df)
    match_flat = flatten_utils.flatten_matches(matches_df)
    lineup_flat = flatten_utils.explode_lineups(lineups_df)
    event_base = flatten_utils.base_events(events_df)

    # ------------------------------------------------------------------
    # 3. Create temporary views for SQL queries
    # ------------------------------------------------------------------
    comp_flat.createOrReplaceTempView("competitions")
    match_flat.createOrReplaceTempView("matches")
    lineup_flat.createOrReplaceTempView("lineups")
    event_base.createOrReplaceTempView("events")

    # ------------------------------------------------------------------
    # 4. Execute the 10 batch queries
    # ------------------------------------------------------------------

    # ----- Query 1: Top 10 goalscorers of all time (ROW_NUMBER) -----
    q1 = spark.sql("""
        SELECT player_name, goals, 
               ROW_NUMBER() OVER (ORDER BY goals DESC) as rank
        FROM (
            SELECT player_name, COUNT(*) as goals
            FROM events
            WHERE event_type_name = 'Shot' AND shot.outcome.name = 'Goal'
            GROUP BY player_name
        ) t
        ORDER BY rank
        LIMIT 10
    """)
    q1.write.format("mongo").mode("overwrite") \
      .option("collection", "top_goalscorers").save()

    # ----- Query 2: Pass completion % with running average over last 5 matches -----
    # First compute per‑match completion
    pass_per_match = spark.sql("""
        SELECT match_id, team_name, 
               COUNT(*) as total_passes,
               SUM(CASE WHEN pass.outcome.name IS NULL THEN 1 ELSE 0 END) as successful_passes,
               SUM(CASE WHEN pass.outcome.name IS NULL THEN 1 ELSE 0 END) / COUNT(*) as completion_rate
        FROM events
        WHERE event_type_name = 'Pass'
        GROUP BY match_id, team_name
    """)
    # Add match date
    pass_with_date = pass_per_match.join(
        match_flat.select("match_id", "match_date"), on="match_id"
    )
    # Define window: last 5 matches (including current) ordered by date
    window_spec = Window.partitionBy("team_name").orderBy("match_date") \
                        .rowsBetween(-4, 0)
    q2 = pass_with_date.withColumn(
        "running_avg_completion", avg("completion_rate").over(window_spec)
    ).select("team_name", "match_date", "completion_rate", "running_avg_completion")
    q2.write.format("mongo").mode("overwrite") \
      .option("collection", "pass_completion_running_avg").save()

    # ----- Query 3: Average xG per player with DENSE_RANK within competition -----
    q3 = spark.sql("""
        SELECT player_name, competition_name, avg_xg,
               DENSE_RANK() OVER (PARTITION BY competition_name ORDER BY avg_xg DESC) as rank_in_comp
        FROM (
            SELECT e.player_name, c.competition_name, AVG(e.shot.statsbomb_xg) as avg_xg
            FROM events e
            JOIN matches m ON e.match_id = m.match_id
            JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
            WHERE e.event_type_name = 'Shot' AND e.shot.statsbomb_xg IS NOT NULL
            GROUP BY e.player_name, c.competition_name
        ) t
    """)
    q3.write.format("mongo").mode("overwrite") \
      .option("collection", "avg_xg_per_player").save()

    # ----- Query 4: Players with most dribbles – show previous season's count (LAG) -----
    # First compute dribbles per player per season
    dribbles_per_season = spark.sql("""
        SELECT e.player_name, m.season_id, COUNT(*) as dribbles
        FROM events e
        JOIN matches m ON e.match_id = m.match_id
        WHERE e.event_type_name = 'Dribble' AND e.dribble.outcome.name = 'Complete'
        GROUP BY e.player_name, m.season_id
    """)
    # Add previous season count using LAG
    window_lag = Window.partitionBy("player_name").orderBy("season_id")
    q4 = dribbles_per_season.withColumn(
        "prev_season_dribbles", lag("dribbles", 1).over(window_lag)
    ).select("player_name", "season_id", "dribbles", "prev_season_dribbles")
    q4.write.format("mongo").mode("overwrite") \
      .option("collection", "dribbles_with_lag").save()

    # ----- Query 5: Most frequent assist combinations (passer → scorer) – count over last 10 matches -----
    # First identify assist events (goal_assist = true) and join with the corresponding goal
    assists = spark.sql("""
        SELECT 
            e1.player_name as passer,
            e2.player_name as scorer,
            m.match_date,
            e1.match_id
        FROM events e1
        JOIN events e2 ON e1.pass.assisted_shot_id = e2.event_id
        JOIN matches m ON e1.match_id = m.match_id
        WHERE e1.event_type_name = 'Pass' AND e1.pass.goal_assist = true
          AND e2.event_type_name = 'Shot' AND e2.shot.outcome.name = 'Goal'
    """)
    # Define window over last 10 matches (ordered by date) for each passer‑scorer pair
    window_last10 = Window.partitionBy("passer", "scorer") \
                          .orderBy("match_date") \
                          .rowsBetween(-9, 0)
    q5 = assists.withColumn(
        "assists_last_10", count("*").over(window_last10)
    ).select("passer", "scorer", "match_date", "assists_last_10")
    q5.write.format("mongo").mode("overwrite") \
      .option("collection", "assist_combinations").save()

    # ----- Query 6: Shots and conversion rate by position – percentile rank within league -----
    # We need league information (competition). Compute conversion rate per position per league.
    conversion = spark.sql("""
        SELECT 
            c.competition_name as league,
            e.position_name,
            COUNT(*) as shots,
            SUM(CASE WHEN e.shot.outcome.name = 'Goal' THEN 1 ELSE 0 END) as goals,
            SUM(CASE WHEN e.shot.outcome.name = 'Goal' THEN 1 ELSE 0 END) / COUNT(*) as conversion_rate
        FROM events e
        JOIN matches m ON e.match_id = m.match_id
        JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
        WHERE e.event_type_name = 'Shot' AND e.position_name IS NOT NULL
        GROUP BY c.competition_name, e.position_name
    """)
    # Percent rank within each league
    window_percent = Window.partitionBy("league").orderBy(col("conversion_rate").desc())
    q6 = conversion.withColumn(
        "percentile_rank", percent_rank().over(window_percent)
    ).select("league", "position_name", "shots", "goals", "conversion_rate", "percentile_rank")
    q6.write.format("mongo").mode("overwrite") \
      .option("collection", "position_conversion_percentile").save()

    # ----- Query 7: Counter‑press events leading to turnovers – rolling 5‑match average -----
    # First, count counterpress events per team per match
    counterpress_per_match = spark.sql("""
        SELECT 
            m.match_id,
            m.match_date,
            e.team_name,
            COUNT(*) as counterpress_events
        FROM events e
        JOIN matches m ON e.match_id = m.match_id
        WHERE e.counterpress = true
        GROUP BY m.match_id, m.match_date, e.team_name
    """)
    # Rolling average over last 5 matches
    window_roll = Window.partitionBy("team_name").orderBy("match_date") \
                        .rowsBetween(-4, 0)
    q7 = counterpress_per_match.withColumn(
        "rolling_avg_counterpress", avg("counterpress_events").over(window_roll)
    ).select("team_name", "match_date", "counterpress_events", "rolling_avg_counterpress")
    q7.write.format("mongo").mode("overwrite") \
      .option("collection", "counterpress_rolling_avg").save()

    # ----- Query 8: Average possession duration per team – difference from league average -----
    # Compute average possession duration per team (overall) and also league average
    # Possession duration is sum of event durations during a possession. Need to compute per possession first.
    # Simplified: average of event durations for each team (not perfect, but acceptable for demo)
    possession_team = spark.sql("""
        SELECT 
            e.team_name,
            AVG(e.duration) as team_avg_duration
        FROM events e
        WHERE e.duration IS NOT NULL
        GROUP BY e.team_name
    """)
    # Compute overall league average (across all teams)
    league_avg = possession_team.agg(avg("team_avg_duration").alias("league_avg")).collect()[0][0]
    # Add column with difference
    q8 = possession_team.withColumn(
        "league_avg_duration", lit(league_avg)
    ).withColumn(
        "diff_from_league_avg", col("team_avg_duration") - col("league_avg_duration")
    ).select("team_name", "team_avg_duration", "league_avg_duration", "diff_from_league_avg")
    q8.write.format("mongo").mode("overwrite") \
      .option("collection", "possession_duration_diff").save()

    # ----- Query 9: Cross completion rates by team – rank by season (RANK) -----
    # Compute cross completion per team per season
    cross_completion = spark.sql("""
        SELECT 
            e.team_name,
            m.season_id,
            COUNT(*) as crosses,
            SUM(CASE WHEN e.pass.outcome.name IS NULL THEN 1 ELSE 0 END) as successful_crosses,
            SUM(CASE WHEN e.pass.outcome.name IS NULL THEN 1 ELSE 0 END) / COUNT(*) as cross_completion
        FROM events e
        JOIN matches m ON e.match_id = m.match_id
        WHERE e.event_type_name = 'Pass' AND e.pass.cross = true
        GROUP BY e.team_name, m.season_id
    """)
    # Rank by cross_completion within each season
    window_rank = Window.partitionBy("season_id").orderBy(col("cross_completion").desc())
    q9 = cross_completion.withColumn(
        "rank_in_season", rank().over(window_rank)
    ).select("team_name", "season_id", "cross_completion", "rank_in_season")
    q9.write.format("mongo").mode("overwrite") \
      .option("collection", "cross_completion_rank").save()

    # ----- Query 10: Goals per season per competition – year‑over‑year growth (LAG) -----
    # Count goals per competition per season
    goals_per_season = spark.sql("""
        SELECT 
            c.competition_name,
            m.season_id,
            COUNT(*) as goals
        FROM events e
        JOIN matches m ON e.match_id = m.match_id
        JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
        WHERE e.event_type_name = 'Shot' AND e.shot.outcome.name = 'Goal'
        GROUP BY c.competition_name, m.season_id
    """)
    # LAG to get previous season's goals
    window_yoy = Window.partitionBy("competition_name").orderBy("season_id")
    q10 = goals_per_season.withColumn(
        "prev_season_goals", lag("goals", 1).over(window_yoy)
    ).withColumn(
        "yoy_growth", when(col("prev_season_goals").isNotNull(),
                           (col("goals") - col("prev_season_goals")) / col("prev_season_goals"))
    ).select("competition_name", "season_id", "goals", "prev_season_goals", "yoy_growth")
    q10.write.format("mongo").mode("overwrite") \
      .option("collection", "goals_yoy_growth").save()

    spark.stop()

if __name__ == "__main__":
    main()