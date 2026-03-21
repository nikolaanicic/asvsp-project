# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import flatten_utils

spark = SparkSession.builder \
    .appName("StatsBomb Query 2 - Average xG per Player") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27017/statsbomb.results") \
    .getOrCreate()

events_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/events/*.json")
matches_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/matches/*/*.json")
competitions_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/competitions.json")

events_flat = flatten_utils.base_events(events_df)
matches_flat = flatten_utils.flatten_matches(matches_df)
competitions_flat = flatten_utils.flatten_competitions(competitions_df)

events_flat.createOrReplaceTempView("events")
matches_flat.createOrReplaceTempView("matches")
competitions_flat.createOrReplaceTempView("competitions")

result = spark.sql("""
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

result.write.format("mongo") \
    .mode("overwrite") \
    .option("collection", "avg_xg_per_player") \
    .save()

spark.stop()