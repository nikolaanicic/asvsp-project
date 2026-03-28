# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
import flatten_utils


MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "statsbomb"
MONGO_COLLECTION = "avg_xg_per_player"

spark = SparkSession.builder \
    .appName("StatsBomb Query 2 - Average xG per Player") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .config("spark.mongodb.collection", MONGO_COLLECTION) \
    .getOrCreate()

events_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/events/*.json", multiLine=True)
events_df = events_df.withColumn("match_id", regexp_extract(input_file_name(), r"(\d+)\.json$", 1))
matches_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/matches/*/*.json", multiLine=True)
competitions_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/competitions.json", multiLine=True)

events_flat = flatten_utils.events_with_match_id(events_df)
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

result.write.format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()