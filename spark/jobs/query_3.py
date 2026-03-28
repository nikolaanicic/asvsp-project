# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import input_file_name, regexp_extract, lag
import flatten_utils

MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "statsbomb"
MONGO_COLLECTION = "dribbles_with_lag"

spark = SparkSession.builder \
    .appName("StatsBomb Query 3 - Dribbles with LAG") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .config("spark.mongodb.collection", MONGO_COLLECTION) \
    .getOrCreate()

events_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/events/*.json", multiLine=True)
events_df = events_df.withColumn("match_id", regexp_extract(input_file_name(), r"(\d+)\.json$", 1))
matches_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/matches/*/*.json", multiLine=True)

events_flat = flatten_utils.events_with_match_id(events_df)
matches_flat = flatten_utils.flatten_matches(matches_df)

events_flat.createOrReplaceTempView("events")
matches_flat.createOrReplaceTempView("matches")

dribbles_per_season = spark.sql("""
    SELECT e.player_name, m.season_id, COUNT(*) as dribbles
    FROM events e
    JOIN matches m ON e.match_id = m.match_id
    WHERE e.event_type_name = 'Dribble' AND e.dribble.outcome.name = 'Complete'
    GROUP BY e.player_name, m.season_id
""")

window_lag = Window.partitionBy("player_name").orderBy("season_id")
result = dribbles_per_season.withColumn(
    "prev_season_dribbles", lag("dribbles", 1).over(window_lag)
).select("player_name", "season_id", "dribbles", "prev_season_dribbles")

result.write.format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()