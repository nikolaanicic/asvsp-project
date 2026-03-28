# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, input_file_name, regexp_extract
import flatten_utils

MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "statsbomb"
MONGO_COLLECTION = "cross_completion_rank"

spark = SparkSession.builder \
    .appName("StatsBomb Query 8 - Cross Completion Rank") \
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

window_rank = Window.partitionBy("season_id").orderBy(col("cross_completion").desc())
result = cross_completion.withColumn(
    "rank_in_season", rank().over(window_rank)
).select("team_name", "season_id", "cross_completion", "rank_in_season")

result.write.format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()