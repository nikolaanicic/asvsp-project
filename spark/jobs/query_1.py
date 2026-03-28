# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, input_file_name, regexp_extract
import flatten_utils

MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "statsbomb"
MONGO_COLLECTION = "pass_completion_running_avg"

spark = SparkSession.builder \
    .appName("StatsBomb Query 1 - Pass Completion Running Average") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .config("spark.mongodb.collection", MONGO_COLLECTION) \
    .getOrCreate()

# Read data
events_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/events/*.json", multiLine=True)
events_df = events_df.withColumn("match_id", regexp_extract(input_file_name(), r"(\d+)\.json$", 1))

print("INPUT_FILE_NAME:", input_file_name())

matches_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/matches/*/*.json", multiLine=True)

events_flat = flatten_utils.events_with_match_id(events_df)
matches_flat = flatten_utils.flatten_matches(matches_df)

events_flat.createOrReplaceTempView("events")
matches_flat.createOrReplaceTempView("matches")

# Compute per‑match completion
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
    matches_flat.select("match_id", "match_date"), on="match_id"
)

# Window for running average over last 5 matches
window_spec = Window.partitionBy("team_name").orderBy("match_date").rowsBetween(-4, 0)
result = pass_with_date.withColumn(
    "running_avg_completion", avg("completion_rate").over(window_spec)
).select("team_name", "match_date", "completion_rate", "running_avg_completion")

result.write.format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()