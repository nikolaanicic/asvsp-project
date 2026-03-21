# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg
import flatten_utils

spark = SparkSession.builder \
    .appName("StatsBomb Query 1 - Pass Completion Running Average") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27017/statsbomb.results") \
    .getOrCreate()

# Read data
events_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/events/*.json")
matches_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/matches/*/*.json")

events_flat = flatten_utils.base_events(events_df)
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

result.write.format("mongo") \
    .mode("overwrite") \
    .option("collection", "pass_completion_running_avg") \
    .save()

spark.stop()