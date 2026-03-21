# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import flatten_utils

spark = SparkSession.builder \
    .appName("StatsBomb Query 8 - Cross Completion Rank") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27017/statsbomb.results") \
    .getOrCreate()

events_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/events/*.json")
matches_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/matches/*/*.json")

events_flat = flatten_utils.base_events(events_df)
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

result.write.format("mongo") \
    .mode("overwrite") \
    .option("collection", "cross_completion_rank") \
    .save()

spark.stop()