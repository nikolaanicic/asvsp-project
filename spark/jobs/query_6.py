# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col
import flatten_utils

spark = SparkSession.builder \
    .appName("StatsBomb Query 6 - Counterpress Rolling Average") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27017/statsbomb.results") \
    .getOrCreate()

events_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/events/*.json")
matches_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/matches/*/*.json")

events_flat = flatten_utils.base_events(events_df)
matches_flat = flatten_utils.flatten_matches(matches_df)

events_flat.createOrReplaceTempView("events")
matches_flat.createOrReplaceTempView("matches")

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

window_roll = Window.partitionBy("team_name").orderBy("match_date").rowsBetween(-4, 0)
result = counterpress_per_match.withColumn(
    "rolling_avg_counterpress", avg("counterpress_events").over(window_roll)
).select("team_name", "match_date", "counterpress_events", "rolling_avg_counterpress")

result.write.format("mongo") \
    .mode("overwrite") \
    .option("collection", "counterpress_rolling_avg") \
    .save()

spark.stop()