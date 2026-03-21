# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, percent_rank
import flatten_utils

spark = SparkSession.builder \
    .appName("StatsBomb Query 5 - Position Conversion Percentile") \
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

window_percent = Window.partitionBy("league").orderBy(col("conversion_rate").desc())
result = conversion.withColumn(
    "percentile_rank", percent_rank().over(window_percent)
).select("league", "position_name", "shots", "goals", "conversion_rate", "percentile_rank")

result.write.format("mongo") \
    .mode("overwrite") \
    .option("collection", "position_conversion_percentile") \
    .save()

spark.stop()