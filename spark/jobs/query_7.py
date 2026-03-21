# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lit
import flatten_utils

spark = SparkSession.builder \
    .appName("StatsBomb Query 7 - Possession Duration Difference") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27017/statsbomb.results") \
    .getOrCreate()

events_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/events/*.json")
events_flat = flatten_utils.base_events(events_df)
events_flat.createOrReplaceTempView("events")

possession_team = spark.sql("""
    SELECT 
        e.team_name,
        AVG(e.duration) as team_avg_duration
    FROM events e
    WHERE e.duration IS NOT NULL
    GROUP BY e.team_name
""")

league_avg = possession_team.agg(avg("team_avg_duration").alias("league_avg")).collect()[0][0]

result = possession_team.withColumn(
    "league_avg_duration", lit(league_avg)
).withColumn(
    "diff_from_league_avg", col("team_avg_duration") - col("league_avg_duration")
).select("team_name", "team_avg_duration", "league_avg_duration", "diff_from_league_avg")

result.write.format("mongo") \
    .mode("overwrite") \
    .option("collection", "possession_duration_diff") \
    .save()

spark.stop()