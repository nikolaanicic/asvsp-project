# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lit, input_file_name, regexp_extract
import flatten_utils


MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "statsbomb"
MONGO_COLLECTION = "possession_duration_diff"

spark = SparkSession.builder \
    .appName("StatsBomb Query 7 - Possession Duration Difference") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .config("spark.mongodb.collection", MONGO_COLLECTION) \
    .getOrCreate()

events_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/events/*.json", multiLine=True)
events_df = events_df.withColumn("match_id", regexp_extract(input_file_name(), r"(\d+)\.json$", 1))

events_flat = flatten_utils.events_with_match_id(events_df)
matches_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/matches/*/*.json", multiLine=True)
matches_flat = flatten_utils.flatten_matches(matches_df)

events_with_matches = events_flat.join(
    matches_flat,
    on="match_id",
    how="left"
)
events_with_matches.createOrReplaceTempView("events")

# Compute team averages per competition/season
possession_team = spark.sql("""
    SELECT 
        e.competition_name,
        e.season_name,
        e.team_name,
        AVG(e.duration) AS team_avg_duration
    FROM events e
    WHERE e.duration IS NOT NULL
    GROUP BY e.competition_name, e.season_name, e.team_name
""")

# Compute league average per competition/season
league_avg_df = possession_team.groupBy(
    "competition_name", "season_name"
).agg(
    avg("team_avg_duration").alias("league_avg_duration")
)

# Join and compute difference
result = possession_team.join(
    league_avg_df,
    on=["competition_name", "season_name"]
).withColumn(
    "diff_from_league_avg", col("team_avg_duration") - col("league_avg_duration")
).select(
    "competition_name", "season_name", "team_name",
    "team_avg_duration", "league_avg_duration", "diff_from_league_avg"
)
result.repartition(10).write.format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()