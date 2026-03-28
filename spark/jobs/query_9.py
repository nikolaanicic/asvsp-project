# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, when, input_file_name, regexp_extract
import flatten_utils


MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "statsbomb"
MONGO_COLLECTION = "cross_completion_rank"

spark = SparkSession.builder \
    .appName("StatsBomb Query 9 - Goals YoY Growth") \
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

goals_per_season = spark.sql("""
    SELECT 
        c.competition_name,
        m.season_id,
        COUNT(*) as goals
    FROM events e
    JOIN matches m ON e.match_id = m.match_id
    JOIN competitions c ON m.competition_id = c.competition_id AND m.season_id = c.season_id
    WHERE e.event_type_name = 'Shot' AND e.shot.outcome.name = 'Goal'
    GROUP BY c.competition_name, m.season_id
""")

window_yoy = Window.partitionBy("competition_name").orderBy("season_id")
result = goals_per_season.withColumn(
    "prev_season_goals", lag("goals", 1).over(window_yoy)
).withColumn(
    "yoy_growth", when(col("prev_season_goals").isNotNull(),
                       (col("goals") - col("prev_season_goals")) / col("prev_season_goals"))
).select("competition_name", "season_id", "goals", "prev_season_goals", "yoy_growth")

result.write.format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()