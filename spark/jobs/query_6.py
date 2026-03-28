# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, input_file_name, regexp_extract
import flatten_utils


MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "statsbomb"
MONGO_COLLECTION = "counterpress_rolling_avg"


spark = SparkSession.builder \
    .appName("StatsBomb Query 6 - Counterpress Rolling Average") \
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

result.write.format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()