# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import input_file_name, regexp_extract, count

import flatten_utils

MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "statsbomb"
MONGO_COLLECTION = "assist_combinations"


spark = SparkSession.builder \
    .appName("StatsBomb Query 4 - Assist Combinations") \
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

assists = spark.sql("""
    SELECT 
        e1.player_name as passer,
        e2.player_name as scorer,
        m.match_date,
        e1.match_id
    FROM events e1
    JOIN events e2 ON e1.pass.assisted_shot_id = e2.event_id
    JOIN matches m ON e1.match_id = m.match_id
    WHERE e1.event_type_name = 'Pass' AND e1.pass.goal_assist = true
      AND e2.event_type_name = 'Shot' AND e2.shot.outcome.name = 'Goal'
""")

window_last10 = Window.partitionBy("passer", "scorer") \
                      .orderBy("match_date") \
                      .rowsBetween(-9, 0)
result = assists.withColumn(
    "assists_last_10", count("*").over(window_last10)
).select("passer", "scorer", "match_date", "assists_last_10")

result.write.format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()