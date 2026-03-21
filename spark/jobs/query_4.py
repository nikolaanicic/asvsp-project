# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import count, col
import flatten_utils

spark = SparkSession.builder \
    .appName("StatsBomb Query 4 - Assist Combinations") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27017/statsbomb.results") \
    .getOrCreate()

events_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/events/*.json")
matches_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/matches/*/*.json")

events_flat = flatten_utils.base_events(events_df)
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

result.write.format("mongo") \
    .mode("overwrite") \
    .option("collection", "assist_combinations") \
    .save()

spark.stop()