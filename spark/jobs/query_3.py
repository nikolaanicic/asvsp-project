# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col
import flatten_utils

spark = SparkSession.builder \
    .appName("StatsBomb Query 3 - Dribbles with LAG") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27017/statsbomb.results") \
    .getOrCreate()

events_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/events/*.json")
matches_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/matches/*/*.json")

events_flat = flatten_utils.base_events(events_df)
matches_flat = flatten_utils.flatten_matches(matches_df)

events_flat.createOrReplaceTempView("events")
matches_flat.createOrReplaceTempView("matches")

dribbles_per_season = spark.sql("""
    SELECT e.player_name, m.season_id, COUNT(*) as dribbles
    FROM events e
    JOIN matches m ON e.match_id = m.match_id
    WHERE e.event_type_name = 'Dribble' AND e.dribble.outcome.name = 'Complete'
    GROUP BY e.player_name, m.season_id
""")

window_lag = Window.partitionBy("player_name").orderBy("season_id")
result = dribbles_per_season.withColumn(
    "prev_season_dribbles", lag("dribbles", 1).over(window_lag)
).select("player_name", "season_id", "dribbles", "prev_season_dribbles")

result.write.format("mongo") \
    .mode("overwrite") \
    .option("collection", "dribbles_with_lag") \
    .save()

spark.stop()