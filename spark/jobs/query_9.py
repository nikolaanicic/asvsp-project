# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, when
import flatten_utils

spark = SparkSession.builder \
    .appName("StatsBomb Query 9 - Goals YoY Growth") \
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

result.write.format("mongo") \
    .mode("overwrite") \
    .option("collection", "goals_yoy_growth") \
    .save()

spark.stop()