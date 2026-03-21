# -*- coding: utf-8 -*-
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import flatten_utils


MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "statsbomb"
MONGO_COLLECTION = "top_goalscorers"

spark = SparkSession.builder \
    .appName("StatsBomb Query 0 - Top Goalscorers") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .config("spark.mongodb.collection", MONGO_COLLECTION) \
    .getOrCreate()

# Read and flatten data
events_df = spark.read.json("hdfs://namenode:9000/raw/statsbomb/events/*.json", multiLine=True)
events_flat = flatten_utils.base_events(events_df)
events_flat.createOrReplaceTempView("events")

# Query
result = spark.sql("""
    SELECT player_name, goals, 
           ROW_NUMBER() OVER (ORDER BY goals DESC) as rank
    FROM (
        SELECT player_name, COUNT(*) as goals
        FROM events
        WHERE event_type_name = 'Shot' AND shot.outcome.name = 'Goal'
        GROUP BY player_name
    ) t
    ORDER BY rank
    LIMIT 10
""")

result.write.format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

spark.stop()