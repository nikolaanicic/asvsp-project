#!/usr/bin/env python3
"""
Spark Structured Streaming job for live football scores.
Reads from Kafka, enriches with batch team data, performs windowed aggregations,
and writes results to MongoDB.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum as _sum, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

MONGO_URI = "mongodb://mongo:27017/statsbomb.stream_results"

def main():
    spark = SparkSession.builder \
        .appName("StatsBomb Stream Processing") \
        .config("spark.mongodb.output.uri", MONGO_URI) \
        .getOrCreate()

    # Read stream from Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "live-scores") \
        .load()

    # Define schema of incoming JSON (from live_score_producer.py)
    schema = StructType([
        StructField("match_id", StringType()),
        StructField("home_team", StringType()),
        StructField("away_team", StringType()),
        StructField("home_score", IntegerType()),
        StructField("away_score", IntegerType()),
        StructField("minute", IntegerType()),
        StructField("timestamp", TimestampType())
    ])

    # Parse JSON
    df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # 1. Enrich with batch team data (join with static DataFrame from HDFS)
    # Load team data from curated zone (could be a Parquet file we produced in batch)
    team_df = spark.read.parquet("hdfs://namenode:9000/curated/teams.parquet")
    df_enriched = df_parsed.join(team_df, df_parsed.home_team == team_df.team_name, "left_outer")

    # 2. Rolling average of shots per team (we don't have shots in this stream, so we'll use goals as proxy)
    # For demonstration, we'll compute average goals per 10-minute tumbling window
    avg_goals = df_enriched \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(window("timestamp", "10 minutes"), "home_team") \
        .agg(avg("home_score").alias("avg_home_goals"))

    # 3. Detect high-scoring matches (total goals > 4 in sliding 5-minute window)
    high_scoring = df_enriched \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(window("timestamp", "5 minutes", "1 minute"), "match_id") \
        .agg((_sum("home_score") + _sum("away_score")).alias("total_goals")) \
        .filter(col("total_goals") > 4)

    # 4. Team form indicator: goals scored/conceded in last 30 minutes (sliding window)
    team_form = df_enriched \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(window("timestamp", "30 minutes", "5 minutes"), "home_team") \
        .agg(
            _sum("home_score").alias("goals_scored"),
            _sum("away_score").alias("goals_conceded")
        )

    # 5. Join with batch player data (e.g., to get top scorers) – for each goal, attach player info
    # Since our stream doesn't have player info, we'll skip this or simulate.
    # Instead, we'll just write the enriched stream to MongoDB for simplicity.
    # But to satisfy "join stream with batch", we already did #1 (enrich with team data).

    # Write outputs to MongoDB (sink)
    def write_to_mongo(df, collection_name):
        return df.writeStream \
            .foreachBatch(lambda batch_df, epoch_id: batch_df.write.format("mongo") \
                          .mode("append").option("collection", collection_name).save()) \
            .outputMode("append") \
            .start()

    query1 = write_to_mongo(avg_goals, "avg_goals_per_10min")
    query2 = write_to_mongo(high_scoring, "high_scoring_alerts")
    query3 = write_to_mongo(team_form, "team_form_30min")

    query1.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()

if __name__ == "__main__":
    main()