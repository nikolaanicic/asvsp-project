# Big Data Football Analytics

**Course:** Architectures and Systems for Big Data  
**Student:** Nikola Anicic
**Project Domain:** Football (soccer) match and event analysis.

## 📌 Project Overview

This project implements a complete big data pipeline for analysing football data. It combines:

- **Batch dataset:** [StatsBomb Open Data](https://github.com/statsbomb/open-data) – detailed event data for >300 MB of matches.
- **Streaming dataset:** Live football scores from [api.football-data.org](https://www.football-data.org/) (free tier, polled every minute).

The data lake is organised into three zones on HDFS:
- **Raw:** Original JSON files as ingested.
- **Transformed:** Flattened and cleaned Parquet files.
- **Curated:** Results of analytical queries stored in MongoDB for fast access.

Batch processing (10 complex queries using window functions) and stream processing (5 transformations with joins and windowing) are implemented in Apache Spark. Apache Airflow orchestrates the batch jobs (daily) and the streaming pipeline (continuous). Results are visualised in Metabase.

## 🏛️ Architecture

**Components:**
- **HDFS** – Hadoop Distributed File System (data lake).
- **Apache Spark** – batch and stream processing.
- **Apache Kafka** – ingestion of live scores.
- **MongoDB** – storage of curated results.
- **Apache Airflow** – workflow orchestration.
- **Hue** – exploration of HDFS/Hive data.
- **Metabase** – dashboards and visualisation.
- **Docker** – containerisation of all services.

## 📊 Batch Analysis – 10 Queries (with window functions)

| # | Question | Window function used |
|---|----------|----------------------|
| 1 | Top 10 goalscorers of all time | ROW_NUMBER() |
| 2 | Pass completion % per team per match (with running average) | AVG() OVER (PARTITION BY team ORDER BY match_date) |
| 3 | Average xG per shot by player, and rank within competition | DENSE_RANK() OVER (PARTITION BY competition ORDER BY avg_xg DESC) |
| 4 | Players with most dribbles – show previous season's count as comparison | LAG() OVER (PARTITION BY player ORDER BY season) |
| 5 | Most frequent assist combinations (passer → scorer) – count over last 10 matches | SUM() OVER (PARTITION BY passer, scorer ORDER BY match_date ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) |
| 6 | Shots and conversion rate by position – percentile rank within league | PERCENT_RANK() OVER (PARTITION BY league ORDER BY conversion_rate) |
| 7 | Counter‑press events leading to turnovers – rolling 5‑match average | AVG() OVER (PARTITION BY team ORDER BY match_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) |
| 8 | Average possession duration per team – difference from league average | AVG(possession_duration) OVER () – to compute league avg then subtract |
| 9 | Cross completion rates by team – rank by season | RANK() OVER (PARTITION BY season ORDER BY cross_completion DESC) |
|10 | Goals per season per competition – year‑over‑year growth | LAG(goals) OVER (PARTITION BY competition ORDER BY season) |

*All queries are implemented in `spark/jobs/batch_processor.py`.*

## ⚡ Stream Processing – 5 Transformations

1. **Live score enrichment:** Join live score stream with batch team data (from HDFS) to add team country, stadium, etc.
2. **Rolling average of shots per team:** Tumbling window of 10 minutes, average shots.
3. **Detect high‑scoring matches:** Alert when total goals in a match exceed 4 within a 5‑minute sliding window.
4. **Team form indicator:** 30‑minute sliding window counting goals scored/conceded.
5. **Join with batch player data:** For each goal event, attach player position from the lineups (batch).

*Implementation in `spark/jobs/stream_processor.py`.*

## 🚀 How to Run

### Prerequisites
- Docker and Docker Compose installed.
- [API key](https://www.football-data.org/client/register) for football‑data.org (free).

### Steps

1. Clone this repository.
2. Place StatsBomb JSON files into HDFS:
   ```bash
   docker cp <path_to_statsbomb> namenode:/tmp/
   docker exec namenode hdfs dfs -mkdir -p /raw/statsbomb
   docker exec namenode hdfs dfs -put /tmp/statsbomb/* /raw/statsbomb/
