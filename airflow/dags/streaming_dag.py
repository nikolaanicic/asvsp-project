from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    'statsbomb_streaming',
    default_args=default_args,
    description='Continuous streaming job for live scores',
    schedule_interval=None,  # triggered manually or runs continuously
    catchup=False,
) as dag:

    stream_job = SparkSubmitOperator(
        task_id='run_stream_processor',
        application='/opt/spark/jobs/stream_processor.py',
        name='StatsBomb Stream',
        conn_id='spark_default',
        verbose=True,
        conf={
            'spark.mongodb.output.uri': 'mongodb://mongo:27017/statsbomb.stream_results'
        },
        jars='/opt/spark/jars/mongo-spark-connector_2.12-10.1.0.jar,/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.3.0.jar',
        driver_class_path='/opt/spark/jars/mongo-spark-connector_2.12-10.1.0.jar'
    )

    stream_job