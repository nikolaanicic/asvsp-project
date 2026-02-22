from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'statsbomb_batch',
    default_args=default_args,
    description='Daily StatsBomb batch processing',
    schedule_interval='0 2 * * *',
    catchup=False,
) as dag:

    batch_job = SparkSubmitOperator(
        task_id='run_batch_processor',
        application='/opt/spark/jobs/batch_processor.py',
        name='StatsBomb Batch',
        conn_id='spark_default',
        verbose=True,
        conf={
            'spark.mongodb.output.uri': 'mongodb://mongo:27017/statsbomb.results'
        },
        jars='/opt/spark/jars/mongo-spark-connector_2.12-10.1.0.jar'
    )

    batch_job