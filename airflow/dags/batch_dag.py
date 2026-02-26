from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os


HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
MONGO_URI = "mongodb://mongodb:27017"
pyspark_app_home = "/home/scripts"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


tasks = {}
task_ids = []
scripts = [f"query_{i}.py" for i in range(11)]

with DAG(
    'statsbomb_batch',
    default_args=default_args,
    description='Daily StatsBomb batch processing',
    schedule_interval='0 2 * * *',
    catchup=False,
) as dag:

    for script in scripts:
        task_id = script.replace(".py","")
        task_ids.append(task_id)
        tasks[task_id] = BashOperator(
            task_id=task_id,
            bash_command=(
                "spark-submit "
                "--master spark://spark-master:7077 "
                "--packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 "
                f"{pyspark_app_home}/{script}"
            ),
            env={
                "CORE_CONF_fs_defaultFS": HDFS_NAMENODE,
                "MONGO_URI": MONGO_URI
            },
            dag=dag
        )