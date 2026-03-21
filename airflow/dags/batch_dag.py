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
# scripts = [f"query_{i}.py" for i in range(10)]
scripts = ["query_0.py"]

with DAG(
    'statsbomb_batch',
    default_args=default_args,
    description='Daily StatsBomb batch processing',
    schedule_interval='0 2 * * *',
    catchup=False,
) as dag:

    os.environ["MAVEN_OPTS"] = "-Dmaven.wagon.http.timeout=600000 -Dmaven.wagon.httpconnectionManager.ttlSeconds=600"
    os.environ["SBT_OPTS"] = "-Dsbt.override.build.repos=true"

    for script in scripts:
        task_id = script.replace(".py","")
        task_ids.append(task_id)
        tasks[task_id] = BashOperator(
            task_id=task_id,
            bash_command=(
                "spark-submit " \
                "--master spark://spark-master:7077 " \
                "--jars $(echo /opt/spark/jars/*.jar | tr ' ' ',') " \
                f"{pyspark_app_home}/{script}"
            ),
            env={
                "JAVA_HOME": "/opt/java-8",
                "SPARK_HOME": "/usr/local/spark",  # full path to Spark installation
                "PATH": "/usr/local/spark/bin:/usr/bin:/usr/local/bin:$PATH",
                "PYSPARK_PYTHON": "/usr/local/bin/python",
                "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python",
                "CORE_CONF_fs_defaultFS": HDFS_NAMENODE,
                "MONGO_URI": MONGO_URI
            },
            dag=dag
        )