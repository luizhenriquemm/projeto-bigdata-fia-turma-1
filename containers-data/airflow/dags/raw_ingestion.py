import airflow
from datetime import datetime, timedelta
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 9, 17),
    'retries': None,
	'retry_delay': timedelta(minutes=15)
}

with airflow.DAG('raw_ingestion',
                  default_args=default_args,
                  schedule_interval='*/30 * * * *') as dag:

    task_raw_ingestion = SSHOperator(
        task_id='raw_ingestion',
        ssh_conn_id='spark_ssh_default',  # Specify your Spark cluster SSH connection ID
        command = 'export SPARK_HOME=/usr/local/spark; /opt/conda/bin/spark-submit --master local[*] --executor-memory 2g /opt/shared/scripts/raw_ingestion.py'
    )

    task_raw_ingestion