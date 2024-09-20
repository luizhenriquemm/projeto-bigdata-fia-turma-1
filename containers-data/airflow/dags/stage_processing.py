import airflow
from datetime import datetime, timedelta
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 9, 19),
    'retries': None
}

with airflow.DAG('stage_processing',
                  default_args=default_args,
                  schedule_interval=None) as dag:

    task_spark_stage_processing = SSHOperator(
        task_id='spark_stage_processing',
        ssh_conn_id='spark_ssh_default',  # Specify your Spark cluster SSH connection ID
        command = 'export SPARK_HOME=/usr/local/spark; /opt/conda/bin/spark-submit --master local[*] --executor-memory 2g /opt/shared/scripts/stage_processing.py {{ ds }}'
    )

    task_spark_stage_processing