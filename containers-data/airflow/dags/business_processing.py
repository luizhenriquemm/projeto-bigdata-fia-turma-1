import airflow
from datetime import datetime, timedelta
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 9, 23),
    'retries': 0
}

with airflow.DAG('business_processing',
                  default_args=default_args,
                  schedule_interval=None,
                  catchup=False,
                  max_active_runs=1) as dag:

    task_spark_business_processing = SSHOperator(
        task_id='spark_business_processing',
        ssh_conn_id='spark_ssh_default',  # Specify your Spark cluster SSH connection ID
        command = 'export SPARK_HOME=/usr/local/spark; /opt/conda/bin/spark-submit --master local[*] --executor-memory 16g /opt/shared/scripts/business_processing.py {{ ds }}'
    )

    task_spark_business_processing