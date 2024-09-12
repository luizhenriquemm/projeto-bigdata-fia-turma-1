import airflow
from datetime import datetime, timedelta
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 9, 12),
    'retries': 10,
	'retry_delay': timedelta(hours=1)
}

with airflow.DAG('raw_ingestion',
                  default_args=default_args,
                  schedule_interval='0 1 * * *') as dag:
    # task_raw_ingestion = SparkSubmitOperator(
    #     task_id='raw_ingestion',
    #     conn_id='spark_default',
    #     application="/opt/shared/scripts/raw_ingestion.py",
    #     # application_args=["{{ds}}","sqlserver://127.0.0.1:1433;databaseName=Teste"],#caso precise enviar dados da dag para o job airflow utilize esta propriedade
    #     total_executor_cores="*",
    #     executor_memory="2g",
    #     conf={
    #         'spark.master': 'spark://spark-master:7077',  # Host Spark no outro container
    #         'spark.executor.memory': '2g'
    #     },
    #     # conf={
    #     #     "spark.mongodb.input.uri": "mongodb://127.0.0.1:27017/Financeiro",
    #     #     "spark.mongodb.output.uri": "mongodb://127.0.0.1:27017/Financeiro",
    #     #     "spark.network.timeout": 10000000,
    #     #     "spark.executor.heartbeatInterval": 10000000,
    #     #     "spark.storage.blockManagerSlaveTimeoutMs": 10000000,
    #     #     "spark.driver.maxResultSize": "20g"
    #     # },
    #     # packages="org.mongodb.spark:mongo-spark-connector_2.12:3.0.0,com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8"
    #     verbose=True
    # )

    task_raw_ingestion = SSHOperator(
        task_id='raw_ingestion',
        ssh_conn_id='spark_ssh_default',  # Specify your Spark cluster SSH connection ID
        command = 'export SPARK_HOME=/usr/local/spark; /opt/conda/bin/spark-submit --master local[*] --executor-memory 2g /opt/shared/scripts/raw_ingestion.py '
    )

    task_raw_ingestion