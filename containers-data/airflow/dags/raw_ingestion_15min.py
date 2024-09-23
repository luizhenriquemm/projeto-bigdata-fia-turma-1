import airflow
from datetime import datetime, timedelta
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 9, 17),
    'retries': 0
	# 'retry_delay': timedelta(minutes=1)
}

with airflow.DAG('raw_ingestion_15min',
                  default_args=default_args,
                  schedule_interval='*/15 * * * *',
                  catchup=False,
                  max_active_runs=1) as dag:

    task_spark_raw_ingestion_corredor = SSHOperator(
        task_id='spark_raw_ingestion_corredor',
        ssh_conn_id='spark_ssh_default',  # Specify your Spark cluster SSH connection ID
        command = 'export SPARK_HOME=/usr/local/spark; /opt/conda/bin/spark-submit --master local[*] --driver-cores 2 --executor-cores 2 --driver-memory 4G --executor-memory 4G /opt/shared/scripts/raw_ingestion.py {{ ds }} corredor',
        conn_timeout=3600,
        cmd_timeout=3600
    )

    task_spark_raw_ingestion_empresa = SSHOperator(
        task_id='spark_raw_ingestion_empresa',
        ssh_conn_id='spark_ssh_default',  # Specify your Spark cluster SSH connection ID
        command = 'export SPARK_HOME=/usr/local/spark; /opt/conda/bin/spark-submit --master local[*] --driver-cores 2 --executor-cores 2 --driver-memory 4G --executor-memory 4G /opt/shared/scripts/raw_ingestion.py {{ ds }} empresa',
        conn_timeout=3600,
        cmd_timeout=3600
    )

    task_spark_raw_ingestion_posicao = SSHOperator(
        task_id='spark_raw_ingestion_posicao',
        ssh_conn_id='spark_ssh_default',  # Specify your Spark cluster SSH connection ID
        command = 'export SPARK_HOME=/usr/local/spark; /opt/conda/bin/spark-submit --master local[*] --driver-cores 2 --executor-cores 2 --driver-memory 4G --executor-memory 4G /opt/shared/scripts/raw_ingestion.py {{ ds }} posicao',
        conn_timeout=3600,
        cmd_timeout=3600
    )

    task_trigger_stage_processing = TriggerDagRunOperator(
        task_id='trigger_stage_processing',
        trigger_dag_id='stage_processing',  # O ID da DAG que você deseja disparar
        execution_date='{{ ds }}',  # Passa a data de execução da DAG
        reset_dag_run=True,  # Reexecuta a DAG, mesmo que já tenha sido executada nesse horário
        wait_for_completion=True,  # Se deseja esperar pela conclusão da DAG chamada
        poke_interval=10,  # Intervalo de verificação (em segundos) até que a DAG chamada termine
        allowed_states=['success'],  # Estado da DAG chamada para continuar
        dag=dag
    )

    [task_spark_raw_ingestion_corredor, task_spark_raw_ingestion_empresa, task_spark_raw_ingestion_posicao] >> task_trigger_stage_processing