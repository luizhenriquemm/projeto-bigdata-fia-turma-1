import airflow
from datetime import datetime, timedelta
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 9, 17),
    'retries': None,
	'retry_delay': timedelta(minutes=15)
}

with airflow.DAG('raw_ingestion',
                  default_args=default_args,
                  schedule_interval='*/30 * * * *') as dag:

    task_spark_stage_processing = SSHOperator(
        task_id='spark_stage_processing',
        ssh_conn_id='spark_ssh_default',  # Specify your Spark cluster SSH connection ID
        command = 'export SPARK_HOME=/usr/local/spark; /opt/conda/bin/spark-submit --master local[*] --executor-memory 2g /opt/shared/scripts/raw_ingestion.py {{ ds }}'
    )

    task_trigger_stage_processing = TriggerDagRunOperator(
        task_id='trigger_stage_processing',
        trigger_dag_id='stage_processing',  # O ID da DAG que você deseja disparar
        execution_date='{{ ds }}',  # Passa a data de execução da DAG
        reset_dag_run=True,  # Reexecuta a DAG, mesmo que já tenha sido executada nesse horário
        wait_for_completion=True,  # Se deseja esperar pela conclusão da DAG chamada
        poke_interval=60,  # Intervalo de verificação (em segundos) até que a DAG chamada termine
        allowed_states=['success'],  # Estado da DAG chamada para continuar
        dag=dag
    )

    task_spark_stage_processing >> task_trigger_stage_processing