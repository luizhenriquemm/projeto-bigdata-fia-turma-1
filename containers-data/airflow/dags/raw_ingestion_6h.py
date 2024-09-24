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

with airflow.DAG('raw_ingestion_6h',
                  default_args=default_args,
                  schedule_interval='0 0,6,12,18 * * *',
                  catchup=False,
                  max_active_runs=1) as dag:
    
    task_spark_raw_ingestion_linhas = SSHOperator(
        task_id='spark_raw_ingestion_linhas',
        ssh_conn_id='spark_ssh_default',  # Specify your Spark cluster SSH connection ID
        command = 'export SPARK_HOME=/usr/local/spark; /opt/conda/bin/spark-submit --master local[*] --driver-cores 4 --executor-cores 4 --driver-memory 16G --executor-memory 16G /opt/shared/scripts/raw_ingestion.py {{ ds }} linhas',
        conn_timeout=3600,  # Timeout de conexão (em segundos) = 1 hora
        cmd_timeout=3600,   # Timeout de execução do comando (em segundos) = 1 hora
    )
    
    task_spark_raw_ingestion_paradas = SSHOperator(
        task_id='spark_raw_ingestion_paradas',
        ssh_conn_id='spark_ssh_default',  # Specify your Spark cluster SSH connection ID
        command = 'export SPARK_HOME=/usr/local/spark; /opt/conda/bin/spark-submit --master local[*] --driver-cores 4 --executor-cores 4 --driver-memory 16G --executor-memory 16G /opt/shared/scripts/raw_ingestion.py {{ ds }} paradas',
        conn_timeout=3600,  # Timeout de conexão (em segundos) = 1 hora
        cmd_timeout=3600,   # Timeout de execução do comando (em segundos) = 1 hora
    )

    task_spark_raw_ingestion_previsao = SSHOperator(
        task_id='spark_raw_ingestion_previsao',
        ssh_conn_id='spark_ssh_default',  # Specify your Spark cluster SSH connection ID
        command = 'export SPARK_HOME=/usr/local/spark; /opt/conda/bin/spark-submit --master local[*] --driver-cores 4 --executor-cores 4 --driver-memory 16G --executor-memory 16G /opt/shared/scripts/raw_ingestion.py {{ ds }} previsao',
        conn_timeout=3600,  # Timeout de conexão (em segundos) = 1 hora
        cmd_timeout=3600,   # Timeout de execução do comando (em segundos) = 1 hora
    )

    # task_trigger_stage_processing = TriggerDagRunOperator(
    #     task_id='trigger_stage_processing',
    #     trigger_dag_id='stage_processing',  # O ID da DAG que você deseja disparar
    #     execution_date='{{ ds }}',  # Passa a data de execução da DAG
    #     # reset_dag_run=True,  # Reexecuta a DAG, mesmo que já tenha sido executada nesse horário
    #     # wait_for_completion=True,  # Se deseja esperar pela conclusão da DAG chamada
    #     # poke_interval=60,  # Intervalo de verificação (em segundos) até que a DAG chamada termine
    #     # allowed_states=['success'],  # Estado da DAG chamada para continuar
    #     dag=dag
    # )

    task_spark_raw_ingestion_linhas >> task_spark_raw_ingestion_paradas >> task_spark_raw_ingestion_previsao