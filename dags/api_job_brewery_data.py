from datetime import timedelta 
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
import pendulum

local_tz = schedule_interval='America/Sao_Paulo'
DAG_NAME = "api_job_brewery_data"

DEFAULT_ARGS = {
    'owner': 'johnywauke',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['johnywauke@gmail.com'],
    'retries': 3, ## 3 tentativas caso falhe
    'retry_delay': timedelta(minutes=5), ##Tempo entre as tentativas
    'email_on_failure': True, ##Alerta em caso de falha
    'email_on_retry': False
}

with DAG(dag_id=DAG_NAME,
        default_args=DEFAULT_ARGS,
        tags=["api_job_brewery_data", "api", "brewery"],
        schedule_interval='*/30 * * * *', ## Roda a cada 30 minutos
        catchup=False,
        dagrun_timeout=timedelta(hours=2) ## Tempo rodando o job
    ) as dag:

    
    notebook_run = DatabricksRunNowOperator(
            task_id="notebook_run",
            job_id="420975064270484",
            dag=dag
        )
        
notebook_run