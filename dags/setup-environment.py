from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
print('dag')
with DAG(
    'setup-environment',
    max_active_runs=1,
    start_date=days_ago(2),
    default_args=default_args
) as dag:

    t1_setup_stage = PostgresOperator(
        task_id='setup_stage',
        postgres_conn_id='postgres_server',
        sql='setup-stage.sql'
    )

    t2_setup_stage_desafio = PostgresOperator(
        task_id='setup_stage_desafio_dados',
        postgres_conn_id='postgres_server',
        sql='setup-stage-desafio-dados.sql'
    )

    t3_setup_dw = PostgresOperator(
        task_id='setup_dw',
        postgres_conn_id='postgres_server',
        sql='setup-dw.sql'
    )

[t1_setup_stage, t2_setup_stage_desafio, t3_setup_dw]