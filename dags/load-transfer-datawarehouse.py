from airflow import DAG
from utils import get_agencies, load_agencies
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'load-transfer-datawarehouse',
    default_args=default_args,
    description='Extract data from agency, cross them with transfer data and kext one, load to Data Warehouse',
    start_date=days_ago(2),
    tags=['example']
) as dag:

    Task_I = PostgresOperator(
        task_id = "truncate_table_agency_stage",
        postgres_conn_id="postgres_server",
        database="airflow",
        sql="TRUNCATE TABLE agency;"
    )

    Task_II = PostgresOperator(
        task_id = "truncate_table_bank_stage",
        postgres_conn_id="postgres_server",
        database="airflow",
        sql="TRUNCATE TABLE bank;"
    )

    Task_III = PythonOperator(
        task_id="load_agency_data",
        python_callable=load_agencies
    )

    Task_V = PostgresOperator(
        task_id='load_dw',
        postgres_conn_id='postgres_server',
        sql='insert-fact-manual.sql'
    )

Task_I >> Task_II >> Task_III >> Task_V