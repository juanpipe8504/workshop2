from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
from base_etl import traer_info,transformacion_grammy
from csv_etl import leer_csv,transformacion
from merge import merge,load
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 13),  
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def func1():
    print(f"the date is: {datetime.now()}")

with DAG(
    'api__project_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@daily',  
) as dag:

    merge = PythonOperator(
        task_id='merge',
        python_callable=merge,
        provide_context = True,
        )

    read_csv = PythonOperator(
        task_id='read_csv',
        python_callable=leer_csv,
        provide_context = True,
        )
     transform_csv = PythonOperator(
        task_id='transform_csv',
        python_callable=transformacion,
        provide_context = True,
        )

    read_db = PythonOperator(
        task_id='read_db',
        python_callable=traer_info,
        provide_context = True,
        )

    transform_db = PythonOperator(
        task_id='transform_db',
        python_callable=transformacion_grammy,
        provide_context = True,
        )

    store = PythonOperator(
        task_id='store',
        python_callable=func1,
        provide_context = True,
        )

    load = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context = True,
        )

    merge >> load >> store
    read_csv >> transform_csv >> merge
    read_db >> transform_db >> merge