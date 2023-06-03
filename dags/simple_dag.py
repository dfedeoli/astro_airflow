from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

def _downloading_data(**kwargs):
    print(kwargs)

# def _downloading_data(my_param, ds):
#     print(my_param)

with DAG(dag_id='simple_dag', schedule_interval="@daily", 
        start_date=days_ago(3), catchup=False) as dag:

    # task_1 = DummyOperator(
    #     task_id='task_1',
    # )

    # task_2 = DummyOperator(
    #     task_id='task_2',
    #     retry=3,
    # )

    # task_3 = DummyOperator(
    #     task_id='task_3',
    # )

    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data,
        # op_kwargs={'my_param': 42}
    )
