from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

def _downloading_data(**kwargs):
    with open('/tmp/my_file.txt', 'w') as f:
        f.write('my_data')

# def _downloading_data(**kwargs):
#     print(kwargs)

# def _downloading_data(my_param, ds):
#     print(my_param)

with DAG(dag_id='simple_dag', schedule_interval="@daily", 
        start_date=days_ago(3), catchup=False) as dag:

    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data,
        # op_kwargs={'my_param': 42}
    )

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='my_file.txt',
        # poke_interval=15
    )