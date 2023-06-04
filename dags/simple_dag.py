from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models.baseoperator import chain, cross_downstream
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'retry': 5,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure':True,
    'email_on_retry':True,
    'email':'admin@admin.com'
}

def _downloading_data(ti, **kwargs):
    with open('/tmp/my_file.txt', 'w') as f:
        f.write('my_data')
    ti.xcom_push(key='my_key',value=43)

def _checking_data(ti):
    my_xcom = ti.xcom_pull(key='my_key', task_ids=['downloading_data'])
    print(my_xcom)

def _failure(context):
    print("On callback failure")
    print(context)

with DAG(dag_id='simple_dag', default_args=default_args, schedule_interval="@daily", 
        start_date=days_ago(3), catchup=True) as dag:

    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data,
        # op_kwargs={'my_param': 42}
    )

    checking_data = PythonOperator(
        task_id='checking_data',
        python_callable=_checking_data
    )

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='my_file.txt',
        # poke_interval=15
    )

    processing_data = BashOperator(
        task_id='processing_data',
        bash_command='exit 1',
        on_failure_callback=_failure
    )

    # downloading_data.set_downstream(waiting_for_data)
    # waiting_for_data.set_downstream(processing_data)
    # downloading_data >> waiting_for_data >> processing_data
    # downloading_data >> [waiting_for_data, processing_data]
    chain(downloading_data, checking_data, waiting_for_data, processing_data)
    # cross_downstream([downloading_data, checking_data],[waiting_for_data, processing_data])