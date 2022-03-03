from ast import operator
from multiprocessing.connection import wait
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain, cross_downstream
from airflow.utils.email import send_email_smtp
import time
import smtplib
from airflow.sensors.external_task_sensor import ExternalTaskSensor

default_args = {
    
    'email': ['senderemail.tester86@gmail.com'],
    #'sla': timedelta(seconds=35),
    #'execution_timeout': timedelta(seconds=45)
}

def _sleeping():
    time.sleep(30)

def _sleeping2():
    time.sleep(55)

with DAG(dag_id="simple_dag_123", default_args=default_args, dagrun_timeout=timedelta(seconds=55), schedule_interval=None, 
    start_date=datetime(2022, 3, 2), catchup=False) as dag:
    sleeping_data=PythonOperator(task_id='sleeping_data',
        python_callable=_sleeping,
    
    )
    operator_2 =PythonOperator(task_id='operator_data',
        python_callable=_sleeping2,
    
    )
    effectiveness_mapping_gsheet_loader_sensor = ExternalTaskSensor(
        task_id="effectiveness_mapping_gsheet_loader_SENSOR",
        external_dag_id='simple_dag_123',
        external_task_id=None,
        allowed_states=['success'],
        execution_timeout= timedelta(seconds=45), #these are seconds, this is 8 hours
        
    )

    sleeping_data >> operator_2

#custom operator (sensor) that looks at when the last task completes, fails when dag doesn't complete sends notification
#best practices for external sensor 