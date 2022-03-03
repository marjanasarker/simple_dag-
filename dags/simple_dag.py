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


#instantiate DAG object
#dag = DAG()
#task_1 = Operator(dag=dag)
#task_2 = Operator(dag=dag)
#instead of using all that above, we will use the with statement
#never use datetime.now() for start date
#schedule_interval can be a cron expression
#schedule_internal None - this will never be manually triggered

#create a dictionary for your default parameters so you don't have to repeat them for each task - good idea!
#this will now apply to all tasks
default_args = {
    'retries':1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure':False,
    'email_on_retry':False,
    'email': ['senderemail.tester86@gmail.com']
}

#creating python function to be called in task (downloading_data)
#for information on your DAG run, use **kwargs as an argument
# ds is the execution date of the DAG
def _downloading_data(**kwargs):
    with open('/tmp/my_file.txt', 'w') as f:
        f.write('my_data')
    return 42

def _checking_data(ti):
    my_xcom=ti.xcom_pull(key='return_value', task_ids=['downloading_data'])
    print(my_xcom)

def _failure(context):
    print("On callback failure")
    print(context)
    
with DAG(dag_id="simple_dag", default_args=default_args, schedule_interval="@daily", 
    start_date=datetime(2022, 3, 2), catchup=True) as dag:
    downloading_data=PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data
    )

    checking_data=PythonOperator(
        task_id='checking_data',
        python_callable=_checking_data

    )

    waiting_for_data = FileSensor(
        task_id="waiting_for_data",
        fs_conn_id="fs_default",
        filepath="my_file.txt"
    )
    processing_data=BashOperator(
        task_id='processing_data',
        bash_command='exit 0',
        on_failure_callback=_failure,
        email_on_failure=True,
        email_on_retry=True
    )

    
    #processing_data.set_upstream(waiting_for_data)
    #waiting_for_data.set_upstream(downloading_data)
    #another way is to use 
    chain(downloading_data, checking_data, waiting_for_data, processing_data)
    #chain is a helper operator
    #downloading_data >> waiting_for_data >> processing_data

    #defining cross dependencies
    #cross_downstream([downloading_data,checking_data], [waiting_for_data,processing_data])

    #put multiple things at the same level or same level, put them in a list
    #downloading_data >> [waiting_for_data, processing_data]