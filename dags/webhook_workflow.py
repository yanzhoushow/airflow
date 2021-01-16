import boto3

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

def receive_message():
    message = "abc"
    print("receive message")
    return message

def post_webhook(**kwargs):
    ti = kwargs['ti']
    value = ti.xcom_pull(task_ids='receive_task')
    print("webhook message: "+value)
    return

dag = DAG('webhook_workflow', description='Workflow of Webhook', schedule_interval='*/1 * * * *', start_date=datetime(2021, 1, 1), catchup=False)

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

receive_task = PythonOperator(task_id='receive_task', python_callable=receive_message, dag=dag)

webhook_task = PythonOperator(task_id='webhook_task', python_callable=post_webhook, dag=dag)

start_operator >> receive_task >> webhook_task
