from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
import requests

url = 'https://fakerapi.it/api/v2/products?_quantity=1'


def get_block_number():

    response = requests.post(url, timeout=10).json()
    print(response)

    return response["data"][0]


def get_block_data(ti):

    block_data = ti.xcom.pull(task_ids=get_block_number.__name__)

    response = requests.get(url, timeout=10).json()

    return response


with DAG("crypto_sync", start_date=datetime(2026, 1, 1), schedule=timedelta(minutes=100), catchup=False) as dag:
    fetchBlockNumberOperator = PythonOperator(
        task_id=get_block_number.__name__, python_callable=get_block_number)

    # fetchBlockOperator = PythonOperator(
    #     task_id=get_block_data.__name__, python_callable=get_block_data)

    # fetchBlockNumberOperator
