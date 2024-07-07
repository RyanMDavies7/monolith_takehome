import sys
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from utilities.ingest import csv_ingest
from utilities.connectors import get_db_connection
from utilities.validation import check_duplicates_in_table
from utilities.transform import normalize


def get_db_connection_task():
    return get_db_connection()


def data_loading_task(**kwargs):
    conn = kwargs['ti'].xcom_pull(task_ids='get_db_connection')
    csv_ingest('static/data/creditcard_2023.csv', conn, 'transactions', 'credit')


def data_cleaning_task(**kwargs):
    conn = kwargs['ti'].xcom_pull(task_ids='get_db_connection')
    check_duplicates_in_table(conn, 'credit')


def model_prediction_task(**kwargs):
    conn = kwargs['ti'].xcom_pull(task_ids='get_db_connection')
    predictions = normalize(conn, 'static/model/credit_card_model.pkl', 'transactions.credit')
    return predictions


def data_export_task(**kwargs):
    predictions = kwargs['ti'].xcom_pull(task_ids='model_prediction')
    predictions.to_csv('static/data/credit_prediction.csv')


def close_db_connection_task(**kwargs):
    conn = kwargs['ti'].xcom_pull(task_ids='get_db_connection')
    if conn and not conn.closed:
        conn.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 5),
    'retries': 1,
}

with DAG(dag_id='credit_card_processing',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    get_db_connection = PythonOperator(
        task_id='get_db_connection',
        python_callable=get_db_connection_task
    )

    data_loading = PythonOperator(
        task_id='data_loading',
        python_callable=data_loading_task,
        provide_context=True
    )

    data_cleaning = PythonOperator(
        task_id='data_cleaning',
        python_callable=data_cleaning_task,
        provide_context=True
    )

    model_prediction = PythonOperator(
        task_id='model_prediction',
        python_callable=model_prediction_task,
        provide_context=True
    )

    data_export = PythonOperator(
        task_id='data_export',
        python_callable=data_export_task,
        provide_context=True
    )

    close_db_connection = PythonOperator(
        task_id='close_db_connection',
        python_callable=close_db_connection_task,
        provide_context=True
    )

    get_db_connection >> data_loading >> data_cleaning >> model_prediction >> data_export >> close_db_connection
