from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Import de tes fonctions (assure-toi que ton code est dans des fonctions appelables)
from hn_api import get_top_stories
from es_handler import transform_and_load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hackernews_pipeline',
    default_args=default_args,
    description='Pipeline ETL HackerNews vers Elasticsearch',
    schedule_interval=timedelta(minutes=5), # Trigger toutes les 5 min
    catchup=False
) as dag:

    # Task 1: Extraction vers S3
    extract_task = PythonOperator(
        task_id='extract_from_api',
        python_callable=get_top_stories,
        op_kwargs={'limit': 50},
    )

    # Task 2: Transformation et Indexation ES
    transform_load_task = PythonOperator(
        task_id='transform_and_load_to_es',
        python_callable=transform_and_load,
    )

    # Ordonnancement des tâches
    extract_task >> transform_load_task