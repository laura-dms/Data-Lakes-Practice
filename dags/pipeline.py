from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_lake_pipeline',
    default_args=default_args,
    description='Pipeline ETL pour le traitement des données',
    schedule=timedelta(days=1),
)

# Tâches du DAG ici ...


# Définir l'ordre des tâches
extract_task >> transform_task >> load_task