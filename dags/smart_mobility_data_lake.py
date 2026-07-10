"""DAG Airflow : pipeline complet VeliLake (raw -> staging -> curated).

Orchestre les briques deja developpees et testees du projet. Chaque tache est
un PythonOperator qui appelle une fonction de `src`. Le code du projet est monte
dans le conteneur Airflow (PYTHONPATH=/opt/project) et la config pointe vers les
autres conteneurs via des variables d'environnement (voir docker-compose.yml).

Graphe :
    setup_infrastructure
        |-> ingest_file_to_raw ---|
        |-> ingest_api_to_raw  ---|-> raw_to_staging -> staging_to_curated -> validate_pipeline
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.ingest_file import ingest_file
from src.ingestion.ingest_velib_api import ingest_velib
from src.ingestion.ingest_weather_api import ingest_weather
from src.storage import mongo_client, mysql_client, s3_client
from src.transformation import raw_to_staging as r2s
from src.transformation.staging_to_curated import staging_to_curated


def _setup_infrastructure(**_):
    """Cree bucket S3, schema MySQL et index MongoDB (idempotent)."""
    s3_client.ensure_bucket()
    mysql_client.init_schema()
    mongo_client.ensure_indexes()


def _ingest_file_to_raw(**_):
    """Ingestion du CSV UCI (ignoree proprement si le fichier est absent)."""
    try:
        ingest_file()
    except FileNotFoundError as exc:
        print(f"CSV UCI absent, tache ignoree : {exc}")


def _ingest_api_to_raw(**_):
    """Ingestion des flux Velib et meteo vers la zone raw."""
    ingest_velib()
    ingest_weather()


def _raw_to_staging(**_):
    """Transformation des donnees brutes vers MySQL."""
    r2s.velib_raw_to_staging()
    r2s.weather_raw_to_staging()
    r2s.bike_file_raw_to_staging()


def _staging_to_curated(**_):
    """Enrichissement et chargement dans MongoDB."""
    staging_to_curated()


def _validate_pipeline(**_):
    """Compte les objets/lignes/documents et echoue si une zone est vide."""
    raw = s3_client.count_objects("raw/")
    rows = {t: mysql_client.count_rows(t) for t in sorted(mysql_client.ALLOWED_TABLES)}
    docs = mongo_client.count_documents()
    print(f"VALIDATION | raw={raw} objets | mysql={rows} | curated={docs} documents")
    if raw == 0 or docs == 0:
        raise ValueError("Zone vide detectee (raw ou curated)")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="smart_mobility_data_lake",
    default_args=default_args,
    description="Pipeline VeliLake : ingestion -> raw -> staging -> curated",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",  # toutes les 15 min ; mettre None pour manuel seulement
    catchup=False,
    tags=["velilake", "datalake"],
) as dag:
    t_setup = PythonOperator(task_id="setup_infrastructure",
                                python_callable=_setup_infrastructure)
    t_file = PythonOperator(task_id="ingest_file_to_raw",
                            python_callable=_ingest_file_to_raw)
    t_api = PythonOperator(task_id="ingest_api_to_raw",
                            python_callable=_ingest_api_to_raw)
    t_staging = PythonOperator(task_id="raw_to_staging",
                                python_callable=_raw_to_staging)
    t_curated = PythonOperator(task_id="staging_to_curated",
                                python_callable=_staging_to_curated)
    t_validate = PythonOperator(task_id="validate_pipeline",
                                python_callable=_validate_pipeline)

    t_setup >> [t_file, t_api] >> t_staging >> t_curated >> t_validate