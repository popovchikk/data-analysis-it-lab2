from __future__ import annotations

import os
from urllib.parse import urlparse

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.sensors.http import HttpSensor
from pendulum import datetime


# Основной источник выбран в ЛР №1: Bike Sharing Dataset, UCI.
# hour.csv — основной файл для анализа в рамках проекта.
SOURCE_URL = os.getenv(
    "SOURCE_URL",
    "https://raw.githubusercontent.com/alfozan/mlflow-example/fa1ed37c09962d5b134d07703ba95f07648b73e0/data/hour.csv",
)
DATASET_SLUG = os.getenv("DATASET_SLUG", "bike-sharing")
FILENAME = os.getenv("SOURCE_FILENAME", "hour.csv")
SOURCE_HTTP_CONN = os.getenv("SOURCE_HTTP_CONN_ID", "source_http_conn")
S3_CONN_ID = os.getenv("S3_CONN_ID", "s3_conn")
RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")


parsed_source = urlparse(SOURCE_URL)
SOURCE_ENDPOINT = parsed_source.path.lstrip("/")
if parsed_source.query:
    SOURCE_ENDPOINT += f"?{parsed_source.query}"


def load_to_raw(
    source_url: str,
    source_endpoint: str,
    filename: str,
    dataset_slug: str,
    s3_conn_id: str,
    http_conn_id: str,
    bucket_name: str,
    ingested_on: str,
    **_: object,
) -> None:
    """Download source bytes and store them unchanged in SeaweedFS raw."""
    if not source_url.startswith(("http://", "https://")):
        raise AirflowFailException(f"SOURCE_URL должен быть HTTP/HTTPS URL: {source_url}")

    object_key = f"{dataset_slug}/ingested_on={ingested_on}/{filename}"
    s3 = S3Hook(aws_conn_id=s3_conn_id)

    if not s3.check_for_bucket(bucket_name=bucket_name):
        raise AirflowFailException(
            f"Бакет '{bucket_name}' недоступен. Выполните docker compose up -d."
        )

    # Идемпотентность: существующий raw-объект не перезаписываем.
    if s3.check_for_key(key=object_key, bucket_name=bucket_name):
        print(f"Object already exists, skip upload: s3://{bucket_name}/{object_key}")
        return

    http = HttpHook(method="GET", http_conn_id=http_conn_id)
    response = http.run(endpoint=source_endpoint)
    response.raise_for_status()
    payload = response.content

    # raw хранит байты HTTP-ответа без очистки, распаковки или преобразований.
    s3.load_bytes(
        bytes_data=payload,
        key=object_key,
        bucket_name=bucket_name,
        replace=False,
    )
    print(f"Uploaded {len(payload)} bytes to s3://{bucket_name}/{object_key}")


with DAG(
    dag_id="ingest_raw",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lab2", "bike-sharing", "raw"],
    render_template_as_native_obj=False,
) as dag:
    wait_for_primary_source = HttpSensor(
        task_id="wait_for_primary_source",
        http_conn_id=SOURCE_HTTP_CONN,
        endpoint=SOURCE_ENDPOINT,
        method="GET",
        poke_interval=60,
        timeout=600,
        mode="poke",
        response_check=lambda response: response.status_code == 200,
    )

    load_to_raw_task = PythonOperator(
        task_id="load_to_raw",
        python_callable=load_to_raw,
        op_kwargs={
            "source_url": SOURCE_URL,
            "source_endpoint": SOURCE_ENDPOINT,
            "filename": FILENAME,
            "dataset_slug": DATASET_SLUG,
            "s3_conn_id": S3_CONN_ID,
            "http_conn_id": SOURCE_HTTP_CONN,
            "bucket_name": RAW_BUCKET,
            "ingested_on": "{{ ds }}",
        },
    )

    wait_for_primary_source >> load_to_raw_task
