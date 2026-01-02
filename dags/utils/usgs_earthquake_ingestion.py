from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import logging
import requests
import pandas as pd




def should_run(**context):
    last_processed_str = Variable.get(
        "usgs_last_processed",
        default_var="1970-01-01T00:00:00+00:00",
    )
    last_processed = datetime.fromisoformat(last_processed_str)

    # janela baseada apenas no ultimo processamento
    starttime = last_processed
    endtime = min(starttime + timedelta(days=1), datetime.now(timezone.utc))

    # limita no maximo 1 dia por execucao
    endtime = min(endtime, starttime + timedelta(days=1))

    logging.info(
        "check_window last_processed=%s starttime=%s endtime=%s",
        last_processed,
        starttime,
        endtime,
    )

    # da skip na dag se o intervalo de execução for menor do que 5 minutos
    if endtime <= starttime or (endtime - starttime) < timedelta(minutes=5):
        return False

    context["ti"].xcom_push(key="starttime", value=starttime.isoformat())
    context["ti"].xcom_push(key="endtime", value=endtime.isoformat())
    return True


def fetch_usgs(**context):
    ti = context["ti"]
    starttime = ti.xcom_pull(key="starttime")
    endtime = ti.xcom_pull(key="endtime")

    base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    limit = 1000
    offset = 0
    all_features = []

    while True:
        params = {
            "format": "geojson",
            "starttime": starttime,
            "endtime": endtime,
            "orderby": "time",
            "limit": limit,
            "offset": offset,
        }

        resp = requests.get(base_url, params=params, timeout=20)
        resp.raise_for_status()
        payload = resp.json()

        features = payload.get("features", [])
        all_features.extend(features)

        if len(features) < limit:
            break
        offset += limit

    df = pd.json_normalize(all_features)
    print(df.head())
    print(f"Total eventos: {len(df)}")
    Variable.set("usgs_last_processed", endtime)




with DAG(
    dag_id="usgs_earthquake_ingestion",
    start_date=datetime(2025, 12, 1, tzinfo=timezone.utc),
    schedule="*/5 * * * *",
    catchup=True,
    default_args={"owner": "airflow"},
    tags=["usgs", "ingestion"],
) as dag:

    check_window = ShortCircuitOperator(
        task_id="check_window",
        python_callable=should_run,
    )

    fetch_task = PythonOperator(
        task_id="fetch_usgs",
        python_callable=fetch_usgs,
    )

    check_window >> fetch_task
