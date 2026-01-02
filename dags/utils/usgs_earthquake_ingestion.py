from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="hello_world",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["test"],
)
def hello_world():
    @task
    def say_hello():
        logging.info("Hello, Airflow! ✅")

    say_hello()


hello_world()
