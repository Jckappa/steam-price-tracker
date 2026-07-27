"""Daily Steam price pipeline: extract -> load -> dbt run -> dbt test."""

from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Paths inside the Airflow container (mounted via docker-compose.override.yml)
PROJECT_DIR = "/usr/local/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/steam_dbt"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="steam_price_pipeline",
    description="Daily Steam price snapshots into BigQuery, transformed with dbt",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 7, 20, tz="Asia/Bangkok"),
    schedule="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["steam", "elt", "bigquery"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="python src/extract.py {{ ds }}",
        cwd=PROJECT_DIR,
    )

    load = BashOperator(
        task_id="load",
        bash_command="python src/load.py data/snapshots_{{ ds }}.jsonl",
        cwd=PROJECT_DIR,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run",
        cwd=DBT_DIR,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test",
        cwd=DBT_DIR,
    )

    extract >> load >> dbt_run >> dbt_test