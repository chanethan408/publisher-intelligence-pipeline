from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="publisher_intelligence_pipeline",
    default_args=default_args,
    description="End-to-end ELT orchestration: YouTube -> S3 -> Snowflake -> dbt",
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Stage 1: Extract telemetry and upload to S3 raw layer
    extract_youtube = BashOperator(
        task_id="extract_youtube",
        bash_command="python /opt/airflow/src/main_extract.py --execution-date {{ ds }}",
    )

    # Stage 2: Stage raw S3 payloads into Snowflake raw tables
    stage_snowflake = BashOperator(
        task_id="stage_snowflake",
        bash_command="python /opt/airflow/src/load/stage_snowflake.py --execution-date {{ ds }}",
    )

    # Stage 3: Execute dbt models (staging, intermediate, incremental fact)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_publisher_intel && dbt run --profiles-dir .",
    )

    # Stage 4: Run dbt assertions and data quality tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_publisher_intel && dbt test --profiles-dir .",
    )

    # Enforce strict dependency ordering
    extract_youtube >> stage_snowflake >> dbt_run >> dbt_test
