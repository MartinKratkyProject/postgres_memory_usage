from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pendulum import timezone
from airflow.models import Variable
import json

# Load configs
TARGET_CONN_ID = Variable.get("target_pg_conn_id")
METRICS_CONN_ID = Variable.get("metrics_pg_conn_id")
SCHEMA_NAME = Variable.get("monitor_schema")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def log_table_sizes(**context):
    schema = SCHEMA_NAME
    tables_raw = Variable.get("monitor_tables", default_var="[]")

    try:
        tables = json.loads(tables_raw)
        if not isinstance(tables, list):
            raise ValueError
    except ValueError:
        raise ValueError("Variable `monitor_tables` must be a valid JSON list or empty.")

    target_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
    metrics_hook = PostgresHook(postgres_conn_id=METRICS_CONN_ID)

    if not tables:
        tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE';
        """
        records = target_hook.get_records(tables_query, parameters=[schema])
        tables = [row[0] for row in records]

    for table in tables:
        query = "SELECT pg_total_relation_size(%s)"
        size_result = target_hook.get_first(query, parameters=[f"{schema}.{table}"])
        size_bytes = size_result[0]

        insert_query = """
            INSERT INTO monitored_table_sizes (schema_name, table_name, size_bytes, checked_at)
            VALUES (%s, %s, %s, NOW());
        """
        metrics_hook.run(insert_query, parameters=(schema, table, size_bytes))


with DAG(
    dag_id="monitor_pg_table_size",
    default_args=default_args,
    schedule_interval="0 6 * * 1",  # Every Monday at 6 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["monitoring", "postgres"],
    timezone=timezone("Europe/Paris"),
) as dag:

    log_size = PythonOperator(
        task_id="log_table_sizes",
        python_callable=log_table_sizes,
    )

    log_size
