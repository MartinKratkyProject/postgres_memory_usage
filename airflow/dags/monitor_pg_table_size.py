from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models.param import Param
from airflow.models import Variable

TARGET_CONN_ID = Variable.get("target_pg_conn_id")
METRICS_CONN_ID = Variable.get("metrics_pg_conn_id")

default_params = {
    "schema_name": Param('public', type='string', description='The schema to monitor'),
    "monitor_tables": Param('all', type='string', description='Comma-separated tables to monitor, "all" means all tables'),
}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def ensure_metrics_table(**context):
    create_sql = """
    CREATE TABLE IF NOT EXISTS monitored_table_sizes (
        id SERIAL PRIMARY KEY,
        schema_name TEXT NOT NULL,
        table_name TEXT NOT NULL,
        size_bytes BIGINT NOT NULL,
        checked_at TIMESTAMPTZ DEFAULT now() 
    );
    """
    metrics_hook = PostgresHook(postgres_conn_id=METRICS_CONN_ID)
    metrics_hook.run(create_sql)


def log_table_sizes(**context):
    schema = context["params"].get("schema_name", "public")
    tables_param = context["params"].get("monitor_tables", "all").strip()

    target_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
    metrics_hook = PostgresHook(postgres_conn_id=METRICS_CONN_ID)

    if tables_param.lower() == "all":
        records = target_hook.get_records("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE';
        """, parameters=[schema])
        tables = [row[0] for row in records]
    else:
        tables = [t.strip() for t in tables_param.split(",") if t.strip()]

    print(f"Monitoring schema: {schema}, tables = {tables}")

    timestamp_now = target_hook.get_first("SELECT NOW()")[0]

    for table in tables:
        size_result = target_hook.get_first(
            "SELECT pg_total_relation_size(%s)",
            parameters=[f"{schema}.{table}"]
        )
        size_bytes = size_result[0]

        metrics_hook.run(
            """
            INSERT INTO monitored_table_sizes (schema_name, table_name, size_bytes, checked_at)
            VALUES (%s, %s, %s, %s);
            """,
            parameters=(schema, table, size_bytes, timestamp_now)
        )

with DAG(
    dag_id="monitor_pg_table_size",
    default_args=default_args,
    params=default_params,
    schedule_interval="0 6 * * 1",  # Every Monday at 6 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["monitoring", "postgres"],
) as dag:

    ensure_table = PythonOperator(
        task_id="ensure_metrics_table",
        python_callable=ensure_metrics_table,
    )

    log_size = PythonOperator(
        task_id="log_table_sizes",
        python_callable=log_table_sizes,
    )

    ensure_table >> log_size
