"""

"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from psycopg2 import connect
from datetime import datetime, timedelta

from sql_create_tables import create_tables
from sql_send_datasets import CopyCsvToPostgres
from sql_merge import InsertMergedDataToTable


default_args = {
    "owner": "Anderson",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 13),
    "email": "andersonvnicius@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=7),
}

dag = DAG(
    "send_merged_csv_data_to_db",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="0 4 * * *",
)

dag.doc_md = __doc__


def transfer_merged_csv_to_db(ds, **kwargs):
    pg_params = "host=127.0.0.1 dbname=postgres user=postgres password='1'"
    conn = connect(pg_params)
    create_tables(conn)
    CopyCsvToPostgres(pg_params, csv_name="product_dataset", db_table_name="products").execute()
    CopyCsvToPostgres(pg_params, csv_name="category_dataset", db_table_name="categories").execute()
    InsertMergedDataToTable(pg_params).execute()


transfer_csv_to_db_task = PythonOperator(
    task_id="save_csv_on_db_task",
    provide_context=True,
    depends_on_past=False,
    python_callable=transfer_merged_csv_to_db,
    dag=dag,
)
