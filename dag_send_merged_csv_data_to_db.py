
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

pg_params = "host=127.0.0.1 dbname=postgres user=postgres password='1'"
conn = connect(pg_params)


def create_tables(ds, **kwargs):
    create_tables(conn)


def copy_csv_to_db(ds, **kwargs):
    CopyCsvToPostgres(pg_params, csv_name="product_dataset", db_table_name="products").execute()
    CopyCsvToPostgres(pg_params, csv_name="category_dataset", db_table_name="categories").execute()


def insert_data_to_merged_table(ds, **kwargs):
    InsertMergedDataToTable(pg_params).execute()


create_tables_task = PythonOperator(
    task_id="create_tables_task",
    provide_context=True,
    depends_on_past=False,
    python_callable=create_tables,
    dag=dag,
)

copy_csv_to_db_task = PythonOperator(
    task_id="save_csv_on_db_task",
    provide_context=True,
    depends_on_past=False,
    python_callable=copy_csv_to_db,
    dag=dag,
)

insert_data_to_merged_table_task = PythonOperator(
    task_id="insert_data_to_merged_table_task",
    provide_context=True,
    depends_on_past=False,
    python_callable=insert_data_to_merged_table,
    dag=dag,
)

create_tables_task >> copy_csv_to_db_task >> insert_data_to_merged_table_task
