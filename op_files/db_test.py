from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import mysql.connector
from airflow.providers.postgres.operators.postgres import PostgresOperator 

import mysql.connector

with DAG(
    dag_id="db_test",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    create_table = PostgresOperator(
        task_id = "create_table",
        postgres_conn_id = "con",
        sql='CREATE table IF NOT EXISTS testable (name varchar(100) NULL,total_price varchar(100))'
        )

    create_table


