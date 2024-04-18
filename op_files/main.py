from airflow.decorators import dag, task
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Connection
from datetime import datetime
import pandas as pd
import os

file_path = os.path.join(os.getcwd(), "raw_data", "input.csv")

@dag(
    start_date=datetime(year=2024, month=1, day=1, hour=9, minute=0),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        'depends_on_past': False
    },
)
def my_dag():

    @task()
    def load_data(filepath):
        print(f"file path is {file_path}")
        
        # Read data from the CSV file
        df = pd.read_csv(filepath)
        data_list = df.to_dict(orient='records')
        return data_list

    @task()
    def inspect_data(data_list):
        res = pd.DataFrame(data_list)
        print(res)

    @task()
    def create_table():
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS my_table (
            name VARCHAR(10) ,
            age INTEGER,
            no INTEGER
        )
        """
        return create_table_sql

    data = load_data(file_path)
    inspected_data = inspect_data(data)

    create_table_op = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='con',  # Replace with your PostgreSQL connection ID
        sql=create_table(),
        dag=my_dag
    )

    data >> inspected_data >> create_table_op

dag = my_dag
