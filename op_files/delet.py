import pandas as pd
from airflow import DAG
import psycopg2
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine


# Function to read data from CSV, transform, and insert into PostgreSQL
def read_file():
    #df=pd.read_csv("./cleaned_data/customer_cleaned.csv")
    conn_string = 'postgres://airflow:airflow@host.docker.internal/test'
  
    db = create_engine(conn_string) 
    conn = db.connect() 
    
    df=pd.read_csv("./cleaned_data/customer_cleaned.csv")
  
    df.to_sql('customer', con=conn, if_exists='replace', index=False) 
    conn = psycopg2.connect(conn_string) 
    conn.autocommit = True
    cursor = conn.cursor() 
    
    sql1 = '''select * from customer;'''
    cursor.execute(sql1) 
    for i in cursor.fetchall(): 
        print(i) 
    conn.close() 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'schedule_interval': None
}

dag = DAG(
    'process_data_dag',
    default_args=default_args,
    description='Process CSV data and insert into PostgreSQL',
    catchup=False
)

# Define a PythonOperator to execute the process_data function
process_data_task = PythonOperator(
    task_id='process_data_task',
    python_callable=read_file,
    dag=dag
)

# Set task dependencies
process_data_task

