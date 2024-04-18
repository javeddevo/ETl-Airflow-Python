import psycopg2
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime
import subprocess
from sqlalchemy import create_engine
import pandas as pd
import os 
import glob



def read_file():
    conn_string = 'postgres://airflow:airflow@host.docker.internal/test'
    db = create_engine(conn_string) 
    conn = db.connect() 
    
    df=pd.read_csv("./cleaned_data/customer_cleaned.csv")
  
    df.to_sql('customer', con=conn, if_exists='replace', index=False) 
    conn = psycopg2.connect(conn_string) 
    conn.autocommit = True
    cursor = conn.cursor() 
    
    sql1 = '''CREATE TABLE IF NOT EXISTS dummy(
            ID INT,
            First_Name VARCHAR(255),
            Last_Name VARCHAR(255));
             
            '''
    cursor.execute(sql1) 
    conn.close() 

dag= DAG(dag_id='create_table',
                         description='Data Ingestion DAG from CSV to PostgreSQL DB',
                         schedule_interval=None,
                         start_date=datetime(2024,1,4))

# Define the tasks
task0 = PythonOperator(task_id='read_file',
                       python_callable=read_file,
                       dag=dag)
task0




