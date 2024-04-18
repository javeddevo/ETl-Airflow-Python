
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
from project1.utils.call_config import load_config

config = load_config('config.json')

def dump_data():
    try:
        conn_string = f'{config["server"]}://{config["user"]}:{config["password"]}@{config["host"]}/{config["db"]}'
        db = create_engine(conn_string) 
        conn = db.connect() 
        df=pd.read_csv(config["output_file"])
        df.to_sql(config["table"],con=conn, if_exists='replace', index=False) 
        conn = psycopg2.connect(conn_string) 
        conn.autocommit = True
        cursor = conn.cursor() 
        
        sql1 =  f"select * from {config['table']};"
        cursor.execute(sql1) 
        for i in cursor.fetchall(): 
            print(i) 
        conn.close() 
    except Exception as e:
        print(f"unable to dump the data as error occured str{e}")
        raise e 


#just to confirm that able to fetch and read the clean file 
def read_file():
    df=pd.read_csv(config["output_file"])
    print(df)