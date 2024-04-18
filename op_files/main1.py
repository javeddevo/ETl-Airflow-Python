from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator 
from sqlalchemy import create_engine
from airflow.operators.bash_operator import BashOperator
import pandas as pd
import os
import psycopg2


default_args = {"owner": "javeed", "retries": 5}
def check_file_exists_and_read():
    try:
            path = "./raw_data/customer.csv"
            print(path)
            if os.path.isfile(path):
                return f"File path exixts you can call the second task"
            else:
                 return "path does not exists"
    except Exception as e:
        print(e)
        raise e


def load_data():
        path = "./raw_data/customer.csv"
        df=pd.read_csv(path)
        df=df.drop_duplicates()
        df=df.drop(columns=df.columns[7::])
        df["Last_Name"]=df["Last_Name"].str.strip("._/")
        df['Phone_Number'] = df['Phone_Number'].str.replace(r'\D', '',regex=True)
        df=df.fillna("")
        df["Paying Customer"]=df["Paying Customer"].str.replace("N/a","")
        df["Paying Customer"]=df["Paying Customer"].str.replace("No","N")
        df["Paying Customer"]=df["Paying Customer"].str.replace("Yes","Y")
        df["Do_Not_Contact"]=df["Do_Not_Contact"].str.replace("Yes","Y")
        df["Do_Not_Contact"]=df["Do_Not_Contact"].str.replace("No","N")
        df=df.drop(columns="Address")
        df=df.rename(columns={"Paying Customer":"Paying_Customer"})
        for i in df.index:
            if df.loc[i,"Do_Not_Contact"]=="Y" or df.loc[i,"Do_Not_Contact"]=="":
                df.drop(i,inplace=True)
            elif df.loc[i,"Phone_Number"]=="":
                df.drop(i,inplace=True)
        df.reset_index(drop=True,inplace=True)
        df.to_csv('./cleaned_data/customer_cleaned.csv',index=False)


def migrate_data(path,db_table):
    try:
        engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow",
                echo=True)
        df = pd.read_csv(path)
        print("<<<<<<<<<<start migrating data>>>>>>>>>>>>>>")
        df.to_sql(db_table, con=engine, if_exists="replace")
        print("<<<<<<<<<<<<<<<<<<<completed>>>>>>>>>>>>>>>>")
    except Exception as e:
         print(e)
         raise e

def copy_csv_to_table():
    conn = psycopg2.connect(database='test',user='airflow',password='airflow',
                            host='host.docker.internal',port='5432')
    conn.autocommit = True
    cursor = conn.cursor()
    copy_csv = '''
               \copy customer (CustomerID,First_Name,Last_Name,Phone_Number,Paying_Customer,Do_Not_Contact)
               FROM './cleaned_data/customer_cleaned.csv'
               DELIMITER ','
               CSV HEADER;
               '''
    cursor.execute(copy_csv)
    conn.close()
with DAG(
    dag_id="my_test1",
    default_args=default_args,
    start_date=datetime(year=2024, month=1, day=1, hour=9, minute=0),
    schedule_interval=None
) as dag:

    check_file_task = PythonOperator(
        task_id="check_file_exists_and_read",
        python_callable=check_file_exists_and_read
    )

    transform = PythonOperator(
        task_id="read_Data",
        python_callable=load_data,
        #op_kwargs={"df": "{{ ti.xcom_pull(task_ids='check_file_exists_and_read') }}"}
    )

    create_table= PostgresOperator(
        task_id='create_table',
        postgres_conn_id='con',  # Replace with your PostgreSQL connection ID
        sql="""CREATE TABLE IF NOT EXISTS customer (
            CustomerID INT,
            First_Name VARCHAR(255),
            Last_Name VARCHAR(255),
            Phone_Number VARCHAR(20),
            Paying_Customer VARCHAR(3),
            Do_Not_Contact VARCHAR(3));
             """,
        dag=dag,
      )
    
    # migrate = PythonOperator(
    #     task_id='migrate',
    #     dag=dag,
    #     python_callable=migrate_data,
    #     op_kwargs={
    #         "path": "./cleaned_data/customer_cleaned.csv",
    #         "db_table":"customer"
    #     }
    # )
    
    inject = BashOperator(
    task_id="inject",
    bash_command=(
                '''
               \copy customer (CustomerID,First_Name,Last_Name,Phone_Number,Paying_Customer,Do_Not_Contact)
               FROM './cleaned_data/customer_cleaned.csv'
               DELIMITER ','
               CSV HEADER;
               '''
    )
    )

    check_file_task >> transform>>create_table>>inject




