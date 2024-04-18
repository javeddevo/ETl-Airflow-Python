from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import traceback
import os 

path=os.getcwd()
with DAG(
     dag_id="reading_csv",
     start_date=datetime(year=2024, month=1, day=1, hour=9, minute=0),
     schedule_interval= None
) as dag:
    
   def read_data():
      print(path)
      try:
        df=pd.read_csv("./raw_datas/input.csv")
        print(df)
      except Exception as e:
         print(e)
         raise e
   task1=PythonOperator(
      task_id="test1",
      dag=dag,
      python_callable=read_data
   )
   task1