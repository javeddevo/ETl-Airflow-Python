from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd

with DAG(
    dag_id="weather",
    start_date=datetime(year=2024, month=1, day=1, hour=9, minute=0),
    schedule_interval=None,
    catchup=False
) as dag:
        def extract_data_callable():
            print("this is the api data")
            return {"name":"javeed",
                    "age":29,
                    "country":"india",
                    "marks":{"english":30,"science":50}}
        
        extract_data=PythonOperator(
            dag=dag,
            task_id="extract_data",
            python_callable=extract_data_callable
            )

        def transform_data_callable(raw_data):
                # Transform response to a list
                transformed_data = {"name":[raw_data.get("name")],
                                    "age":[raw_data.get("age")],
                                    "country":[raw_data.get("country")],
                                    "english_marks":[raw_data.get("marks").get("english")]}
                return transformed_data
        transform_data = PythonOperator(
        dag=dag,
        task_id="transform_data",
        python_callable=transform_data_callable,
        op_kwargs={"raw_data": "{{ ti.xcom_pull(task_ids='extract_data') }}"}
        )

        def load(transform):
            df=pd.DataFrame(transform)
            print(df)
            return df
        load_data = PythonOperator(
        dag=dag,
        task_id="load_data",
        python_callable=load,
        op_kwargs={"transform": "{{ ti.xcom_pull(task_ids='transform_data') }}"}
        )
        extract_data >> transform_data >> load_data
# data=extract_data()
# transform =transform_data_callable(data)
# print(load(transform))

