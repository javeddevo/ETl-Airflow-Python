# from airflow import DAG
# from datetime import datetime
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator 


# with DAG(
#     dag_id="test",
#     start_date=datetime(year=2024, month=1, day=1, hour=9, minute=0),
#     schedule_interval= None,
#     catchup=False
# ) as dag:
#        def test1():
#               return "test1"
#        def test2():
#               return "test2"
#        def test3():
#               return "test3"
#        task1=PythonOperator(
#               task_id="task1",
#               dag=dag,
#               retries=6,
#               python_callable=test1
#        )
#        task2=PythonOperator(
#               task_id="task2",
#               dag=dag,
#               retries=6,
#               python_callable=test2
#        )
#        task3=PythonOperator(
#               task_id="task3",
#               dag=dag,
#               retries=6,
#               python_callable=test3
#        )
#        task1 >> task2 >> task3