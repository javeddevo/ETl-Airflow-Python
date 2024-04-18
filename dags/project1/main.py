"""
ETL Project Documentation

Author: Javeed
Email:javed30ma@gmail.com
Supporter: Sahithi
Project: ETL (Extract, Transform, Load)

Description:
This project implements an ETL (Extract, Transform, Load) pipeline using Python, Apache Airflow, SQL, PostgreSQL, Linux, and Docker. The purpose of the project is to extract data from various sources, perform necessary transformations, and load it into a PostgreSQL database for analysis and reporting.

Technologies Used:
- Python: Programming language used for scripting and data manipulation.
- Apache Airflow: Workflow management platform used for orchestrating the ETL pipeline.
- SQL: Language used for querying and manipulating relational databases.
- PostgreSQL: Relational database management system used for storing the extracted and transformed data.
- Linux: Operating system environment used for hosting the project.
- Docker: Containerization platform used for packaging and deploying the project components.


Usage:
To execute the ETL pipeline, ensure that Apache Airflow and PostgreSQL are properly configured and running. Then, schedule and trigger the Airflow DAGs defined in the 'dags/' directory. Monitor the execution status and logs in the Airflow UI to track the progress of the ETL processes.

For further information and troubleshooting, refer to the project documentation and source code comments.

"""


from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime
from project1.utils.preprocess import cleaning,install_dependencies
from project1.utils.db_connection import connect_to_db,create_table_db
from project1.utils.db_transaction import dump_data,read_file

#defining a dag
data_ingestion_dag = DAG(dag_id='data-ingestion',
                         description='Data Ingestion DAG from CSV to PostgreSQL DB',
                         schedule_interval=None,
                         start_date=datetime(2024,1,4))


task0 = PythonOperator(task_id='Install-dependencies',
                       python_callable=install_dependencies,
                       dag=data_ingestion_dag)
task1 = PythonOperator(task_id='data-cleaning',
                       python_callable=cleaning ,
                       dag=data_ingestion_dag)
task2 = PythonOperator(task_id='Connect-to-postgres_db',
                       python_callable=connect_to_db,
                       dag=data_ingestion_dag)
task3 = PythonOperator(task_id='Create-Table-In_Postgres',
                       python_callable=create_table_db,
                       dag=data_ingestion_dag)
task4=PythonOperator(task_id="check-csv-file",
                     python_callable=read_file,
                     dag=data_ingestion_dag)

task5 = PythonOperator(task_id='Copy-csv-to-postgres_table',
                       python_callable=dump_data,
                       dag=data_ingestion_dag)

task0>>task1>>task2>>task3>>task4>>task5
    
print('Done')