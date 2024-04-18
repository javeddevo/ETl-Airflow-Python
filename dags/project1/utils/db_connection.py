import psycopg2
import subprocess
from sqlalchemy import create_engine
import pandas as pd
import os 
import glob
from project1.utils.call_config import load_config


config = load_config('config.json')


# connecting to db 
def connect_to_db():
    try:
        conn = psycopg2.connect(database=config["db"],user=config["user"],password=config["password"],
                                host=config["host"],port=config["port"])
        conn.close()
        print('DB connected successfully')
    except Exception as e:
        print(f"unable connect to db as eror occured:str{e}")
        raise e 


# creating a table in psotgres 
def create_table_db():
    try:
        conn_string = f'{config["server"]}://{config["user"]}:{config["password"]}@{config["host"]}/{config["db"]}'
        db = create_engine(conn_string) 
        conn = db.connect() 
        sql_create_table = f"""
            CREATE TABLE IF NOT EXISTS {config["table"]}(
                CustomerID INT,
                First_Name VARCHAR(255),
                Last_Name VARCHAR(255),
                Phone_Number VARCHAR(20),
                Paying_Customer VARCHAR(3),
                Do_Not_Contact VARCHAR(3)
            );
        """
        conn.execute(sql_create_table)
        conn.close() 
    except Exception as e:
        print(e)
        raise e 