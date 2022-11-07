from pathlib import Path
from dotenv import load_dotenv
import os
import mysql.connector
from mysql.connector import errorcode
import psycopg2 as psycopg2
import psycopg2.extras as extras
from sqlalchemy import create_engine
import urllib
import datetime as dt
import pandas as pd
import numpy as np


env_path = str(Path.home()) + '\dir\to\envfile\dev.env'
load_dotenv(dotenv_path=env_path)

DBHost = os.getenv("DBHost")
DBUser = os.getenv("DBUser")
DBPassword = os.getenv("DBPassword")
DBPort = os.getenv("DBPort")
DBSchema = os.getenv("DBSchema")

DBdict = {
    "host": DBHost,
    "database": DBSchema,
    "port": DBPort,
    "user": DBUser,
    "password": DBPassword
}



def create_connections(conntype):
    """Create a connection to a DB."""
    try:
        if conntype == 'mysql':
            connect_string = f"mysql+mysqldb://{DBUser}:{urllib.parse.quote_plus(DBPassword)}@{DBHost}:{DBPort}/{DBSchema}"
            print("Info: Opened a MySQL DB connection")
        elif conntype == 'pg':
            connect_string = f"postgresql+psycopg2://{DBUser}:{urllib.parse.quote_plus(DBPassword)}@{DBHost}:{DBPort}/{DBSchema}"
            print("Info: Opened a Postgres DB connection")
        conn = create_engine(connect_string)
        return conn
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def run_query(db, engine, query):
    """Run a query and put results into a dataframe.
       and notify if resulting df is empty."""
    if db == 'pg' or db == 'mysql':
        with engine.connect() as connection:
            df = pd.read_sql(sql=query, con=connection)
    else:
        df = 0
        print('Error resulting df is empty')
    return df


querystring = """
    SELECT *
    FROM "tablename" as tn
    ;"""
engine = create_connections('pg')
df = run_query('pg', engine, querystring)