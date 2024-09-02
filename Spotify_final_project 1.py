from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

import os
import requests
from datetime import datetime
import json
import pandas as pd
import configparser

import psycopg2 as ps 
from sqlalchemy import create_engine  

import matplotlib.pyplot as oPlot

from datetime import datetime, timedelta


## Folders and targets
AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
config = configparser.ConfigParser()
config.read(AIRFLOW_HOME + '/airflow.cfg')
AIRFLOW_DAGS = config.get('core', 'dags_folder')
SPOTIFY_JSON_TARGET = AIRFLOW_DAGS + '/spotify/spotify.json' # download from Spotify
SPOTIFY_PREPARED_CSV = AIRFLOW_DAGS + '/spotify/spotify_prepared.csv' # prepared data to csv format
CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))


## Function to download the json data from API and save the data in json format to SPOTIFY_JSON_TARGET
# WE NEED TO HAVE A CONFIG FILE TO SAVE API KEY
def _download_from_spotify_api():
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    response_api = requests.get("https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=59.90&lon=10.75",headers = headers)
    return response_api

## Prepare the data and save it in csv format to SPOTIFY_PREPARED_CSV
def _prepare_data():
    json_data = pd.read_json(SPOTIFY_JSON_TARGET)

    df_spotify.to_csv(SPOTIFY_PREPARED_CSV, header=True)


## STAGE 

def postgres_creator():  # sqlalchemy requires callback-function for connections
  return ps.connect(
        dbname="tuomas",  # name of schema
        user="tuomas"
    #   password=db_pw,
    #   port=5432,          # default port is 5432, but can be specified
    #   host="localhost"
  )

postgres_engine = create_engine(
    url="postgresql+psycopg2://localhost",  # driver identification + dbms api
    creator=postgres_creator  # connection details
)

# Data processing

spotify_prepared_data_path = SPOTIFY_PREPARED_CSV

def _stage():
    spotify_data = pd.read_csv(
    spotify_prepared_data_path,
    sep=",",
    # header=None
    # encoding="unicode_escape"
    )

    # fake_data.columns = ["name", "age"] # Change column names if needed
    conn = ps.connect(dbname="tuomas", user="tuomas")

    cur = conn.cursor()

    with postgres_engine.connect() as conn:
        # Drop the table with CASCADE if it exists
        conn.execute("DROP TABLE IF EXISTS spotify_data CASCADE;") ## Need to be made for the spotify database views


    # Write to new sources
    spotify_data.to_sql(name="spotify_data", con=postgres_engine, if_exists="replace", index=False) # write to postgres


## MODEL ##

def _model():
    # Change dbname to be the name of your schema and user to be the owner of said database schema
    conn = ps.connect(dbname="tuomas", user="tuomas")

    cur = conn.cursor()

    cur.execute("CREATE VIEW norway_tomorrow AS SELECT temperature FROM norway_data WHERE date::date = CURRENT_DATE + INTERVAL '1 day';")

    cur.execute("CREATE VIEW swe_tomorrow AS SELECT temperature FROM swe_data WHERE date::date = CURRENT_DATE + INTERVAL '1 day';")

    cur.execute("SELECT * FROM norway_tomorrow;") 
    result_norway = cur.fetchall()

    conn.commit()
    

    cur.execute("SELECT * FROM swe_tomorrow;")
    result_swe = cur.fetchall()

    conn.commit()

    cur.close()
    conn.close()

    tomorrow_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

    hours_24 = list(range(1, 25))
    oPlot.figure(facecolor="grey")
    oPlot.plot(hours_24, result_swe, color="black")
    oPlot.scatter(hours_24, result_swe, color="red")
    oPlot.title("Tomorrow's weather in Östhammar, Sweden", color="black", fontweight="bold")
    oPlot.xlabel("Hours", color="black")
    oPlot.ylabel("Temperature (°C)", color = "black")
    oPlot.xticks(list(range(1, 25)), labels=hours_24, rotation=30)
    oPlot.savefig(f'weather_tomorrow_swe_{tomorrow_date}.png')
    
    oPlot.figure(facecolor="grey")
    oPlot.plot(hours_24, result_norway, color="black")
    oPlot.scatter(hours_24, result_norway, color="red")
    oPlot.title("Tomorrow's weather in Oslo Opera house, Norway", color="black", fontweight="bold")
    oPlot.xlabel("Hours", color="black")
    oPlot.ylabel("Temperature (°C)", color = "black")
    oPlot.xticks(list(range(1, 25)), labels=hours_24, rotation=30)
    oPlot.savefig(f'weather_tomorrow_nor_{tomorrow_date}.png')


## DAG ##
with DAG(
    "mini_project_dag",
    start_date=datetime(2021, 1, 1), 
    schedule="@DAILY", # Hourly
    catchup=False
):
    download = PythonOperator(
        task_id="download",
        python_callable=_download_from_spotify_api
    )

    prepare_data = PythonOperator(
        task_id="prepare_data",
        python_callable=_prepare_data
    )

    stage = PythonOperator(
        task_id="stage",
        python_callable=_stage
    )

    model = PythonOperator(
        task_id="model",
        python_callable=_model
    )
    
    download >> prepare_data >> stage >> model
