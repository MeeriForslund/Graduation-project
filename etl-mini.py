from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

import os
import requests
from datetime import datetime
import json
import pandas as pd
import configparser

## Folders and targets
AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
config = configparser.ConfigParser()
config.read(AIRFLOW_HOME + '/airflow.cfg')
AIRFLOW_DAGS = config.get('core', 'dags_folder')
WEATHER_JSON_TARGET = AIRFLOW_DAGS + '/weather_log.json' # download with Meeris API Key
WEATHER_CSV_TARGET = AIRFLOW_DAGS + '/weather_log.csv' # download from https://fmiodata-timeseries-convert.fmi.fi/Helsinki%20Kaisaniemi:%2018.8.2024%20-%2018.8.2024_a83b3556-4f51-4ac7-a116-d91e4bafdd4f.csv
TRANSFORMED_WEATHER = AIRFLOW_DAGS + '/weather_log.csv'

# appid = "replace_this" # Should use configparser to hide details  # Added if spare time is left!

chosen_locations = ["Helsinki Kaisaniemi" # download from Finnish Meteorological Institute
             "the other place we choose?" # download with Meeris API Key
]

## API GET with Meeris API Key  ##


## DOWNLOAD from https://fmiodata-timeseries-convert.fmi.fi/Helsinki%20Kaisaniemi:%2018.8.2024%20-%2018.8.2024_a83b3556-4f51-4ac7-a116-d91e4bafdd4f.csv ##



## COMBINE API GET AND DOWNLOAD TO SAME FUNCTION? ##

def _download():
    pass

## Save Raw files ##

def _save_raw():
    pass

## HARMONIZE ##

def _harmonize():
    pass


## CLEANSE ##

def _cleanse():
    pass

## STAGE ##

def _stage():
    pass

## MODEL ##

def _model():
    pass

## DAG ##
with DAG(
    "mini_project_dag",
    start_date=datetime(2021, 1, 1), 
    schedule="@DAILY", # Hourly
    catchup=False
):
    download = PythonOperator(
        task_id="download",
        python_callable=_download
    )

    save_raw = BranchPythonOperator(
        task_id="save_raw",
        python_callable=_save_raw
    )

    harmonize = PythonOperator(
        task_id="harmonize",
        python_callable=_harmonize
    )

    cleanse = PythonOperator(
        task_id="cleanse",
        python_callable=_cleanse
    )

    stage = PythonOperator(
        task_id="stage",
        python_callable=_stage
    )

    model = PythonOperator(
        task_id="model",
        python_callable=_model
    )
    
    download >> save_raw >> harmonize >> cleanse >> stage >> model