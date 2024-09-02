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

import spotipy
from spotipy.oauth2 import SpotifyOAuth
import logging


## Folders and targets
AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
config = configparser.ConfigParser()
config.read(AIRFLOW_HOME + '/airflow.cfg')
AIRFLOW_DAGS = config.get('core', 'dags_folder')
CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))


# Spotify API credentials # Later we can create a config file to hide the below credentials
client_id = '7ac4100bc0f84e978f1b4c8e4b74576b'
client_secret = '62fed3a94a224ef48272a7b3d8ea0583'
redirect_uri = 'http://localhost:8888/callback'
scope = 'user-read-recently-played'


## Function to download the json data from API and save the data in json format to SPOTIFY_JSON_TARGET
def _download_from_spotify_api():
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Initialize Spotipy with Spotify credentials
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
                                                client_secret=client_secret,
                                                redirect_uri=redirect_uri,
                                                scope=scope))
    
    # Fetch recently played tracks
    results = sp.current_user_recently_played(limit=50)

    # Save raw JSON data
    with open('recently_played_tracks_raw.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    logging.info("Raw data saved to recently_played_tracks_raw.json")
    
    # Convert JSON to DataFrame and save as CSV
    df = pd.json_normalize(results['items'])  # Flatten the JSON structure to create a table
    df.to_csv('recently_played_tracks_raw.csv', index=False)
    logging.info("Raw data saved to recently_played_tracks_raw.csv")


## Prepare the data and save it in csv format to SPOTIFY_PREPARED_CSV
def _prepare_data():
    json_data = pd.read_csv('recently_played_tracks_raw.csv')
    album_name = []
    album_type = []
    album_total_tracks = []
    album_available_markets = []
    album_release_date = []
    album_artist = []
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object      
    for song in data["items"]:
        album_name.append(song["track"]["album"]['name'])
        album_type.append(song["track"]["album"]['album_type'])
        album_total_tracks.append(song["track"]["album"]['total_tracks'])
        album_available_markets.append(song["track"]["album"]['available_markets'])
        album_release_date.append(song["track"]["album"]['release_date'])
        album_artist.append(song["track"]["album"]['artists'])
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    df_spotify.to_csv(SPOTIFY_PREPARED_CSV, header=True)





## STAGE 

spotify_prepared_data_path = SPOTIFY_PREPARED_CSV

def execute_sql_commands():
    # Connect to the database using psycopg2
    conn = ps.connect(dbname="tuomas", user="tuomas")
    cur = conn.cursor()

    # List of SQL commands to execute
    sql_commands = [
        """
        CREATE TABLE IF NOT EXISTS public.tracks (
            track_id serial NOT NULL,
            track_name text,
            track_lenghts integer,
            date_id integer,
            album_id integer,
            popularity_id integer,
            PRIMARY KEY (track_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.album (
            album_id serial NOT NULL,
            album_name text,
            album_link text,
            album_type text,
            total_tracks integer,
            release_year integer,
            PRIMARY KEY (album_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.listening_date (
            date_id serial NOT NULL,
            date_time timestamp without time zone,
            hour integer,
            weekday text,
            week_of_year integer,
            month_num integer,
            month_name text,
            quarter integer,
            year integer,
            weekend text,
            PRIMARY KEY (date_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.artists (
            artist_id serial NOT NULL,
            artist text,
            PRIMARY KEY (artist_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.popularity (
            popularity_id serial NOT NULL,
            popularity integer,
            PRIMARY KEY (popularity_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.artists_tracks (
            track_id integer,
            artist_id integer,
            id serial NOT NULL,
            PRIMARY KEY (id)
        );
        """,
        """
        ALTER TABLE IF EXISTS public.tracks
            ADD CONSTRAINT tracks_listening_date FOREIGN KEY (date_id)
            REFERENCES public.listening_date (date_id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;
        """,
        """
        ALTER TABLE IF EXISTS public.tracks
            ADD CONSTRAINT tracks_album FOREIGN KEY (album_id)
            REFERENCES public.album (album_id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;
        """,
        """
        ALTER TABLE IF EXISTS public.tracks
            ADD CONSTRAINT tracks_popularity FOREIGN KEY (popularity_id)
            REFERENCES public.popularity (popularity_id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;
        """,
        """
        ALTER TABLE IF EXISTS public.artists_tracks
            ADD CONSTRAINT arttrack_tracks FOREIGN KEY (track_id)
            REFERENCES public.tracks (track_id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;
        """,
        """
        ALTER TABLE IF EXISTS public.artists_tracks
            ADD CONSTRAINT arttrack_artists FOREIGN KEY (artist_id)
            REFERENCES public.artists (artist_id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;
        """
    ]

    # Execute each SQL command
    for command in sql_commands:
        cur.execute(command)

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and the connection
    cur.close()
    conn.close()



def _stage():
    execute_sql_commands()
    
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
