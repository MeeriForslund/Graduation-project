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

import numpy as np

from sqlalchemy.exc import IntegrityError


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
    
    # Calculate yesterday's midnight in Unix timestamp (milliseconds) 
    yesterday_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1) 
    yesterday_midnight_unix = int(yesterday_midnight.timestamp() * 1000) 
    logging.info(f"Yesterday's midnight in Unix timestamp (milliseconds): {yesterday_midnight_unix}")
    
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
def prepare_listening_date_dataframe():
    # Load the CSV into a DataFrame
    df = pd.read_csv('recently_played_tracks_raw.csv')

    # Ensure the 'played_at' column is in datetime format
    df['date_time'] = pd.to_datetime(df['played_at'])

    # Extract hour, weekday, week of year, month number, month name, quarter, and year
    df['hour'] = df['date_time'].dt.hour
    df['weekday'] = df['date_time'].dt.day_name()
    df['week_of_year'] = df['date_time'].dt.isocalendar().week
    df['month_num'] = df['date_time'].dt.month
    df['month_name'] = df['date_time'].dt.month_name()
    df['quarter'] = df['date_time'].dt.quarter
    df['year'] = df['date_time'].dt.year

    # Determine if the day is a weekend
    df['weekend'] = np.where(df['weekday'].isin(['Saturday', 'Sunday']), 'Yes', 'No')

    # Changing date_time column to only contain the date in date format
    df['date_time'] = df['date_time'].dt.date

    # Select only the columns relevant for the listening_date table
    listening_date_df = df[['date_time', 'hour', 'weekday', 'week_of_year', 'month_num', 'month_name', 'quarter', 'year', 'weekend']]

    return listening_date_df


def _prepare_data():
    df = pd.read_csv('recently_played_tracks_raw.csv')
    
    columns_to_drop = [
        'context', 
        'track.album.artists', 
        'track.album.available_markets', 
        'track.album.uri',
        'track.album.href', 
        'track.album.id', 
        'track.album.images', 
        'track.album.type', 
        'track.album.release_date_precision',
        'track.available_markets', 
        'track.disc_number', 
        'track.explicit', 
        'track.external_ids.isrc', 
        'track.external_urls.spotify',
        'track.href', 
        'track.is_local', 
        'track.preview_url', 
        'track.track_number', 
        'track.type', 
        'track.album.external_urls.spotify',
        'track.id',
        'track.uri'
    ]

    df.drop(columns=columns_to_drop, inplace=True)

    df['track.duration_ms'] = df['track.duration_ms'] / 1000
    df.rename(columns={'track.duration_ms': 'track.duration_sec'}, inplace=True)

    def parse_artists(artists_str):
        try:
            # Parse the JSON-like string into a Python list
            artists = json.loads(artists_str.replace("'", '"'))
            # Extract artist names
            artist_names = [artist['name'] for artist in artists]
            return artist_names
        except (json.JSONDecodeError, TypeError):
            # Handle any errors that occur during parsing
            return []

    # Apply the parsing function to the 'track.artists' column
    df['artist_names'] = df['track.artists'].apply(parse_artists)

    

    listening_date_dataframe = prepare_listening_date_dataframe()

    # played_at column no longer needed after its extracted to other dataframe
    # df.drop(columns=['played_at'], inplace=True)
    df.drop(columns=['played_at'], inplace=True)
    df.drop(columns=['track.artists'], inplace=True)

    # Combine the two DataFrames side by side (column-wise)
    combined_data = pd.concat([df, listening_date_dataframe], axis=1)

    combined_data.rename(columns={'track.album.album_type': 'album_type'}, inplace=True)
    combined_data.rename(columns={'track.album.name': 'album_name'}, inplace=True)
    combined_data.rename(columns={'track.album.release_date': 'release_date'}, inplace=True)
    combined_data.rename(columns={'track.album.total_tracks': 'total_tracks'}, inplace=True)
    combined_data.rename(columns={'track.duration_sec': 'track_lenghts'}, inplace=True)
    combined_data.rename(columns={'track.popularity': 'popularity'}, inplace=True)
    combined_data.rename(columns={'artist_names': 'artist'}, inplace=True)
    combined_data.rename(columns={'track.name': 'track_name'}, inplace=True)
    
    # Save the combined DataFrame to a new CSV file
    combined_data.to_csv('combined_spotify.csv', index=False)



def execute_sql_commands():
    # Connect to the database using psycopg2
    conn = ps.connect(dbname="tuomas", user="tuomas")
    cur = conn.cursor()

    # List of SQL commands to execute
    sql_commands = [
        """
            CREATE TABLE IF NOT EXISTS public.tracks
            (
                track_id serial NOT NULL,
                track_name text,
                length_id integer,
                date_id integer,
                album_id integer,
                popularity_id integer,
                artist_id integer,
                PRIMARY KEY (track_id)
            );

        """,
        """
            CREATE TABLE IF NOT EXISTS public.album
            (
                album_id serial NOT NULL,
                album_name text,
                album_type text,
                total_tracks integer,
                release_date date,
                PRIMARY KEY (album_id)
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS public.listening_date
            (
                date_id serial NOT NULL,
                date_time date,
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
            CREATE TABLE IF NOT EXISTS public.popularity
            (
                popularity_id serial NOT NULL,
                popularity integer,
                PRIMARY KEY (popularity_id)
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS public.artists
            (
                artist_id serial NOT NULL,
                artist_name text,
                PRIMARY KEY (artist_id)
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS public.lengths
            (
                length_id serial NOT NULL,
                length integer,
                PRIMARY KEY (length_id)
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
            ALTER TABLE IF EXISTS public.tracks
                ADD CONSTRAINT tracks_artists FOREIGN KEY (artist_id)
                REFERENCES public.artists (artist_id) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
                NOT VALID;
        """,
        """
            ALTER TABLE IF EXISTS public.tracks
                ADD CONSTRAINT tracks_lengths FOREIGN KEY (length_id)
                REFERENCES public.lengths (length_id) MATCH SIMPLE
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

def postgres_creator():  # sqlalchemy requires callback-function for connections
  return ps.connect(
            dbname="tuomas",  # name of schema
            user="tuomas"
        #   password=db_pw,
        #   port=5432,          # default port is 5432, but can be specified
        #   host="localhost"
    )


# Connect to the PostgreSQL database
postgres_engine = create_engine(
    url="postgresql+psycopg2://localhost",  # driver identification + dbms api
    creator=postgres_creator  # connection details
)

def _stage():
    # Ensure that tables exist by running SQL commands
    execute_sql_commands()

    # Path to the combined CSV file
    combined_data_path = 'combined_spotify.csv'

    # Read the data from the combined CSV file
    combined_data = pd.read_csv(combined_data_path)
    combined_data['track_lenghts'] = combined_data['track_lenghts'].round(0)

    # Ensure date_time is stripped of any extra spaces and converted to datetime.date object
    combined_data['date_time'] = pd.to_datetime(combined_data['date_time']).dt.date

    # Connect to the PostgreSQL database
    with postgres_engine.connect() as conn:
        # Prepare album data and handle duplicates
        album_df = combined_data[['album_name', 'album_type', 'total_tracks', 'release_date']].drop_duplicates()
        album_df.to_sql('album', conn, if_exists='append', index=False, method='multi')

        # Prepare popularity data and handle potential duplicates
        popularity_df = combined_data[['popularity']].drop_duplicates()
        popularity_df.to_sql('popularity', conn, if_exists='append', index=False, method='multi')

        # Prepare listening_date data and handle duplicates
        listening_date_df = combined_data[['date_time', 'hour', 'weekday', 'week_of_year', 'month_num', 'month_name', 'quarter', 'year', 'weekend']].drop_duplicates()
        listening_date_df.to_sql('listening_date', conn, if_exists='append', index=False, method='multi')

        # Prepare lengths data (track lengths) and handle duplicates
        lengths_df = combined_data[['track_lenghts']].drop_duplicates()
        lengths_df = lengths_df.rename(columns={'track_lenghts': 'length'})
        lengths_df.to_sql('lengths', conn, if_exists='append', index=False, method='multi')

        # Prepare artist data and handle duplicates
        artists_df = combined_data[['artist']].drop_duplicates()
        artists_df = artists_df.explode('artist')  # Split the artist column into multiple rows
        artists_df = artists_df.drop_duplicates().rename(columns={'artist': 'artist_name'})
        artists_df.to_sql('artists', conn, if_exists='append', index=False, method='multi')

        # Extract ID mappings from inserted data
        album_id_map = {row['album_name']: row['album_id'] for row in conn.execute('SELECT album_id, album_name FROM public.album').fetchall()}
        popularity_id_map = {row['popularity']: row['popularity_id'] for row in conn.execute('SELECT popularity_id, popularity FROM public.popularity').fetchall()}
        listening_date_id_map = {row['date_time']: row['date_id'] for row in conn.execute('SELECT date_id, date_time FROM public.listening_date').fetchall()}
        length_id_map = {row['length']: row['length_id'] for row in conn.execute('SELECT length_id, length FROM public.lengths').fetchall()}
        artist_id_map = {row['artist_name']: row['artist_id'] for row in conn.execute('SELECT artist_id, artist_name FROM public.artists').fetchall()}

        # Debug: Print available keys in listening_date_id_map and other maps to inspect them
        print(f"Available keys in listening_date_id_map: {list(listening_date_id_map.keys())}")
        print(f"Available keys in album_id_map: {list(album_id_map.keys())}")
        print(f"Available keys in popularity_id_map: {list(popularity_id_map.keys())}")
        print(f"Available keys in length_id_map: {list(length_id_map.keys())}")
        print(f"Available keys in artist_id_map: {list(artist_id_map.keys())}")

        # Prepare track data with IDs
        track_data = combined_data[['track_name', 'track_lenghts', 'date_time', 'album_name', 'popularity', 'artist']].copy()
        track_data['album_id'] = track_data['album_name'].map(album_id_map)
        track_data['popularity_id'] = track_data['popularity'].map(popularity_id_map)
        track_data['date_id'] = track_data['date_time'].map(listening_date_id_map)
        track_data['length_id'] = track_data['track_lenghts'].map(length_id_map)

        # Check for missing values in the mapped IDs
        print(f"Missing album IDs: {track_data[track_data['album_id'].isna()]}")
        print(f"Missing popularity IDs: {track_data[track_data['popularity_id'].isna()]}")
        print(f"Missing date IDs: {track_data[track_data['date_id'].isna()]}")
        print(f"Missing length IDs: {track_data[track_data['length_id'].isna()]}")

        # Insert tracks into the 'tracks' table
        track_data = track_data[['track_name', 'length_id', 'date_id', 'album_id', 'popularity_id']].drop_duplicates()
        track_data.to_sql('tracks', conn, if_exists='append', index=False, method='multi')

        # Link artists to tracks
        for _, row in combined_data.iterrows():
            # Ensure the date_time has no leading/trailing spaces and exists in the map
            date_time_stripped = row['date_time']
            track_id = conn.execute(
                'SELECT track_id FROM public.tracks WHERE track_name = %s AND length_id = %s AND date_id = %s AND album_id = %s AND popularity_id = %s',
                (
                    row['track_name'],
                    length_id_map.get(row['track_lenghts']),
                    listening_date_id_map.get(date_time_stripped),
                    album_id_map.get(row['album_name']),
                    popularity_id_map.get(row['popularity'])
                )
            ).fetchone()

            if track_id:
                for artist in json.loads(row['artist'].replace("'", '"')):
                    artist_id = artist_id_map.get(artist)
                    if artist_id:
                        conn.execute(
                            'INSERT INTO public.artists_tracks (track_id, artist_id) VALUES (%s, %s)', 
                            (track_id[0], artist_id)
                        )
                    else:
                        print(f"Artist {artist} not found in artist_id_map")




## STAGE 
### TESTING SECTION ###
# _download_from_spotify_api()
_prepare_data()
_stage()


# ## MODEL ##

# def _model():
#     # Change dbname to be the name of your schema and user to be the owner of said database schema
#     conn = ps.connect(dbname="tuomas", user="tuomas")

#     cur = conn.cursor()

#     cur.execute("CREATE VIEW norway_tomorrow AS SELECT temperature FROM norway_data WHERE date::date = CURRENT_DATE + INTERVAL '1 day';")

#     cur.execute("CREATE VIEW swe_tomorrow AS SELECT temperature FROM swe_data WHERE date::date = CURRENT_DATE + INTERVAL '1 day';")

#     cur.execute("SELECT * FROM norway_tomorrow;") 
#     result_norway = cur.fetchall()

#     conn.commit()
    

#     cur.execute("SELECT * FROM swe_tomorrow;")
#     result_swe = cur.fetchall()

#     conn.commit()

#     cur.close()
#     conn.close()

#     tomorrow_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

#     hours_24 = list(range(1, 25))
#     oPlot.figure(facecolor="grey")
#     oPlot.plot(hours_24, result_swe, color="black")
#     oPlot.scatter(hours_24, result_swe, color="red")
#     oPlot.title("Tomorrow's weather in Östhammar, Sweden", color="black", fontweight="bold")
#     oPlot.xlabel("Hours", color="black")
#     oPlot.ylabel("Temperature (°C)", color = "black")
#     oPlot.xticks(list(range(1, 25)), labels=hours_24, rotation=30)
#     oPlot.savefig(f'weather_tomorrow_swe_{tomorrow_date}.png')
    
#     oPlot.figure(facecolor="grey")
#     oPlot.plot(hours_24, result_norway, color="black")
#     oPlot.scatter(hours_24, result_norway, color="red")
#     oPlot.title("Tomorrow's weather in Oslo Opera house, Norway", color="black", fontweight="bold")
#     oPlot.xlabel("Hours", color="black")
#     oPlot.ylabel("Temperature (°C)", color = "black")
#     oPlot.xticks(list(range(1, 25)), labels=hours_24, rotation=30)
#     oPlot.savefig(f'weather_tomorrow_nor_{tomorrow_date}.png')


# ## DAG ##
# with DAG(
#     "mini_project_dag",
#     start_date=datetime(2021, 1, 1), 
#     schedule="@DAILY", # Hourly
#     catchup=False
# ):
#     download = PythonOperator(
#         task_id="download",
#         python_callable=_download_from_spotify_api
#     )

#     prepare_data = PythonOperator(
#         task_id="prepare_data",
#         python_callable=_prepare_data
#     )

#     stage = PythonOperator(
#         task_id="stage",
#         python_callable=_stage
#     )

#     model = PythonOperator(
#         task_id="model",
#         python_callable=_model
#     )
    
#     download >> prepare_data >> stage >> model
