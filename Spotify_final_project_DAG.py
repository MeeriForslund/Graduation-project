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

import matplotlib.pyplot as plt

from datetime import datetime, timedelta

import spotipy
from spotipy.oauth2 import SpotifyOAuth
import logging

import numpy as np

from sqlalchemy.exc import IntegrityError

import seaborn as sns

# ## Folders and targets
# AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
# config = configparser.ConfigParser()
# config.read(AIRFLOW_HOME + '/airflow.cfg')
# AIRFLOW_DAGS = config.get('core', 'dags_folder')
# CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))


# Use AIRFLOW_HOME if it's set, otherwise use a default path
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/Users/zakariyafarah/airflow')

# Parse the airflow.cfg file from the AIRFLOW_HOME directory
config = configparser.ConfigParser()
config.read(os.path.join(AIRFLOW_HOME, 'airflow.cfg'))

# Get the DAGs folder path
AIRFLOW_DAGS = config.get('core', 'dags_folder')

# Print to verify paths
print(f"AIRFLOW_HOME: {AIRFLOW_HOME}")
print(f"DAGs folder: {AIRFLOW_DAGS}")


# Spotify API credentials 
# # LATER CREATE a config file to hide the below credentials
client_id = '7ac4100bc0f84e978f1b4c8e4b74576b'
client_secret = '62fed3a94a224ef48272a7b3d8ea0583'
redirect_uri = 'http://localhost:8888/callback'
scope = 'user-read-recently-played'


## Function to download the json data from API and save the data in json and csv format
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


## Prepare the data and save it in csv format
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

    # Select only the columns relevant for the listening_date table
    listening_date_df = df[['date_time', 'hour', 'weekday', 'week_of_year', 'month_num', 'month_name', 'quarter', 'year', 'weekend']]

    return listening_date_df


def _prepare_data():
    df = pd.read_csv('recently_played_tracks_raw.csv')
    
    columns_to_drop = [
        #'context', 
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
    df.drop(columns=['played_at'], inplace=True)
    df.drop(columns=['track.artists'], inplace=True)

    # Combine the two DataFrames side by side (column-wise)
    combined_data = pd.concat([df, listening_date_dataframe], axis=1)

    combined_data.rename(columns={'track.album.album_type': 'album_type'}, inplace=True)
    combined_data.rename(columns={'track.album.name': 'album_name'}, inplace=True)
    combined_data.rename(columns={'track.album.release_date': 'release_date'}, inplace=True)
    combined_data.rename(columns={'track.album.total_tracks': 'total_tracks'}, inplace=True)
    combined_data.rename(columns={'track.duration_sec': 'track_lenght'}, inplace=True)
    combined_data.rename(columns={'track.popularity': 'popularity'}, inplace=True)
    combined_data.rename(columns={'artist_names': 'artist'}, inplace=True)
    combined_data.rename(columns={'track.name': 'track_name'}, inplace=True)

    # Change the type of artist columns contents to strings, round the track_lenght, change the date_time to date
    combined_data['artist'] = combined_data['artist'].apply(lambda x: str(x) if isinstance(x, list) else x)
    combined_data['track_lenghts'] = combined_data['track_lenght'].round(0)

    # Deal with the timestamp
    combined_data.rename(columns={'date_time': 'dates'}, inplace=True)
    
    def remove_milliseconds(ts):
        return ts.strftime('%Y-%m-%d %H:%M:%S')

    # Apply the function to the 'dates' column
    combined_data['dates'] = combined_data['dates'].apply(remove_milliseconds)
    combined_data['dates'] = combined_data['dates'].astype(str)

    # List of columns to be dropped
    columns_to_drop = ['context.external_urls.spotify', 'context.uri', 'context.type', 'context.href']

    # Drop columns if they exist
    for column in columns_to_drop:
        if column in combined_data.columns:
            combined_data.drop(columns=[column], inplace=True)
        else:
            print(f"Column '{column}' not found in the DataFrame.")

    combined_data.info()

    # Save the combined DataFrame to a new CSV file
    combined_data.to_csv('combined_spotify.csv', index=False)




def execute_sql_commands():
    # Connect to the database using psycopg2
    conn = ps.connect(dbname="graduation_project", user="zakariyafarah")
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
    dates text,
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
CREATE TABLE IF NOT EXISTS public.artists
(
    artist_id serial NOT NULL,
    artist text,
    PRIMARY KEY (artist_id)
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
CREATE TABLE IF NOT EXISTS public.lengths
(
    length_id serial NOT NULL,
    track_lenght integer,
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
    ADD CONSTRAINT track_length FOREIGN KEY (length_id)
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
            dbname="graduation_project",  # name of schema
            user="zakariyafarah"
        #   password=db_pw,
        #   port=5432,          # default port is 5432, but can be specified
        #   host="localhost"
    )


# Update your connection string accordingly (replace with your actual username and password)
postgres_engine = create_engine('postgresql+psycopg2://zakariyafarah:AWAD12@localhost/graduation_project')

## STAGE 

def _stage():
    # Create the tables and relations, comment out after first run!
    # execute_sql_commands()

    # Path to the combined CSV file
    combined_data_path = 'combined_spotify.csv'

    # Read the data from the combined CSV file
    combined_data = pd.read_csv(combined_data_path)

    yesterday_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    combined_data['dates'] = pd.to_datetime(combined_data['dates'])
    combined_data = combined_data[combined_data['dates'] > yesterday_midnight]
    combined_data['dates'] = combined_data['dates'].astype(str)

    # Prepare individual DataFrames for each table
    tracks_df = combined_data[['track_name', 'album_name', 'artist', 'popularity', 'track_lenght', 'dates']].copy()
    album_df = combined_data[['album_name', 'album_type', 'total_tracks', 'release_date']].drop_duplicates()
    listening_date_df = combined_data[['dates', 'hour', 'weekday', 'week_of_year', 'month_num', 'month_name', 'quarter', 'year', 'weekend']].drop_duplicates()
    artists_df = combined_data[['artist']].drop_duplicates()
    popularity_df = combined_data[['popularity']].drop_duplicates()
    lengths_df = combined_data[['track_lenght']].drop_duplicates()

    # Load existing data from the reference tables
    existing_album_df = pd.read_sql_table('album', con=postgres_engine)
    existing_artists_df = pd.read_sql_table('artists', con=postgres_engine)
    existing_popularity_df = pd.read_sql_table('popularity', con=postgres_engine)
    existing_lengths_df = pd.read_sql_table('lengths', con=postgres_engine)
    existing_listening_date_df = pd.read_sql_table('listening_date', con=postgres_engine)

    # Identify new records
    new_album_df = album_df[~album_df.set_index(['album_name', 'album_type', 'total_tracks', 'release_date']).index.isin(existing_album_df.set_index(['album_name', 'album_type', 'total_tracks', 'release_date']).index)]
    new_artists_df = artists_df[~artists_df.set_index('artist').index.isin(existing_artists_df.set_index('artist').index)]
    new_popularity_df = popularity_df[~popularity_df.set_index('popularity').index.isin(existing_popularity_df.set_index('popularity').index)]
    new_lengths_df = lengths_df[~lengths_df.set_index('track_lenght').index.isin(existing_lengths_df.set_index('track_lenght').index)]
    new_listening_date_df = listening_date_df[~listening_date_df.set_index('dates').index.isin(existing_listening_date_df.set_index('dates').index)]

    # Insert only new records into the reference tables
    new_album_df.to_sql(name="album", con=postgres_engine, if_exists="append", index=False)
    new_artists_df.to_sql(name="artists", con=postgres_engine, if_exists="append", index=False)
    new_popularity_df.to_sql(name="popularity", con=postgres_engine, if_exists="append", index=False)
    new_lengths_df.to_sql(name="lengths", con=postgres_engine, if_exists="append", index=False)
    new_listening_date_df.to_sql(name="listening_date", con=postgres_engine, if_exists="append", index=False)

    # Load data from the foreign key tables to get the IDs
    album_ids = pd.read_sql_table('album', con=postgres_engine)[['album_id', 'album_name']]
    artists_ids = pd.read_sql_table('artists', con=postgres_engine)[['artist_id', 'artist']]
    popularity_ids = pd.read_sql_table('popularity', con=postgres_engine)[['popularity_id', 'popularity']]
    lengths_ids = pd.read_sql_table('lengths', con=postgres_engine)[['length_id', 'track_lenght']]
    listening_date_ids = pd.read_sql_table('listening_date', con=postgres_engine)[['date_id', 'dates']]

    # Ensure that both 'dates' columns are of the same type
    tracks_df['dates'] = tracks_df['dates'].astype(str)
    listening_date_ids['dates'] = listening_date_ids['dates'].astype(str)

    # Map names/values to their respective IDs
    tracks_df = tracks_df.merge(album_ids, how='left', on='album_name')
    tracks_df = tracks_df.merge(artists_ids, how='left', on='artist')
    tracks_df = tracks_df.merge(popularity_ids, how='left', on='popularity')
    tracks_df = tracks_df.merge(lengths_ids, how='left', on='track_lenght')
    tracks_df = tracks_df.merge(listening_date_ids, how='left', on='dates')

    # Prepare the final DataFrame for the tracks table
    tracks_final_df = tracks_df[['track_name', 'length_id', 'date_id', 'album_id', 'popularity_id', 'artist_id']].copy()

    # Insert data into the tracks table
    tracks_final_df.to_sql(name='tracks', con=postgres_engine, if_exists='append', index=False)


## MODEL ##
def _model():
    # Change dbname to be the name of your schema and user to be the owner of said database schema
    conn = ps.connect(dbname="graduation_project", user="zakariyafarah")
    cur = conn.cursor()

    # Picture nr.1 scatter

    cur.execute("SELECT t.track_name, l.track_lenght, p.popularity FROM public.tracks t JOIN public.lengths l ON t.length_id = l.length_id JOIN public.popularity p ON t.popularity_id = p.popularity_id;")
    name_lenght_popularity = cur.fetchall()
    print(name_lenght_popularity)
    df = pd.DataFrame(name_lenght_popularity)
    
    df = pd.DataFrame(name_lenght_popularity, columns=['track_name', 'track_length_seconds', 'popularity'])
    
    df['track_length_seconds'] = (df['track_length_seconds'] / 1000).round()
    print(df['track_length_seconds'])

    plt.figure(figsize=(10, 6))
    scatter = sns.scatterplot(x='track_length_seconds', y='popularity', data=df)

    # Optional: Add a trend line
    sns.regplot(x='track_length_seconds', y='popularity', data=df, scatter=False, color='red')

    # Customize the plot
    plt.title('Track Popularity vs. Track Length')
    plt.xlabel('Track Length (seconds)')
    plt.ylabel('Popularity')

    plt.savefig('scatter.png')


## TESTING SECTION ###
_download_from_spotify_api()
_prepare_data()
_stage()
_model()


## DAG ##
with DAG(
    "final_project_dag",
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