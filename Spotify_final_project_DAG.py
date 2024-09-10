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
import matplotlib.patches as patches
from mpl_toolkits.mplot3d import Axes3D

from datetime import datetime, timedelta

import spotipy
from spotipy.oauth2 import SpotifyOAuth
import logging

import numpy as np

from sqlalchemy.exc import IntegrityError

import seaborn as sns

## Folders and targets
AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
config = configparser.ConfigParser()
config.read(AIRFLOW_HOME + '/airflow.cfg')
AIRFLOW_DAGS = config.get('core', 'dags_folder')
CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))


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
        # 'context', 
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


    combined_data['release_date'] = combined_data['release_date'].astype(str)

    # Function to convert year-only dates to full dates
    def convert_year_to_date(date_str):
        if len(date_str) == 4:  # only year
            return f"{date_str}-01-01"
        return date_str
    
    combined_data['release_date'] = combined_data['release_date'].apply(convert_year_to_date)
    combined_data['release_date'] = pd.to_datetime(combined_data['release_date'], format='%Y-%m-%d')

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

def check_table_exists(table_name, conn):
    # Query to check if a table exists in the database
    query = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = '{table_name}'
    );
    """
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()[0]
    cur.close()
    return result

## STAGE 



def _stage():
    # Check if the tables already exist
    tables_to_check = ['tracks', 'album', 'listening_date', 'artists', 'popularity', 'lengths']
    conn = postgres_creator()
    tables_missing = any(not check_table_exists(table, conn) for table in tables_to_check)

    if tables_missing:
        print("Some tables are missing, executing SQL commands to create them.")
        execute_sql_commands()
    else:
        print("All required tables already exist.")

    conn.close()

    # Path to the combined CSV file
    combined_data_path = 'combined_spotify.csv'

    # Read the data from the combined CSV file
    combined_data = pd.read_csv(combined_data_path)

    # # Used to limit the inputs to only previous days listened songs, not necessary to use
    # yesterday_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    # combined_data['dates'] = pd.to_datetime(combined_data['dates'])
    # combined_data = combined_data[combined_data['dates'] > yesterday_midnight]
    # combined_data['dates'] = combined_data['dates'].astype(str)

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

    existing_tracks_df = pd.read_sql_table('tracks', con=postgres_engine)

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

    # Identify existing records in the 'tracks' table
    existing_tracks_df = existing_tracks_df[['track_name', 'length_id', 'date_id', 'album_id', 'popularity_id', 'artist_id']]

    # Filter out duplicates by checking for existing records
    new_tracks_df = tracks_final_df[~tracks_final_df.set_index(['track_name', 'length_id', 'date_id', 'album_id', 'popularity_id', 'artist_id']).index.isin(existing_tracks_df.set_index(['track_name', 'length_id', 'date_id', 'album_id', 'popularity_id', 'artist_id']).index)]

    # Insert only new records into the tracks table
    new_tracks_df.to_sql(name='tracks', con=postgres_engine, if_exists='append', index=False)

    # # Insert data into the tracks table
    # tracks_final_df.to_sql(name='tracks', con=postgres_engine, if_exists='append', index=False)


## MODEL ##
def _model():
    # Change dbname to be the name of your schema and user to be the owner of said database schema
    conn = ps.connect(dbname="tuomas", user="tuomas")
    cur = conn.cursor()

    ## Picture nr.1 scatter

    cur.execute("SELECT t.track_name, l.track_lenght, p.popularity FROM public.tracks t JOIN public.lengths l ON t.length_id = l.length_id JOIN public.popularity p ON t.popularity_id = p.popularity_id;")
    name_lenght_popularity = cur.fetchall()
    df = pd.DataFrame(name_lenght_popularity)
    
    df = pd.DataFrame(name_lenght_popularity, columns=['track_name', 'track_length_seconds', 'popularity'])
    
    df['track_length_seconds'] = (df['track_length_seconds'] / 1000).round()

    plt.figure(figsize=(10, 6))
    scatter = sns.scatterplot(x='track_length_seconds', y='popularity', data=df)

    sns.regplot(x='track_length_seconds', y='popularity', data=df, scatter=False, color='red')

    plt.title('Track Popularity vs. Track Length')
    plt.xlabel('Track Length (seconds)')
    plt.ylabel('Popularity')

    plt.savefig('pic_1.png')

    ## Picture nr.2 listening activity by time

    cur.execute("""
    SELECT hour, weekday FROM listening_date;
    """)
    
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['hour', 'weekday'])

    grouped_df = df.groupby(['hour', 'weekday']).size().reset_index(name='count')

    pivot_df = grouped_df.pivot(index='weekday', columns='hour', values='count')

    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    pivot_df = pivot_df.reindex(weekday_order)

    plt.figure(figsize=(12, 6))
    sns.heatmap(pivot_df, cmap='Blues', annot=True)

    plt.title('Listening Activity by Time of Day and Weekday')
    plt.xlabel('Hour of Day')
    plt.ylabel('Day of Week')

    plt.savefig('pic_2.png')

    ## Picture nr.3 bar chart of most frequently played tracks

    cur.execute("""
    SELECT track_name FROM tracks;
    """)

    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['track_name'])

    track_counts = df['track_name'].value_counts().reset_index()
    track_counts.columns = ['track_name', 'number_of_plays']

    track_counts = track_counts.head(15)

    plt.figure(figsize=(12, 6))
    sns.barplot(x='track_name', y='number_of_plays', data=track_counts, palette='viridis')

    plt.title('15 Most Played Tracks')
    plt.xlabel('Track Name')
    plt.ylabel('Number of Plays')
    
    plt.xticks(rotation=45, ha='right')

    plt.tight_layout()

    plt.savefig('pic_3.png')

    ## Picture nr.4 histogram of release year distribution

    cur.execute("""
    SELECT release_date FROM album;
    """)

    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['release_date'])

    df['release_year'] = pd.to_datetime(df['release_date']).dt.year

    plt.figure(figsize=(10, 6))
    sns.histplot(df['release_year'], bins=20, kde=False, color='blue')

    # Customize the plot
    plt.title('Distribution of Track Release Years')
    plt.xlabel('Release Year')
    plt.ylabel('Number of Tracks')

    plt.tight_layout()

    plt.savefig('pic_4.png')

    ## Picture nr.5 Track Length Distribution
    cur.execute("SELECT track_lenght FROM lengths;")

    data = cur.fetchall()

    df = pd.DataFrame(data, columns=['track_length'])
    df['track_length'] = (df['track_length'] / 1000).round()

    plt.figure(figsize=(10, 6))
    sns.histplot(df['track_length'], bins=20, kde=True, color='green')
    plt.title('Track Length Distribution')
    plt.xlabel('Track Length (Seconds)')
    plt.ylabel('Frequency')

    plt.savefig('pic_5.png')



    ## Picture nr.6 histogram hour plot
    cur.execute("""
    SELECT hour, weekday FROM listening_date;
    """)
    
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['hour', 'weekday'])

    weekday_count = df['weekday'].value_counts().reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])

    plt.figure(figsize=(8, 6))
    sns.barplot(x=weekday_count.index, y=weekday_count.values, palette='Blues_d')

    plt.title('Tracks Played by Weekday', fontsize=16)
    plt.xlabel('Day of the Week', fontsize=14)
    plt.ylabel('Number of Tracks Played', fontsize=14)

    plt.savefig('pic_6.png')

    ## Picture nr.7 Weekly listening trends

    cur.execute("""
    WITH WeeklyListens AS (
        SELECT
            DATE_TRUNC('week', CAST(ld.dates AS TIMESTAMP)) AS week_start,  -- Cast text to TIMESTAMP
            COUNT(t.track_id) AS listens_count
        FROM
            public.tracks t
        JOIN
            public.listening_date ld ON t.date_id = ld.date_id
        GROUP BY
            week_start
    ),
    AverageWeeklyListens AS (
        SELECT
            AVG(listens_count) AS average_weekly_listens
        FROM
            WeeklyListens
    ),
    WeeklyTrends AS (
        SELECT
            wl.week_start,
            wl.listens_count,
            awl.average_weekly_listens
        FROM
            WeeklyListens wl
        CROSS JOIN
            AverageWeeklyListens awl
    )
    SELECT
        week_start,
        listens_count,
        average_weekly_listens,
        CASE
            WHEN listens_count > average_weekly_listens THEN 'Above Average'
            ELSE 'Below Average'
        END AS trend_comparison
    FROM
        WeeklyTrends
    ORDER BY
        week_start DESC;
    """)

    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['week_start', 'listens_count', 'average_weekly_listens', 'trend_comparison'])

    df['average_weekly_listens'] = pd.to_numeric(df['average_weekly_listens'])

    # Plotting
    plt.figure(figsize=(10, 6))

    # Plot listens_count
    plt.bar(df['week_start'] - pd.DateOffset(days=1), df['listens_count'], width=0.4, label='Listens Count', color='skyblue', align='center')

    # Plot average_weekly_listens as a horizontal line
    plt.axhline(y=df['average_weekly_listens'].iloc[0], color='red', linestyle='--', label='Average Weekly Listens')

    # Add labels and title
    plt.xlabel('Week Start')
    plt.ylabel('Number of Listens')
    plt.title('Weekly Listening Trends')
    plt.legend()

    df['week_start'] = pd.to_datetime(df['week_start'])

    # Format x-axis to show dates properly
    plt.xticks(df['week_start'], df['week_start'].dt.strftime('%Y-%m-%d'), rotation=45)

    # Add grid
    plt.grid(axis='y')

    # Show plot
    plt.tight_layout()
    plt.savefig('pic_7.png')  

    ## Picture nr.8 Personal most popular songs listened during the weekend
    cur.execute("""
    SELECT
        t.track_id,
        t.track_name,
        a.album_name,
        ar.artist,
        ld.dates,
        ld.weekend,
        p.popularity
    FROM
        public.tracks t
    JOIN
        public.listening_date ld ON t.date_id = ld.date_id
    JOIN
        public.album a ON t.album_id = a.album_id
    JOIN
        public.artists ar ON t.artist_id = ar.artist_id
    JOIN
        public.popularity p ON t.popularity_id = p.popularity_id
    WHERE
        ld.weekend = 'Yes'
    ORDER BY
        p.popularity DESC;
    """)
    
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['track_id', 'track_name', 'album_name', 'artist', 'dates', 'weekend', 'popularity'])

    df = df.sort_values(by='popularity', ascending=False)

    plt.figure(figsize=(12, 8))

    # Plot the track names against their popularity as a bar chart
    plt.barh(df['track_name'], df['popularity'], color='skyblue')

    # Add labels and title
    plt.xlabel('Popularity')
    plt.ylabel('Track Name')
    plt.title('Personal Most Popular Songs Listened During Weekends')

    # Invert the y-axis to have the most popular songs on top
    plt.gca().invert_yaxis()

    # Display the plot
    plt.tight_layout()
    plt.savefig('pic_8.png') 

    ## Picture nr.9 Relationship Between Artist Popularity and Play Frequency
    cur.execute("""
        SELECT
            a.artist AS artist_name,
            COUNT(t.track_id) AS play_count,
            p.popularity AS artist_popularity
        FROM
            public.tracks t
        JOIN
            public.artists a ON t.artist_id = a.artist_id
        JOIN
            public.popularity p ON t.popularity_id = p.popularity_id
        GROUP BY
            a.artist, p.popularity
        ORDER BY
            play_count DESC;
    """)

    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['artist_name', 'play_count', 'artist_popularity'])

    df['artist_category'] = df['artist_popularity'].apply(lambda x: 'Lesser-Known' if x < 50 else 'Well-Known')

    plt.figure(figsize=(12, 8))
    scatter = sns.scatterplot(data=df, x='artist_popularity', y='play_count', hue='artist_category', palette='coolwarm')

    sns.regplot(data=df, x='artist_popularity', y='play_count', scatter=False, color='gray')

    plt.title('Relationship Between Artist Popularity and Play Frequency')
    plt.xlabel('Artist Popularity')
    plt.ylabel('Play Count')
    plt.legend(title='Artist Category')
    plt.tight_layout()

    plt.savefig('pic_9.png')

    ## Picture nr.10 15 Least played artists

    cur.execute("""
    SELECT a.artist, COUNT(t.track_id) AS play_count
    FROM 
        public.tracks t
    JOIN 
        public.artists a ON t.artist_id = a.artist_id
    GROUP BY 
        a.artist
    ORDER BY 
        play_count ASC;
    """)

    data = cur.fetchall()

    df = pd.DataFrame(data, columns=['artist','play_count'])
    df = df[df['artist'].str.len() > 2]

    df['artist'] = df['artist'].apply(eval)

    df_expanded = df.explode('artist').reset_index(drop=True) 

    artist_play_count = df_expanded.groupby('artist', as_index=False)['play_count'].sum()
    
    artist_play_count = artist_play_count.sort_values(by='play_count', ascending=False).reset_index(drop=True)

    least_played_artists = artist_play_count.tail(15)

    fig, ax = plt.subplots(figsize=(6, 4))

    ax.axis('off')

    for i, artist in enumerate(least_played_artists['artist']):
        ax.text(0.5, 1 - i * 0.2, artist, fontsize=15, ha='center', va='center')

    plt.title('Your 15 Least Played Artists', fontsize=18, pad=40)

    plt.savefig('pic_10.png', bbox_inches='tight')

    ## Picture nro.11 most listened albums
    cur.execute("""
    SELECT 
    a.album_name,
        COUNT(t.track_id) AS listen_count
    FROM 
        public.tracks t
    JOIN 
        public.album a ON t.album_id = a.album_id
    GROUP BY 
        a.album_name
    ORDER BY 
        listen_count DESC;
    """)

    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['album_name','listen_count'])
    df = df.head(10)

    plt.figure(figsize=(10, 6))  
    plt.barh(df['album_name'], df['listen_count'], color='skyblue')

    plt.xlabel('Number of Listens')
    plt.ylabel('Album Name')
    plt.title('Most Listened Albums')

    plt.gca().invert_yaxis()

    plt.tight_layout()
    plt.savefig('pic_11.png', bbox_inches='tight')


    ## Picture nro.12 weekly most listened albums
    cur.execute("""
        WITH WeeklyListenCounts AS (
            SELECT 
                a.album_name,
                d.week_of_year,
                d.year,
                COUNT(t.track_id) AS listen_count
            FROM 
                public.tracks t
            JOIN 
                public.album a ON t.album_id = a.album_id
            JOIN 
                public.listening_date d ON t.date_id = d.date_id
            WHERE 
                d.year = EXTRACT(YEAR FROM CURRENT_DATE)
            GROUP BY 
                a.album_name, d.week_of_year, d.year
        ),
        MaxListenCounts AS (
            SELECT
                week_of_year,
                year,
                MAX(listen_count) AS max_listen_count
            FROM
                WeeklyListenCounts
            GROUP BY
                week_of_year, year
        )
        SELECT
            w.album_name,
            w.week_of_year,
            w.year,
            w.listen_count
        FROM
            WeeklyListenCounts w
        JOIN
            MaxListenCounts m
        ON
            w.week_of_year = m.week_of_year
            AND w.year = m.year
            AND w.listen_count = m.max_listen_count
        ORDER BY
            w.week_of_year, w.year;
    """)

    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['album_name', 'week_of_year', 'year', 'listen_count'])

    plt.figure(figsize=(12, 8))

    # Create the barplot
    sns.barplot(x='week_of_year', y='listen_count', hue='album_name', data=df, palette='coolwarm')

    plt.xlabel('Week Number')
    plt.ylabel('Number of Listens')
    plt.title('Most Listened Albums per Week')

    plt.tight_layout()

    plt.show()
    plt.savefig('pic_12.png', bbox_inches='tight')

    ## Picture nro.13 Personal most popular albums based on total track popularity score
    cur.execute("""
        SELECT 
            a.album_name,
            SUM(p.popularity) AS total_popularity
        FROM 
            public.tracks t
        JOIN 
            public.album a ON t.album_id = a.album_id
        JOIN 
            public.listening_date d ON t.date_id = d.date_id
        JOIN 
            public.popularity p ON t.popularity_id = p.popularity_id
        WHERE 
            d.week_of_year = EXTRACT(WEEK FROM CURRENT_DATE)
            AND d.year = EXTRACT(YEAR FROM CURRENT_DATE)
        GROUP BY 
            a.album_name
        ORDER BY 
            total_popularity DESC
        LIMIT 10;
    """)

    # Fetch the data
    data = cur.fetchall()

    # Create a DataFrame
    df = pd.DataFrame(data, columns=['album_name', 'total_popularity'])

    # Plot the data
    plt.figure(figsize=(10, 6))

    # Create the barplot
    sns.barplot(x='total_popularity', y='album_name', data=df, palette='coolwarm')

    plt.xlabel('Total Popularity Score (This Week)')
    plt.ylabel('Album Name')
    plt.title('Top 10 Most Popular Albums This Week (Based on Total Popularity Score)')

    plt.tight_layout()

    # Show the plot
    plt.show()

    # Save the plot
    plt.savefig('pic_13.png', bbox_inches='tight')


    ## Picture nro.14 Least listened albums this month
    cur.execute("""
        SELECT 
            a.album_name,
            COUNT(t.track_id) AS listen_count
        FROM 
            public.tracks t
        JOIN 
            public.album a ON t.album_id = a.album_id
        JOIN 
            public.listening_date d ON t.date_id = d.date_id
        WHERE 
            d.month_num = EXTRACT(MONTH FROM CURRENT_DATE)
            AND d.year = EXTRACT(YEAR FROM CURRENT_DATE)
        GROUP BY 
            a.album_name
        ORDER BY 
            listen_count ASC
        LIMIT 10;
    """)

    # Fetch the data
    data = cur.fetchall()

    # Create a DataFrame
    df = pd.DataFrame(data, columns=['album_name', 'listen_count'])

    # Plot the data
    plt.figure(figsize=(10, 6))

    # Create the barplot
    sns.barplot(x='listen_count', y='album_name', data=df, palette='coolwarm')

    plt.xlabel('Number of Listens (This Month)')
    plt.ylabel('Album Name')
    plt.title('Top 10 Least Listened Albums This Month')

    plt.tight_layout()

    # Show the plot
    plt.show()

    # Save the plot
    plt.savefig('pic_14.png', bbox_inches='tight')
    
    ## Picture nro.15 Least listened artists with more than 10 listenings

    cur.execute("""
    WITH TotalListens AS (
            SELECT 
                a.artist_id,
                COUNT(t.track_id) AS total_listens
            FROM 
                public.tracks t
            JOIN 
                public.artists a ON t.artist_id = a.artist_id
            JOIN 
                public.listening_date d ON t.date_id = d.date_id
            WHERE 
                d.year = EXTRACT(YEAR FROM CURRENT_DATE)
            GROUP BY 
                a.artist_id
            HAVING 
                COUNT(t.track_id) > 5
        ),
        MonthlyListens AS (
            SELECT 
                a.artist_id,
                a.artist,
                COUNT(t.track_id) AS monthly_listens
            FROM 
                public.tracks t
            JOIN 
                public.artists a ON t.artist_id = a.artist_id
            JOIN 
                public.listening_date d ON t.date_id = d.date_id
            WHERE 
                d.month_num = EXTRACT(MONTH FROM CURRENT_DATE)
                AND d.year = EXTRACT(YEAR FROM CURRENT_DATE)
            GROUP BY 
                a.artist_id, a.artist
        )
        SELECT 
            m.artist,
            COALESCE(m.monthly_listens, 0) AS monthly_listens
        FROM 
            MonthlyListens m
        JOIN 
            TotalListens t ON m.artist_id = t.artist_id
        ORDER BY 
            monthly_listens ASC
        LIMIT 15;
    """)

    # Fetch the data
    data = cur.fetchall()

    # Create a DataFrame
    df = pd.DataFrame(data, columns=['artist', 'monthly_listens'])

    # Plot the data
    plt.figure(figsize=(12, 8))

    # Create the barplot
    sns.barplot(x='monthly_listens', y='artist', data=df, palette='coolwarm')

    plt.xlabel('Number of Listens (This Month)')
    plt.ylabel('Artist')
    plt.title('15 Least Listened Artists This Month (With More Than 5 Total Listenings)')

    plt.tight_layout()

    # Show the plot
    plt.show()

    # Save the plot
    plt.savefig('pic_15.png', bbox_inches='tight')
    

    cur.close()
    conn.close()

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



