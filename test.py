import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Spotify API credentials
client_id = '7ac4100bc0f84e978f1b4c8e4b74576b'
client_secret = '62fed3a94a224ef48272a7b3d8ea0583'
redirect_uri = 'http://localhost:8888/callback'
scope = 'user-read-recently-played'

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


    