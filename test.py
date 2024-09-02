import requests
import logging
import base64
import pandas as pd
import json
import os
from urllib.parse import urlencode, urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, HTTPServer

# Set up logging
logging.basicConfig(level=logging.INFO)

# Spotify API credentials
client_id = '7ac4100bc0f84e978f1b4c8e4b74576b'
client_secret = '62fed3a94a224ef48272a7b3d8ea0583'
redirect_uri = 'http://localhost:8888/callback'
scope = 'user-read-recently-played'

# Step 1: Get the authorization URL to authenticate the user
def get_auth_url():
    auth_url = 'https://accounts.spotify.com/authorize?' + urlencode({
        'response_type': 'code',
        'client_id': client_id,
        'scope': scope,
        'redirect_uri': redirect_uri,  # Correctly using redirect_uri variable
    })
    return auth_url

# Step 2: Exchange the authorization code for an access token
def get_access_token(auth_code):
    token_url = "https://accounts.spotify.com/api/token"
    headers = {
        'Authorization': f'Basic {base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'authorization_code',
        'code': auth_code,
        'redirect_uri': redirect_uri,  # Correctly using redirect_uri variable
    }

    response = requests.post(token_url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()

# Step 3: Fetch recently played tracks using the access token
def get_recently_played_tracks(access_token, limit=50):
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    params = {
        'limit': limit
    }

    response = requests.get('https://api.spotify.com/v1/me/player/recently-played', headers=headers, params=params)
    response.raise_for_status()
    return response.json()

# Step 4: Save the raw JSON and CSV files
def save_raw_data(data):
    # Save raw JSON
    with open('recently_played_tracks_raw.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logging.info("Raw data saved to recently_played_tracks_raw.json")

    # Convert JSON to DataFrame and save to CSV
    df = pd.json_normalize(data['items'])  # Flatten the JSON structure to create a table
    df.to_csv('recently_played_tracks_raw.csv', index=False)
    logging.info("Raw data saved to recently_played_tracks_raw.csv")

# Step 5: Handle the callback and extract the authorization code
class OAuthCallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        query_components = parse_qs(urlparse(self.path).query)
        if 'code' in query_components:
            auth_code = query_components['code'][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'Authorization successful! You can close this window.')
            
            # Exchange the authorization code for an access token
            token_info = get_access_token(auth_code)
            access_token = token_info['access_token']
            
            # Fetch recently played tracks
            data = get_recently_played_tracks(access_token, limit=50)
            
            # Save the data to JSON and CSV
            save_raw_data(data)
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'Authorization code not found in the callback URL.')

# Step 6: Start the server to handle the callback
def run_server(server_class=HTTPServer, handler_class=OAuthCallbackHandler, port=8888):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting server on port {port}...')
    httpd.serve_forever()

# Step 7: Main function to start the flow
def main():
    print("Please visit the following URL to authorize the app:")
    print(get_auth_url())

    # Start the local server to handle the callback
    run_server()

if __name__ == "__main__":
    main()

    