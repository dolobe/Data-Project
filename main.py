import os
import requests
import pymongo
import pandas as pd
import time
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
REDIRECT_URI = os.getenv('SPOTIFY_REDIRECT_URI')

MONGO_URI = os.getenv('MONGO_URI')

client = pymongo.MongoClient(MONGO_URI)
db = client['spotify_db']
collection = db['artists']

AUTH_URL = 'https://accounts.spotify.com/api/token'
BASE_URL = 'https://api.spotify.com/v1/'
SEARCH_URL = 'https://api.spotify.com/v1/search'

def get_access_token():
    response = requests.post(AUTH_URL, data={
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    })
    return response.json()['access_token']

def search_artist(artist_name, access_token):
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'q': artist_name, 'type': 'artist', 'limit': 1}
    response = requests.get(SEARCH_URL, headers=headers, params=params)
    data = response.json()
    
    if 'artists' in data and 'items' in data['artists'] and len(data['artists']['items']) > 0:
        artist = data['artists']['items'][0]
        artist_id = artist['id']
        artist_name = artist['name']
        print(f"Artist ID: {artist_id}, Artist Name: {artist_name}")
        return artist_id
    else:
        print(f"Artist {artist_name} not found.")
        return None

def get_artist_info(artist_id, access_token):
    headers = {'Authorization': f'Bearer {access_token}'}
    artist_url = f'{BASE_URL}artists/{artist_id}'
    response = requests.get(artist_url, headers=headers)
    return response.json()

def get_artist_albums(artist_id, access_token):
    headers = {'Authorization': f'Bearer {access_token}'}
    albums_url = f'{BASE_URL}artists/{artist_id}/albums'
    response = requests.get(albums_url, headers=headers)
    return response.json()

def get_album_tracks(album_id, access_token):
    headers = {'Authorization': f'Bearer {access_token}'}
    album_tracks_url = f'{BASE_URL}albums/{album_id}/tracks'
    response = requests.get(album_tracks_url, headers=headers)
    return response.json()

def insert_data_into_mongo(data, collection):
    collection.insert_one(data)

def retrieve_artists_info_from_csv(csv_file, limit=300):
    df = pd.read_csv(csv_file)
    artist_names = df['artist_name'].tolist()
    access_token = get_access_token()

    count = 0
    for artist_name in artist_names:
        if count >= limit:
            break

        artist_id = search_artist(artist_name, access_token)
        if artist_id:
            artist_info = get_artist_info(artist_id, access_token)
            albums = get_artist_albums(artist_id, access_token)

            artist_data = {
                'artist_id': artist_info['id'],
                'name': artist_info['name'],
                'genres': artist_info['genres'],
                'popularity': artist_info['popularity'],
                'albums': []
            }

            for album in albums['items']:
                album_data = {
                    'album_id': album['id'],
                    'album_name': album['name'],
                    'release_date': album['release_date'],
                    'tracks': []
                }

                tracks = get_album_tracks(album['id'], access_token)
                for track in tracks['items']:
                    album_data['tracks'].append({
                        'track_id': track['id'],
                        'track_name': track['name'],
                        'duration_ms': track['duration_ms'],
                        'preview_url': track['preview_url']
                    })

                artist_data['albums'].append(album_data)

            insert_data_into_mongo(artist_data, collection)
            print(f"The data for artist {artist_name} has been inserted into MongoDB.")
            count += 1

def process_multiple_csv_files(base_path, limit_per_hour=300):
    file_index = 2
    while True:
        csv_file = f"{base_path}/artists_chunk_{file_index}.csv"
        if not os.path.exists(csv_file):
            print(f"File {csv_file} not found. Stopping the process.")
            break

        print(f"Processing file: {csv_file}")
        retrieve_artists_info_from_csv(csv_file, limit=limit_per_hour)
        
        print(f"Pausing for 1 hour to respect API rate limits...")
        time.sleep(3600)
        file_index += 1

base_path = "split_artists"
process_multiple_csv_files(base_path)
