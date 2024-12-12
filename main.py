import os
import requests
import pymongo
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
        print(f"Artiste {artist_name} non trouvé.")
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

def retrieve_artists_info(artist_names):
    access_token = get_access_token()

    for artist_name in artist_names:
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
            print(f"Les données de l'artiste {artist_name} ont été insérées dans MongoDB.")

artist_names = ['Adele', 'Drake', 'Taylor Swift', 'Ed Sheeran', 'Jul']

retrieve_artists_info(artist_names)
