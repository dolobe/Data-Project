import os
import requests
import pymongo
import pandas as pd
import time
from dotenv import load_dotenv
from utilities.connexion import fetch_artists_dataframe
load_dotenv()

CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
REDIRECT_URI = os.getenv('SPOTIFY_REDIRECT_URI')
MONGO_URI = os.getenv('MONGO_URI')


class GetAPI:
    """
    Classe pour gérer la connexion à l'API Spotify, obtenir un token d'accès et effectuer des requêtes.
    """
    
    def __init__(self, client_id, client_secret, redirect_uri):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.token = None

    def get_access_token(self):
        """
        Récupère un access token pour l'API Spotify.
        """
        auth_url = 'https://accounts.spotify.com/api/token'
        response = requests.post(auth_url, data={
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        })

        if response.status_code == 200:
            self.token = response.json()['access_token']
            print("Access token successfully obtained.")
        else:
            print(f"Failed to get access token. Status Code: {response.status_code}")
        return self.token


class GetArtist(GetAPI):
    """
    Classe pour rechercher un artiste, récupérer ses informations, albums et pistes.
    Hérite de la classe GetAPI pour la gestion du token et des requêtes API.
    """

    def search_artist(self, artist_name):
        """
        Recherche un artiste sur Spotify par son nom.
        """
        if not self.token:
            print("No access token found. Please retrieve a token first.")
            return None

        search_url = 'https://api.spotify.com/v1/search'
        headers = {'Authorization': f'Bearer {self.token}'}
        params = {'q': artist_name, 'type': 'artist', 'limit': 1}

        response = requests.get(search_url, headers=headers, params=params)
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

    def get_artist_info(self, artist_id):
        """
        Récupère des informations sur un artiste via son ID.
        """
        if not self.token:
            print("No access token found. Please retrieve a token first.")
            return None

        artist_url = f'https://api.spotify.com/v1/artists/{artist_id}'
        headers = {'Authorization': f'Bearer {self.token}'}
        response = requests.get(artist_url, headers=headers)
        return response.json()

    def get_artist_albums(self, artist_id):
        """
        Récupère les albums d'un artiste via son ID.
        """
        if not self.token:
            print("No access token found. Please retrieve a token first.")
            return None

        albums_url = f'https://api.spotify.com/v1/artists/{artist_id}/albums'
        headers = {'Authorization': f'Bearer {self.token}'}
        response = requests.get(albums_url, headers=headers)
        return response.json()

    def get_album_tracks(self, album_id):
        """
        Récupère les pistes d'un album via son ID.
        """
        if not self.token:
            print("No access token found. Please retrieve a token first.")
            return None

        album_tracks_url = f'https://api.spotify.com/v1/albums/{album_id}/tracks'
        headers = {'Authorization': f'Bearer {self.token}'}
        response = requests.get(album_tracks_url, headers=headers)
        return response.json()


class InsertDataToMongoDB:
    """
    Classe pour gérer l'insertion des données dans la base MongoDB.
    """

    def __init__(self, mongo_uri):
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client['Spotify_db_ml']
        self.collection = self.db['Artists_db']
        print("Connected to MongoDB successfully.")

    def insert_data_into_mongo(self, data):
        """
        Insère les données d'un artiste dans MongoDB.
        """
        if self.collection.find_one({'artist_id': data['artist_id']}):
            print(f"Artist {data['name']} already exists in the database. Skipping insertion.")
        else:
            self.collection.insert_one(data)
            print(f"Artist {data['name']} inserted into MongoDB.")


class RetrieveArtistsInfoFromCSV:
    """
    Classe pour récupérer les informations des artistes depuis un fichier CSV et les insérer dans MongoDB.
    """

    def __init__(self, csv_file, mongo_db, api_handler):
        self.csv_file = csv_file
        self.mongo_db = mongo_db
        self.api_handler = api_handler

    def retrieve_artists_info_from_csv(self, limit=300):
        """
        Récupère les informations des artistes à partir du fichier CSV et les insère dans MongoDB.
        """
        df = pd.read_csv(self.csv_file)
        artist_names = df['artist_name'].tolist()

        count = 0
        for artist_name in artist_names:
            if count >= limit:
                break

            artist_id = self.api_handler.search_artist(artist_name)
            if artist_id:
                artist_info = self.api_handler.get_artist_info(artist_id)
                albums = self.api_handler.get_artist_albums(artist_id)

                artist_data = {
                    'artist_id': artist_info['id'],
                    'name': artist_info['name'],
                    'genres': artist_info['genres'],
                    'popularity': artist_info['popularity'],
                    'followers': artist_info['followers']['total'],
                    'albums': []
                }

                for album in albums['items']:
                    album_data = {
                        'album_id': album['id'],
                        'album_name': album['name'],
                        'release_date': album['release_date'],
                        'tracks': []
                    }

                    tracks = self.api_handler.get_album_tracks(album['id'])
                    for track in tracks['items']:
                        album_data['tracks'].append({
                            'track_id': track['id'],
                            'track_name': track['name'],
                            'duration_ms': track['duration_ms'],
                            'preview_url': track['preview_url']
                        })

                    artist_data['albums'].append(album_data)

                self.mongo_db.insert_data_into_mongo(artist_data)
                print(f"The data for artist {artist_name} has been inserted into MongoDB.")
                count += 1


def process_multiple_csv_files(base_path, api_handler, mongo_db, limit_per_hour=300):
    """
    Traite plusieurs fichiers CSV pour récupérer les informations des artistes et les insérer dans MongoDB.
    """
    file_index = 2
    while True:
        csv_file = f"{base_path}/artists_chunk_{file_index}.csv"
        if not os.path.exists(csv_file):
            print(f"File {csv_file} not found. Stopping the process.")
            break

        print(f"Processing file: {csv_file}")
        artist_info_retriever = RetrieveArtistsInfoFromCSV(csv_file, mongo_db, api_handler)
        artist_info_retriever.retrieve_artists_info_from_csv(limit=limit_per_hour)

        print(f"Pausing for 1 hour to respect API rate limits...")
        time.sleep(3600)
        file_index += 1


import os
import requests
import pymongo
import pandas as pd
import time
from dotenv import load_dotenv
from utilities.connexion import fetch_artists_dataframe

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Variables d'environnement pour Spotify API et MongoDB
CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
REDIRECT_URI = os.getenv('SPOTIFY_REDIRECT_URI')
MONGO_URI = os.getenv('MONGO_URI')


class GetAPI:
    """
    Classe pour gérer la connexion à l'API Spotify, obtenir un token d'accès et effectuer des requêtes.
    """
    
    def __init__(self, client_id, client_secret, redirect_uri):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.token = None

    def get_access_token(self):
        """
        Récupère un access token pour l'API Spotify.
        """
        auth_url = 'https://accounts.spotify.com/api/token'
        response = requests.post(auth_url, data={
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        })

        if response.status_code == 200:
            self.token = response.json()['access_token']
            print("Access token successfully obtained.")
        else:
            print(f"Failed to get access token. Status Code: {response.status_code}")
        return self.token


class GetArtist(GetAPI):
    """
    Classe pour rechercher un artiste, récupérer ses informations, albums et pistes.
    Hérite de la classe GetAPI pour la gestion du token et des requêtes API.
    """

    def search_artist(self, artist_name):
        """
        Recherche un artiste sur Spotify par son nom.
        """
        if not self.token:
            print("No access token found. Please retrieve a token first.")
            return None

        search_url = 'https://api.spotify.com/v1/search'
        headers = {'Authorization': f'Bearer {self.token}'}
        params = {'q': artist_name, 'type': 'artist', 'limit': 1}

        response = requests.get(search_url, headers=headers, params=params)
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

    def get_artist_info(self, artist_id):
        """
        Récupère des informations sur un artiste via son ID.
        """
        if not self.token:
            print("No access token found. Please retrieve a token first.")
            return None

        artist_url = f'https://api.spotify.com/v1/artists/{artist_id}'
        headers = {'Authorization': f'Bearer {self.token}'}
        response = requests.get(artist_url, headers=headers)
        return response.json()

    def get_artist_albums(self, artist_id):
        """
        Récupère les albums d'un artiste via son ID.
        """
        if not self.token:
            print("No access token found. Please retrieve a token first.")
            return None

        albums_url = f'https://api.spotify.com/v1/artists/{artist_id}/albums'
        headers = {'Authorization': f'Bearer {self.token}'}
        response = requests.get(albums_url, headers=headers)
        return response.json()

    def get_album_tracks(self, album_id):
        """
        Récupère les pistes d'un album via son ID.
        """
        if not self.token:
            print("No access token found. Please retrieve a token first.")
            return None

        album_tracks_url = f'https://api.spotify.com/v1/albums/{album_id}/tracks'
        headers = {'Authorization': f'Bearer {self.token}'}
        response = requests.get(album_tracks_url, headers=headers)
        return response.json()


class InsertDataToMongoDB:
    """
    Classe pour gérer l'insertion des données dans la base MongoDB.
    """

    def __init__(self, mongo_uri):
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client['Spotify_db_ml']
        self.collection = self.db['Artists_db']
        print("Connected to MongoDB successfully.")

    def insert_data_into_mongo(self, data):
        """
        Insère les données d'un artiste dans MongoDB.
        """
        if self.collection.find_one({'artist_id': data['artist_id']}):
            print(f"Artist {data['name']} already exists in the database. Skipping insertion.")
        else:
            self.collection.insert_one(data)
            print(f"Artist {data['name']} inserted into MongoDB.")


class RetrieveArtistsInfoFromCSV:
    """
    Classe pour récupérer les informations des artistes depuis un fichier CSV et les insérer dans MongoDB.
    """

    def __init__(self, csv_file, mongo_db, api_handler):
        self.csv_file = csv_file
        self.mongo_db = mongo_db
        self.api_handler = api_handler

    def retrieve_artists_info_from_csv(self, limit=300):
        """
        Récupère les informations des artistes à partir du fichier CSV et les insère dans MongoDB.
        """
        df = pd.read_csv(self.csv_file)
        artist_names = df['artist_name'].tolist()

        count = 0
        for artist_name in artist_names:
            if count >= limit:
                break

            artist_id = self.api_handler.search_artist(artist_name)
            if artist_id:
                artist_info = self.api_handler.get_artist_info(artist_id)
                albums = self.api_handler.get_artist_albums(artist_id)

                artist_data = {
                    'artist_id': artist_info['id'],
                    'name': artist_info['name'],
                    'genres': artist_info['genres'],
                    'popularity': artist_info['popularity'],
                    'followers': artist_info['followers']['total'],
                    'albums': []
                }

                for album in albums['items']:
                    album_data = {
                        'album_id': album['id'],
                        'album_name': album['name'],
                        'release_date': album['release_date'],
                        'tracks': []
                    }

                    tracks = self.api_handler.get_album_tracks(album['id'])
                    for track in tracks['items']:
                        album_data['tracks'].append({
                            'track_id': track['id'],
                            'track_name': track['name'],
                            'duration_ms': track['duration_ms'],
                            'preview_url': track['preview_url']
                        })

                    artist_data['albums'].append(album_data)

                self.mongo_db.insert_data_into_mongo(artist_data)
                print(f"The data for artist {artist_name} has been inserted into MongoDB.")
                count += 1


def process_multiple_csv_files(base_path, api_handler, mongo_db, limit_per_hour=300):
    """
    Traite plusieurs fichiers CSV pour récupérer les informations des artistes et les insérer dans MongoDB.
    """
    file_index = 2
    while True:
        csv_file = f"{base_path}/artists_chunk_{file_index}.csv"
        if not os.path.exists(csv_file):
            print(f"File {csv_file} not found. Stopping the process.")
            break

        print(f"Processing file: {csv_file}")
        artist_info_retriever = RetrieveArtistsInfoFromCSV(csv_file, mongo_db, api_handler)
        artist_info_retriever.retrieve_artists_info_from_csv(limit=limit_per_hour)

        print(f"Pausing for 1 hour to respect API rate limits...")
        time.sleep(3600)
        file_index += 1


if __name__ == '__main__':
    api_handler = GetArtist(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI)
    api_handler.get_access_token()

    mongo_db = InsertDataToMongoDB(MONGO_URI)

    base_path = "split_artists"
    process_multiple_csv_files(base_path, api_handler, mongo_db)
