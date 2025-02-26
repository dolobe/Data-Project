import pandas as pd
from pymongo import MongoClient
import sys
import os

mongo_url = "mongodb+srv://GroupeYboost:Yboost123456@dataspotifyml.j8nw7.mongodb.net/"
db_name = "Spotify_db_ml"
collection_name = "Artists_db"

def fetch_artists_dataframe():
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[collection_name]
    data = list(collection.find())
    df = pd.DataFrame(data)
    return df

def add_utilities_path():
    utilities_path = os.path.abspath(os.path.dirname(__file__))
    if utilities_path not in sys.path:
        sys.path.append(utilities_path)
