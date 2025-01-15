import requests
from pymongo import MongoClient

# 1. Récupérer un token Spotify
def get_spotify_token(client_id, client_secret):
    url = "https://accounts.spotify.com/api/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "client_credentials"}
    response = requests.post(url, headers=headers, data=data, auth=(client_id, client_secret))
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        raise Exception(f"Failed to get token: {response.text}")

# 2. Récupérer les artistes les plus populaires
def get_top_artists(token, limit=50):
    url = f"https://api.spotify.com/v1/search"
    params = {
        "q": "genre:pop",  # Exemple : filtrer par genre "pop"
        "type": "artist",
        "limit": limit
    }
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()["artists"]["items"]
    else:
        raise Exception(f"Failed to get artists: {response.text}")

# 3. Stocker les artistes dans MongoDB
def store_artists_in_mongodb(artists, collection):
    for artist in artists:
        # Préparer les données à insérer
        artist_data = {
            "id": artist["id"],
            "name": artist["name"],
            "popularity": artist["popularity"],
            "genres": artist["genres"],
            "followers": artist["followers"]["total"],
            "external_url": artist["external_urls"]["spotify"]
        }
        # Vérifier si l'artiste existe déjà dans la collection
        if not collection.find_one({"id": artist["id"]}):
            collection.insert_one(artist_data)
            print(f"Inserted: {artist['name']}")
        else:
            print(f"Already exists: {artist['name']}")

# 4. Configuration MongoDB et Spotify
def main():
    # Remplace par tes propres identifiants
    CLIENT_ID = "ff4433f7b0044319b904dd1ee3e9f08f"
    CLIENT_SECRET = "db0529ed52344a3594433d70906a7b56"
    
    # URI MongoDB Atlas (remplace par ton URI)
    MONGO_URI = "mongodb+srv://dada416:azerty@cluster0.o8o8r.mongodb.net/"
    DATABASE_NAME = "spotify_db"
    COLLECTION_NAME = "top_artists"
    
    # Obtenir un token Spotify
    token = get_spotify_token(CLIENT_ID, CLIENT_SECRET)
    
    # Obtenir les 50 artistes les plus populaires
    top_artists = get_top_artists(token, limit=50)
    
    # Connexion à MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    
    # Stocker les artistes dans MongoDB
    store_artists_in_mongodb(top_artists, collection)
    print("All artists have been processed and stored in MongoDB.")

# Lancer le script
if __name__ == "__main__":
    main()
