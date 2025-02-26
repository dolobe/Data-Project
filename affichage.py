from flask import Flask, jsonify
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration MongoDB
MONGO_URI = os.getenv('MONGO_URI')
client = MongoClient(MONGO_URI)
db = client['spotify_db']
collection = db['artists']

# Créer l'application Flask
app = Flask(__name__)

# Route pour la page d'accueil
@app.route('/', methods=['GET'])
def home():
    """
    Page d'accueil simple.
    """
    return '''
    <h1>Bienvenue sur l'API Spotify</h1>
    <p>Utilisez <a href="/artists">/artists</a> pour voir les données des artistes.</p>
    '''

@app.route('/artists', methods=['GET'])
def get_artists():
    """
    Récupère et affiche les artistes depuis MongoDB.
    """
    try:
        artists = list(collection.find({}, {'_id': 0}))
        if not artists:
            return jsonify({'message': 'Aucun artiste trouvé dans la base de données.'}), 404
        
        return jsonify(artists), 200
    except Exception as e:
        return jsonify({'error': f'Une erreur est survenue : {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
