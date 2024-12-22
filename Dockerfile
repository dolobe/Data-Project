# Utiliser une image Python légère
FROM python:3.9-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers nécessaires dans le conteneur
COPY requirements.txt ./
COPY main.py ./
COPY .env ./

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier le dossier contenant vos fichiers CSV
COPY split_artists ./split_artists

# Commande par défaut pour exécuter le script
CMD ["python", "main.py"]
