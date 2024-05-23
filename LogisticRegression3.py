import pymongo
import matplotlib.pyplot as plt
from flask import Flask, send_from_directory
import os

app = Flask(__name__)
IMAGE_FOLDER = os.path.join('static', 'images')
os.makedirs(IMAGE_FOLDER, exist_ok=True)

@app.route('/average_calls.png')
def average_calls():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["churn"]
    collection = db["predictions"]

    # Pipeline d'agrégation MongoDB
    pipeline = [
        {"$group": {
            "_id": "$State",  # Grouper par État
            "average_calls": {"$avg": "$Customer_service_calls"}  # Calculer la moyenne des appels
        }},
        {"$sort": {"average_calls": 1}}  # Trier par la moyenne des appels
    ]

    avg_calls_by_state = list(collection.aggregate(pipeline))

    # Préparation des données pour le graphique
    states = [data['_id'] for data in avg_calls_by_state]
    average_calls = [data['average_calls'] for data in avg_calls_by_state]

    # Création du graphique à barres
    path = os.path.join(IMAGE_FOLDER, 'average_calls.png')
    plt.figure(figsize=(15, 7))
    plt.bar(states, average_calls, color='cyan')
    plt.xlabel('État')
    plt.ylabel('Moyenne des appels au service clientèle')
    plt.title('Moyenne des appels au service clientèle par État')
    plt.xticks(rotation=45)
    plt.savefig(path)
    plt.close()

    return send_from_directory(IMAGE_FOLDER, 'average_calls.png')

if __name__ == '__main__':
    app.run(debug=True)
