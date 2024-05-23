import pymongo
import matplotlib.pyplot as plt
from flask import Flask, send_from_directory
import os

app = Flask(__name__)

IMAGE_FOLDER = os.path.join('static', 'images')
os.makedirs(IMAGE_FOLDER, exist_ok=True)  # S'assure que le dossier existe

@app.route('/plot.png')
def plot_png():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["churn"]
    collection = db["predictions"]
    data = collection.find({}, {"Customer_service_calls": 1})
    service_calls = [item['Customer_service_calls'] for item in data]

    # Assurez-vous que des appels sont présents
    if not service_calls:
        service_calls = [0]  # Evite les erreurs si la liste est vide

    path = os.path.join(IMAGE_FOLDER, 'plot.png')

    # Création de l'histogramme
    plt.figure(figsize=(10, 5))
    plt.hist(service_calls, bins=range(int(max(service_calls)) + 2), color='purple', alpha=0.7, rwidth=0.85)
    plt.xlabel('Nombre d\'appels au service clientèle')
    plt.ylabel('Nombre de clients')
    plt.title('Distribution des appels au service clientèle')
    plt.xticks(range(int(max(service_calls)) + 1))
    plt.savefig(path)
    plt.close()

    return send_from_directory(IMAGE_FOLDER, 'plot.png', as_attachment=False)

if __name__ == '__main__':
    app.run(debug=True)
