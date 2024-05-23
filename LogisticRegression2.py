import pymongo
import matplotlib.pyplot as plt
from flask import Flask, send_from_directory
import os
app = Flask(__name__)

IMAGE_FOLDER = os.path.join('static', 'images')
os.makedirs(IMAGE_FOLDER, exist_ok=True)  # S'assure que le dossier existe

@app.route('/plot2.png')
def plot_png():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["churn"]
    collection = db["predictions"]

    data = list(collection.find({}, {"Customer_service_calls": 1, "Churn": 1}))
    churn_true = [item['Customer_service_calls'] for item in data if item['Churn']]
    churn_false = [item['Customer_service_calls'] for item in data if not item['Churn']]

    
    plt.figure(figsize=(10, 5))
    plt.hist([churn_false, churn_true], bins=range(int(max(churn_false + churn_true)) + 2), color=['green', 'red'], label=['No Churn', 'Churn'], alpha=0.7, rwidth=0.85)
    plt.xlabel('Nombre d\'appels au service clientèle')
    plt.ylabel('Nombre de clients')
    plt.title('Relation entre les appels au service clientèle et le churn')
    plt.xticks(range(int(max(churn_false + churn_true)) + 1))
    plt.legend()
    path = os.path.join(IMAGE_FOLDER, 'plot2.png')
    plt.savefig(path)
    plt.close()


   

    return send_from_directory(IMAGE_FOLDER, 'plot2.png', as_attachment=False)

if __name__ == '__main__':
    app.run(debug=True)
