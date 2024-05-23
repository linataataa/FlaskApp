from flask import Flask, render_template
import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient
import seaborn as sns
import io
import base64

app = Flask(__name__)

# Initialize MongoDB client
client = MongoClient('localhost', 27017)
db = client['churn']  # Replace with your database name
collection = db['predictions']
df = pd.DataFrame(list(collection.find()))

@app.route('/')
def home():
    return render_template('index.html')


@app.route('/calls')
def calls():
    plot_urls = []

    if not df.empty:
        # Create a pie chart for churn
        churn_counts = df['International_plan'].value_counts()
        labels = churn_counts.index
        sizes = churn_counts.values

        plt.figure()  # Create a new figure
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
        plt.title('Proportion of Churned vs Non-Churned Customers')
        plt.legend(labels, loc="best", bbox_to_anchor=(1, 0.5), title="Churn", title_fontsize="14")
        img = io.BytesIO()
        plt.savefig(img, format='png')  # Save the pie chart as an image
        img.seek(0)
        pie_chart_url = base64.b64encode(img.getvalue()).decode('utf8')
        plot_urls.append(pie_chart_url)
        plt.close()
        
        # Histogram for totalDayCalls and totalDayMinutes by state
        selected_states = ['AZ', 'CA', 'NY', 'TX']
        selected_state_data = df[df['State'].isin(selected_states)]

        plt.figure(figsize=(12, 6))
        state_day_calls = selected_state_data.groupby('State')['Total_day_calls'].sum()
        state_day_minutes = selected_state_data.groupby('State')['Total_day_minutes'].sum()
       
        states = state_day_calls.index
        bar_width = 0.35
        index = range(len(states))

        plt.bar(index, state_day_calls, bar_width, label='Total Day Calls')
        plt.bar([i + bar_width for i in index], state_day_minutes, bar_width, label='Total Day Minutes')

        plt.xlabel('State')
        plt.ylabel('Calls/Minutes')
        plt.title('Total Day Calls and Total Day Minutes by State')
        plt.xticks([i + bar_width / 2 for i in index], states, rotation=90)
        plt.legend()

        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        state_calls_hist_url = base64.b64encode(img.getvalue()).decode('utf8')
        plot_urls.append(state_calls_hist_url)
        plt.close()
        
        # Create histogram for totalDayMinutes by internationalPlan
        plt.figure(figsize=(8, 4))
        sns.histplot(data=df, x='Total_day_minutes', hue='International_plan', multiple='stack', palette='viridis')
        plt.xlabel('Total Day Minutes')
        plt.ylabel('Frequency')
        plt.title('Histogram of Total Day Minutes by International Plan')
        
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        hist_url = base64.b64encode(img.getvalue()).decode('utf-8')
        plot_urls.append(hist_url)
        plt.close()
        
        # Calculate total international charge
        total_intl_charge = df['Total_intl_charge'].sum()
    else:
        total_intl_charge = 0
    
    return render_template('calls.html', plot_urls=plot_urls, total_intl_charge=total_intl_charge)


@app.route('/clients')
def clients():
    return render_template('clients.html')

if __name__ == "__main__":
    app.run(debug=True)
