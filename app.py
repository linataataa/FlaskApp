from flask import Flask, render_template
app = Flask(__name__)


@app.route('/')
#def home():
#   return "Hello World !"
def index():
    return render_template('index.html')
@app.route('/calls')
def calls():
    return render_template('calls.html')

@app.route('/clients')
def clients():
    return render_template('clients.html')

if __name__ == '__main__':
    app.run(debug=True)