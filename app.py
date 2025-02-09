from flask import Flask, render_template, jsonify, request
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

# PostgreSQL
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://kafka_user:password@localhost/web_traffic'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Database Model
class PageTraffic(db.Model):
    __tablename__ = 'page_traffic'
    id = db.Column(db.Integer, primary_key=True)
    page = db.Column(db.String(255), unique=True, nullable=False)
    total_visits = db.Column(db.Integer, nullable=False)
    unique_users = db.Column(db.Integer, nullable=False)
    last_updated = db.Column(db.TIMESTAMP, nullable=False)

latest_consumer_output= []

@app.route('/data')
def get_data():
    data = PageTraffic.query.all()
    traffic_data = [{"page": d.page, "total_visits": d.total_visits, "unique_users": d.unique_users} for d in data]
    return jsonify(traffic_data)

@app.route('/consumer_output', methods=['GET'])
def get_consumer_output():
    global latest_consumer_output
    return jsonify(latest_consumer_output)
@app.route('/consumer_output', methods=['POST'])
def update_consumer_output():
    global latest_consumer_output
    data = request.json

    latest_consumer_output = data
    return jsonify({"status": "success", "message": "Consumer data updated!"})

# Dashboard
@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

