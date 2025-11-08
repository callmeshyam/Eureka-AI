from flask import Flask, render_template, request
from flask_socketio import SocketIO
import subprocess
import threading
from kafka import KafkaConsumer
import json

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

def kafka_listener(topic, event_name):
    consumer = KafkaConsumer(
        topic, bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    for message in consumer:
        socketio.emit(event_name, message.value)

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/start-analysis', methods=['POST'])
def start_analysis_endpoint():
    config = request.json
    subprocess.Popen(['python', 'runner.py', json.dumps(config)])
    return {"status": "Analysis started!"}

if __name__ == '__main__':
    print("🚀 Starting Dashboard Server...")
    # Start listeners in the background
    threading.Thread(target=kafka_listener, args=('dashboard_stats', 'update_stats'), daemon=True).start()
    threading.Thread(target=kafka_listener, args=('dashboard_best_result', 'update_best'), daemon=True).start()
    threading.Thread(target=kafka_listener, args=('dashboard_plot_data', 'update_plot'), daemon=True).start()
    socketio.run(app, port=5000)