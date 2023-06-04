from flask import Flask, request, render_template
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer, KafkaProducer
from flask_cors import CORS
import eventlet

eventlet.monkey_patch()

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # set CORS for all routes
socketio = SocketIO(app, async_mode='eventlet', cors_allowed_origins="*", logger=True, engineio_logger=True)
my_kafka_server = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=my_kafka_server, batch_size=16384)
topic = "Hello"

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/send_message', methods=['POST'])
def send_message():
    message = request.json.get('message')
    message_bytes = message.encode('utf-8')
    producer.send(topic, message_bytes)
    return {'status': 'message sent'}

def kafka_consumer():
    print("kafka_consumer function started")
    consumer = KafkaConsumer(topic, group_id='my-group', bootstrap_servers=my_kafka_server)
    for message in consumer:
        print(f"Received message from Kafka: {message.value.decode()}")
        socketio.emit('new_message', {'message': message.value.decode()}, namespace='/')



if __name__ == '__main__':
    socketio.start_background_task(kafka_consumer)
    socketio.run(app, debug=True)
