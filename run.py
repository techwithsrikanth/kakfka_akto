from flask import Flask, request
from kafka import KafkaProducer
import json

app = Flask(__name__)
KAFKA_BROKER_IP = 'localhost:9092'
KAFKA_TOPIC = 'test2'

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_IP,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

@app.route('/', methods=['GET', 'POST'])
def log_request():
    if request.method == 'POST':
        # Log request data
        request_data = {
            'method': request.method,
            'path': request.path,
            'headers': dict(request.headers),
            'body': request.get_data(as_text=True)
        }

        try:
            producer.send(KAFKA_TOPIC, value=request_data)  
            print("Data sent to Kafka:", request_data) 
        except Exception as e:
            print("Failed to send:", e) 

    return 'OK'

if __name__ == "__main__":
    app.run(debug=True)
