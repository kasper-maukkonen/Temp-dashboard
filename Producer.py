from confluent_kafka import Producer
import time
import json
import random

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

producer_conf = {
    'bootstrap.servers': 'localhost:9092', 
}

producer = Producer(producer_conf)
topic = 'temperature-readings'

while True:
    temperature = random.uniform(14000, 16000)  # Simulated temperature reading
    payload = json.dumps({'temperature': temperature}).encode('utf-8')
    producer.produce(topic, value=payload, callback=delivery_report)
    producer.poll(0)  # Trigger delivery reports
    time.sleep(1)  # Send a reading every second