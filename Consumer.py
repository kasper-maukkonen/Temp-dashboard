from confluent_kafka import Consumer, KafkaError
import json
import numpy as np
import websocket

consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'temperature-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
topic = 'temperature-readings'
consumer.subscribe([topic])

threshold_multiplier = 2.0  # Adjust this multiplier as needed

# Initialize variables for calculating average and standard deviation
temperature_values = []

# WebSocket server address
websocket_server_url = 'ws://localhost:3000'

def send_to_websocket(data):
    try:
        ws = websocket.WebSocket()
        ws.connect(websocket_server_url)
        ws.send(json.dumps(data))  # Send data as JSON
        ws.close()
    except Exception as e:
        print(f'Error sending data to WebSocket server: {str(e)}')

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('Reached end of partition')
        else:
            print('Error: {}'.format(msg.error()))
    else:
        payload = json.loads(msg.value())
        temperature = payload['temperature']
        status = payload.get('status', '')  # Get the status, default to an empty string if not present
        print('Received temperature reading: {:.02f}'.format(temperature))

        # Add the temperature reading to the list
        temperature_values.append(temperature)

        # Calculate average and standard deviation
        average_temperature = np.mean(temperature_values)
        std_deviation = np.std(temperature_values)

        # Perform predictive analytics: Check if the current temperature is significantly higher
        if temperature > (average_temperature + threshold_multiplier * std_deviation):
                print('Temperature crossed the threshold! Alarm!')
    # Set the status as "Alarm" here
    status = "Alarm"
    try:
        send_to_websocket({'temperature': temperature, 'status': status})
    except Exception as e:
        print(f'Error sending data to WebSocket server: {str(e)}')