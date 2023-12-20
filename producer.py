from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for i in range(10):
    data = {'message': f'Message {i}'}
    producer.send('test-topic', value=data)
    print(f"Sent: {data}")
    time.sleep(1)

producer.close()
