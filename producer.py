from kafka import KafkaProducer
import json
import time
import logging

logger = logging.getLogger('producer')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)

try:
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    for i in range(10):
        data = {'message': f'Message {i}'}
        producer.send('test-topic', value=data)
        logger.info(f"Sent: {data}")
        time.sleep(1)

    producer.close()
except Exception as e:
    logger.error(f"Error: {str(e)}")
