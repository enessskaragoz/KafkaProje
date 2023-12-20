from kafka import KafkaConsumer
import json
import logging

logger = logging.getLogger('consumer')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)

try:
    consumer = KafkaConsumer('test-topic', bootstrap_servers='localhost:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
    for message in consumer:
        logger.info(f"Received: {message.value}")

    consumer.close()
except Exception as e:
    logger.error(f"Error: {str(e)}")
