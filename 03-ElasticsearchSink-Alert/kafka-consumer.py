import os
from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch
import json

# Configs for Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'avroic_aggregated')
KAFKA_GROUP_ID = os.getenv('KAFKA_TOPIC', 'ElasticsearchSink-Alert')

# Configs for Elasticsearch
ES_HOST = os.getenv('ES_HOST', 'http://localhost:9200')

# Initialize Kafka consumer
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([KAFKA_TOPIC])
print(f"Consuming messages from topic: {KAFKA_TOPIC}")

# Initialize Elasticsearch client
es = Elasticsearch(ES_HOST)
print(f"Connected to Elasticsearch: {ES_HOST}")

# Data Mapper
MAPPER = {
    'agg_interactions_item': {
        'id': 'item_id',
        'alerts': [('max_interactions', '>', 12)],
    },
    'agg_interactions_user': {
        'id': 'user_id',
        'alerts': [('total_interactions', '>', 100),
                   ('avg_interactions', '<', 40)],
    },
    'avg_interactions': {
        'id': 'avg_interactions',
    },
}


def save_to_elasticsearch(data):
    """ Save data into Elasticsearch
    """
    res = es.index(index=data['index'], id=data['id'], document=data['value'])
    

def extract_index_from_key(text):
    """ Extract index of Elasticsearch from key
    """
    indice = [key for key in MAPPER.keys()]
    for index in indice:
        if text.startswith(index):
            return index
        

def extract_id_from_data(data: dict):
    """ Extract id from value
    """
    document = data['value']
    key = MAPPER[data['index']]['id']
    return document[key]


def is_it_alert(data: dict):
    """ Check alert from value
    """
    document = data['value']
    alerts = MAPPER[data['index']]['alerts']
    for alert in alerts:
        alert_text = ' '.join([str(item) for item in alert])
        key, op, value = alert
        if op == '>':
            if document[key] > value:
                return True, alert_text
        elif op == '<':
            if document[key] < value:
                return True, alert_text
        elif op == '==':
            if document[key] == value:
                return True, alert_text
    return False, 'No Alert'


def save_alert(data):
    """ Save alert to Elasticsearch
    """
    es.index(index='alerts', body=data)


if __name__ == "__main__":
    # Consume messages from Kafka and save to Elasticsearch
    print("Polling messages from Kafka...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        
        # Prepair Data
        key = str(msg.key().decode('utf-8'))
        data = {}
        data['value'] = json.loads(msg.value().decode('utf-8'))
        data['index'] = extract_index_from_key(key)
        data['id'] = extract_id_from_data(data)

        save_to_elasticsearch(data)
        print(f"Saved to Elasticsearch: index={data['index']}, id={data['id']}")

        # Check Alert
        is_critical, alert_text = is_it_alert(data)
        if is_critical:
            data['alert_text'] = alert_text
            save_alert(data)
            print(f"Saved to Elasticsearch: index=alerts, id={data['id']}, alert={alert_text}")

    print("Closing Kafka consumer...")
    consumer.close()
