import os
from time import time, sleep
from random import choice
import json
from confluent_kafka import Producer, Message
from confluent_kafka.admin import AdminClient, NewTopic


# Configs
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'avroic')
KAFKA_TOPIC_PARTITION = int(os.getenv('KAFKA_TOPIC_PARTITION', '4'))
KAFKA_TOPIC_REPLICATION_FACTOR = int(os.getenv('KAFKA_TOPIC_REPLICATION_FACTOR', '1'))
RANDOM_DATA_MAX_USERS = int(os.getenv('RANDOM_DATA_MAX_USERS', '1000'))
RANDOM_DATA_MAX_ITEMS = int(os.getenv('RANDOM_DATA_MAX_ITEMS', '100'))
PRODUCER_RATE = float(os.getenv('PRODUCER_RATE', '0.1'))

p = Producer({'bootstrap.servers': KAFKA_BROKER})


def get_random_data() -> dict:
    """ Generate and return random data
        return: dict
        {
            "user_id": "user_765",
            "item_id": "item_43",
            "interaction_type": "like",
            "timestamp": 1736404972
        }
    """
    data = {
        "user_id": f"user_{choice(range(RANDOM_DATA_MAX_USERS))}",
        "item_id": f"item_{choice(range(RANDOM_DATA_MAX_ITEMS))}",
        "interaction_type": choice(["like", "click", "view", "purchase"]),
        "timestamp": int(time()),
    }
    return data


def create_topic():
    """ Create a topic in Kafka if not exists
    """
    admin = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    topic_list = admin.list_topics(timeout=10).topics
    if KAFKA_TOPIC not in topic_list:
        new_topic = NewTopic(KAFKA_TOPIC, num_partitions=KAFKA_TOPIC_PARTITION, replication_factor=KAFKA_TOPIC_REPLICATION_FACTOR)
        admin.create_topics([new_topic])
        print(f"Topic[{KAFKA_TOPIC}] created!")
    else:
        print(f"Topic[{KAFKA_TOPIC}] already exists!")


def delivery_report(err, msg: Message):
    """ Called once for each message produced to show delivery result.
    """
    try:
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to topic[{msg.topic()}] @partition-{msg.partition()}')
    except KeyboardInterrupt:
            pass


def produce_messages():
    """ Produce message to Kafka until press Ctrl+C
    """
    global p
    pool_len = 0
    total_produced = 0
    while True:
        try:
            if pool_len >= 10:
                p.poll(0)
                pool_len = 0

            # Generate and serialized data
            data = get_random_data()
            serialized_value = json.dumps(data).encode('utf-8')
            serialized_key = data["user_id"].encode('utf-8')

            # Send data into kafka
            p.produce(KAFKA_TOPIC, key=serialized_key, value=serialized_value, callback=delivery_report)
            sleep(PRODUCER_RATE)
            pool_len += 1
            total_produced += 1
        except BufferError:
            p.flush()
            print("BufferError: Flushing the pool...")
        except KeyboardInterrupt:
            print("Ctrl+C pressed, exiting...")
            break

    # Flush the pool
    p.flush()
    print(f'{total_produced} Messages produced!')


if __name__ == '__main__':
    create_topic()
    produce_messages()