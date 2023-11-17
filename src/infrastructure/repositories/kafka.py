from kafka import KafkaConsumer
from kafka import KafkaProducer
from settings import KAFKA_CONFIG


class RawPriceConsumer:
    def __init__(self, topic):
        self.consumer = KafkaConsumer(topic, **KAFKA_CONFIG)

    def consume_raw_price(self):
        return self.consumer

class TransformedPriceProducer:
    def __init__(self):
        self.producer = KafkaProducer(**KAFKA_CONFIG)

    def publish(self, topic, message):
        self.producer.send(topic, value=message)
        self.producer.flush()
