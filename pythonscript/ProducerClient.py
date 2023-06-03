from confluent_kafka import Producer

class KafkaProducer:
    def __init__(self, brokers):
        self.producer = Producer({'bootstrap.servers': brokers})
        self.counter = 0

    def produce_message(self, topic, value):
        key = str(self.counter)
        self.producer.produce(topic, key=key, value=value)
        self.producer.flush()
        self.counter += 1