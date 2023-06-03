from confluent_kafka import Consumer, KafkaError

class KafkaConsumer:
    def __init__(self, brokers, group):
        self.consumer = Consumer({
            'bootstrap.servers': brokers,
            'group.id': group,
            'auto.offset.reset': 'earliest'
        })

    def subscribe(self, topic):
        self.consumer.subscribe([topic])

        while True:
            msg = self.consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            print('Received message: {}'.format(msg.value().decode('utf-8')))

# Usage
kafka_consumer = KafkaConsumer('localhost:9092', 'random')
kafka_consumer.subscribe('Hello')