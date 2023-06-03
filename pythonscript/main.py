# - Simple Kafka Admin commands -
from AdminClient import KafkaAdmin

kafka_admin = KafkaAdmin('localhost:9092')
kafka_admin.increase_partitions('Hello',5)

from ProducerClient import KafkaProducer

kafka_producer = KafkaProducer('localhost:9092')
kafka_producer.produce_message('Hello', 'Yessir')



