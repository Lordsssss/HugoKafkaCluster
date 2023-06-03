from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions

class KafkaAdmin:
    def __init__(self, bootstrap_servers):
        self.client = AdminClient({'bootstrap.servers':bootstrap_servers})

    
    def create_topic(self, topic_name, num_partitions=1, replication_factor=1):
        new_topics = [NewTopic(topic_name, num_partitions, replication_factor)]
        fs = self.client.create_topics(new_topics)

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))
    # deletes an existing topic. It takes the topic name as an argument.
    def delete_topic(self, topic_name):
        fs = self.client.delete_topics([topic_name])

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))
            except Exception as e:
                print("Failed to delete topic {}: {}".format(topic, e))
                
    # lists all topics in the Kafka cluster. It doesn't take any arguments.
    def list_topics(self):
        topics = self.client.list_topics().topics
        print("Topics in cluster:")
        for topic in topics:
            print(topic)
            
    # provides metadata for a specific topic. It takes the topic name as an 
    def describe_topic(self, topic_name):
        metadata = self.client.list_topics(topic=topic_name).topics[topic_name]
        print("Topic: {} \n\tPartitions: {} \n\tReplication factor: {}".format(
            topic_name, metadata.partitions, metadata.replication_factor))
    
    # increases the number of partitions for a topic. It takes the topic name and the new total number of partitions as arguments.
    def increase_partitions(self, topic_name, num_partitions):
        new_parts = [NewPartitions(topic_name, num_partitions)]
        fs = self.client.create_partitions(new_parts)

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Increased partitions for topic {}".format(topic))
            except Exception as e:
                print("Failed to increase partitions for topic {}: {}".format(topic, e))