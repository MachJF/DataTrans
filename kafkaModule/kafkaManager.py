from kafka import KafkaAdminClient
from kafka.admin import NewTopic

# from kafkaModual.kafkaProducer import createProducer
#
# producer = createProducer(["127.0.0.1:9092"])


def createTopic(servers, topic_name):
    client = KafkaAdminClient(bootstrap_servers=servers)
    topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
    client.create_topics(topic_list)

#
# if __name__ == "__main__":
#     createTopic(["127.0.0.1:9092"], "topic01")
