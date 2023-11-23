from common.utils import Singleton


@Singleton
class DataTransInfo:
    def __init__(self, kafka_server, agent_topic_name, identifier_topic_name, minIO=None):
        self.kafka_server = kafka_server
        self.agent_topic_name = agent_topic_name
        self.identifier_topic_name = identifier_topic_name
        self.minIO = minIO
