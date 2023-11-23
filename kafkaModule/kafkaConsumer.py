import logging
import threading

from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - {%(name)s}-{%(filename)s [%(lineno)d]} - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)


class KafkaConsumerOperation:
    def __init__(self):
        self.messageHandler = None
        self.__active = False

    def terminateConsumer(self):
        self.__active = False

    def createConsumer(self, topics, bootstrap_servers, group_id, consumer_active):
        self.__active = consumer_active
        consumer = KafkaConsumer(topics, bootstrap_servers=bootstrap_servers, group_id=group_id,
                                 enable_auto_commit=True, auto_commit_interval_ms=1)
        while self.__active:
            records = consumer.poll(timeout_ms=1)
            for k, v in records.items():
                for i in v:
                    if i is None:
                        continue
                    # logger.info(i.v)
                    logger.info("receive message: {}".format(i.value))
                    if self.messageHandler:
                        self.messageHandler(str(i.value))


class KafkaListener:
    def __init__(self, server, listenTopic, groupId, listeningFun, terminateFun):
        self.__thread = None
        self.__server = server
        self.__listenTopic = listenTopic
        self.__groupId = groupId
        self.__listeningFun = listeningFun
        self.__terminateFun = terminateFun
        self.__consumerActive = False
        self.__listenerActive = False

    def start_listening(self):
        self.__consumerActive = True
        self.__thread = threading.Thread(target=self.__listeningFun,
                                         args=(self.__listenTopic, self.__server,
                                               self.__groupId, self.__consumerActive))
        self.__thread.start()
        self.__listenerActive = True

    def stop_listening(self):
        self.__terminateFun()
        self.__thread.join()
        self.__listenerActive = False
        self.__consumerActive = False
