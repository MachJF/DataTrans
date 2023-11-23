import time

from kafka import KafkaProducer


def createProducer(servers):
    producer = KafkaProducer(bootstrap_servers=servers, acks=-1)
    # producer.send()
    return producer


def closeProducer(producer):
    producer.close()


if __name__ == "__main__":
    producer = createProducer(['127.0.0.1:9092'])
    for i in range(50):
        producer.send("topic01", "hello".encode())
        print(i)
        time.sleep(1)
