import logging
import cv2
import uvicorn
import numpy as np
from fastapi import FastAPI

from common.dataTransInfo import DataTransInfo
from dataTransModule.dataTrans import DataTrans
from kafkaModule.kafkaConsumer import KafkaConsumerOperation, KafkaListener

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - {%(name)s}-{%(filename)s [%(lineno)d]} - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)
app = FastAPI()


@app.get("/dataTransInit")
async def dataTransInit(kafka_servers, agent_topic, identifier_topic, group_id):
    logger.info(kafka_servers)
    info = DataTransInfo(kafka_servers, agent_topic, identifier_topic)
    operation = KafkaConsumerOperation()
    dataTrans = DataTrans()
    operation.messageHandler = dataTrans.initialize(weights="dataTransModule/mouse.pt", index=1)
    listener = KafkaListener(info.kafka_server, info.agent_topic_name, group_id,
                             operation.createConsumer, operation.terminateConsumer)
    listener.start_listening()
    return {"message": "success"}


@app.get("/dataTransCapture")
async def dataTransCapture(kafka_servers, agent_topic, identifier_topic, group_id):
    logger.info(kafka_servers)
    info = DataTransInfo(kafka_servers, agent_topic, identifier_topic)
    operation = KafkaConsumerOperation()
    dataTrans = DataTrans()
    operation.messageHandler = dataTrans.initialize(weights="dataTransModule/mouse.pt", index=0)
    success, img = dataTrans.image_capture()
    listener = KafkaListener(info.kafka_server, info.agent_topic_name, group_id,
                             operation.createConsumer, operation.terminateConsumer)
    listener.start_listening()
    if success:
        return {"message": "success",
                "img": np.array2string(img)}
    else:
        return {"message": "failed"}


@app.get("/dataTransControl")
async def dataTransControl(kafka_servers, agent_topic, identifier_topic, group_id):
    logger.info(kafka_servers)
    info = DataTransInfo(kafka_servers, agent_topic, identifier_topic)
    operation = KafkaConsumerOperation()
    dataTrans = DataTrans()
    operation.messageHandler = dataTrans.control(None)
    listener = KafkaListener(info.kafka_server, info.agent_topic_name, group_id, operation.createConsumer,
                             operation.terminateConsumer())
    listener.start_listening()
    return {"message": "success"}


if __name__ == "__main__":
    uvicorn.run(app="main:app", host="127.0.0.1", port=8080, reload=False)
