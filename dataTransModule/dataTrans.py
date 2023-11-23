import logging
import cv2
from ultralytics import YOLO

from common.utils import Singleton
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - {%(name)s}-{%(filename)s [%(lineno)d]} - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

@Singleton
class DataTrans:
    def __init__(self):
        self.dataTransInfo = None
        self.model = None
        self.cap = None

    def initialize(self, weights, index):
        self.load_model(weights)
        self.connect_to_terminal(index)

    def load_model(self, weights):
        self.model = YOLO(weights)
        logger.info("模型加载成功")

    def connect_to_terminal(self, index):
        self.cap = cv2.VideoCapture(index)
        logger.info("终端视频信号连接成功")

    def image_capture(self):
        ret, frame = self.cap.read()
        return ret, frame

    def mouse_detect(self):
        ret, frame = self.image_capture()
        if not ret:
            logger.info("未获取到图像")
            return
        logger.info("获取到图像，开始识别")
        results = self.model(frame)
        xyxy = results[0].boxes.xyxy.tolist()
        logger.info(f"鼠标位置：{xyxy[0]}")
        logger.info("识别完毕")

    def control(self, task):
        logger.info(f"接收到指令:{task}")
        # to do
        logger.info(f"执行完毕")

