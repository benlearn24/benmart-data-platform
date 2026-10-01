import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseProcessor(ABC):

    def __init__(self, spark, config, s3_bucket=None):
        self.spark = spark
        self.config = config
        self.s3_bucket = s3_bucket

    @property
    @abstractmethod
    def processor_name(self):
        pass

    @abstractmethod
    def process(self):
        pass

    def run(self):
        logger.info(f"{'='*50}")
        logger.info(f"▶ {self.processor_name} — Starting")
        logger.info(f"{'='*50}")
        try:
            result = self.process()
            logger.info(f"✅ {self.processor_name} — Complete")
            return result
        except Exception as e:
            logger.error(f"❌ {self.processor_name} — FAILED: {e}")
            raise
