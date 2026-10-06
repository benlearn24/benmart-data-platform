
"""Base processor — abstract foundation for Bronze, Silver, Gold processors."""

import logging
import time
from abc import ABC, abstractmethod
from functools import wraps

logger = logging.getLogger(__name__)


def log_step(step_name):
    """Decorator — logs start/end/duration of any processing step."""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            processor = getattr(self, 'processor_name', self.__class__.__name__)
            logger.info(f"[{processor}] {step_name} — START")
            start = time.time()
            try:
                result = func(self, *args, **kwargs)
                elapsed = round(time.time() - start, 2)
                logger.info(f"[{processor}] {step_name} — DONE ({elapsed}s)")
                return result
            except Exception as e:
                elapsed = round(time.time() - start, 2)
                logger.error(f"[{processor}] {step_name} — FAILED ({elapsed}s): {e}")
                raise
        return wrapper
    return decorator


class BaseProcessor(ABC):
    """Abstract base for all layer processors. Enforces standard interface."""

    def __init__(self, spark, config, s3_bucket=None):
        self.spark = spark
        self.config = config
        self.s3_bucket = s3_bucket

    @property
    @abstractmethod
    def processor_name(self):
        """Return processor identifier (e.g., 'BRONZE:orders', 'SILVER')."""
        pass

    @abstractmethod
    def process(self):
        """Main entry point — orchestrates the full processing flow."""
        pass

    @staticmethod
    def count_nulls(df, column):
        """Count null values in a column. Returns (null_count, null_percentage)."""
        from pyspark.sql import functions as F
        total = df.count()
        if total == 0:
            return 0, 0.0
        null_count = df.filter(F.col(column).isNull()).count()
        return null_count, round((null_count / total) * 100, 2)

    @staticmethod
    def safe_count(df, label=""):
        """Count with logging — avoids silent empty DataFrames."""
        count = df.count()
        if count == 0:
            logger.warning(f"EMPTY DataFrame{f': {label}' if label else ''}")
        else:
            logger.info(f"Row count{f' ({label})' if label else ''}: {count:,}")
        return count

    def run(self):
        """Template method — wraps process() with error handling."""
        try:
            return self.process()
        except Exception as e:
            logger.error(f"[{self.processor_name}] FAILED: {e}")
            raise
