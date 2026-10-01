"""BenMart Pipeline Orchestrator — one call runs Bronze → Silver → Gold."""

import logging
import time
from functools import wraps

from src.ingestion.bronze_processor import process_bronze
from src.transformation.silver_processor import process_silver
from src.aggregation.gold_processor import process_gold

logger = logging.getLogger(__name__)


def timed(step_name):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"{'='*50}")
            logger.info(f"▶ STARTING: {step_name}")
            logger.info(f"{'='*50}")
            start = time.time()
            try:
                result = func(*args, **kwargs)
                elapsed = round(time.time() - start, 1)
                logger.info(f"✅ {step_name} COMPLETE | {elapsed}s")
                return result
            except Exception as e:
                elapsed = round(time.time() - start, 1)
                logger.error(f"❌ {step_name} FAILED after {elapsed}s | {e}")
                raise
        return wrapper
    return decorator


class PipelineOrchestrator:

    def __init__(self, spark, config, s3_bucket=None):
        self.spark = spark
        self.config = config
        self.s3_bucket = s3_bucket
        self.tables = list(config['tables'].keys())

    @timed("BRONZE LAYER")
    def _run_bronze(self):
        results = {}
        for table in self.tables:
            logger.info(f"  📦 Processing: {table}")
            results[table] = process_bronze(
                self.spark, self.config, table, self.s3_bucket
            )
            logger.info(f"  ✅ {table} done")
        return results

    @timed("SILVER LAYER")
    def _run_silver(self):
        return process_silver(self.spark, self.config, self.s3_bucket)

    @timed("GOLD LAYER")
    def _run_gold(self):
        return process_gold(self.spark, self.config, self.s3_bucket)

    def run(self):
        start = time.time()
        logger.info(f"{'='*50}")
        logger.info("🚀 PIPELINE STARTING")
        logger.info(f"  Tables: {self.tables}")
        logger.info(f"{'='*50}")

        self._run_bronze()
        self._run_silver()
        self._run_gold()

        total = round(time.time() - start, 1)
        logger.info(f"{'='*50}")
        logger.info(f"🎉 PIPELINE COMPLETE | Total: {total}s")
        logger.info(f"{'='*50}")
