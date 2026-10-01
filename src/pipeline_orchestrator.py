"""BenMart Pipeline Orchestrator — one call runs Bronze → Silver → Gold."""

import logging
import time
from functools import wraps

from src.ingestion.bronze_processor import process_bronze
from src.transformation.silver_processor import process_silver
from src.aggregation.gold_processor import process_gold

logger = logging.getLogger(__name__)


def timed(step_name):
    """Logs execution time of each pipeline step."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"▶ Starting: {step_name}")
            start = time.time()
            result = func(*args, **kwargs)
            elapsed = round(time.time() - start, 1)
            logger.info(f"✅ {step_name} complete | {elapsed}s")
            return result
        return wrapper
    return decorator


class PipelineOrchestrator:
    """Chains Bronze → Silver → Gold in one call."""

    def __init__(self, spark, config, s3_bucket=None):
        self.spark = spark
        self.config = config
        self.s3_bucket = s3_bucket
        self.tables = list(config['tables'].keys())

    @timed("BRONZE LAYER")
    def _run_bronze(self):
        results = {}
        for table in self.tables:
            results[table] = process_bronze(
                self.spark, self.config, table, self.s3_bucket
            )
        return results

    @timed("SILVER LAYER")
    def _run_silver(self):
        return process_silver(self.spark, self.config, self.s3_bucket)

    @timed("GOLD LAYER")
    def _run_gold(self):
        return process_gold(self.spark, self.config, self.s3_bucket)

    def run(self):
        start = time.time()
        logger.info("🚀 Pipeline starting")

        self._run_bronze()
        self._run_silver()
        self._run_gold()

        total = round(time.time() - start, 1)
        logger.info(f"🎉 Pipeline complete | Total: {total}s")
