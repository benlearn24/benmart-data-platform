"""
glue_silver_job.py — AWS Glue entry point for Silver layer processing.

Reads Bronze Parquet data (orders, customers, products), joins tables,
applies business rules (cancel filter, GST calc), writes to Silver bucket.

Triggered by: pipeline_runner.py → start_job("benmart-silver-job")
Depends on: Bronze job must complete first (reads Bronze output).
"""

import sys
import os
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from src.utils.config_loader import load_config
from src.transformation.silver_processor import process_silver

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV'])
env = args['ENV']
os.environ['ENV'] = env

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"Starting Silver Processing - Environment: {env}")

try:
    config_bucket = f"benmart-{env}-raw"
    config = load_config(s3_bucket=config_bucket)

    process_silver(spark, config, s3_bucket=config_bucket)

    logger.info("Silver processing complete!")
except Exception as e:
    logger.error(f"Silver processing FAILED: {str(e)}")
    raise
finally:
    job.commit()