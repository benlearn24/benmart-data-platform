"""
glue_gold_job.py — AWS Glue entry point for Gold layer processing.

Reads Silver enriched data, creates Star Schema tables (fact + dimensions + aggregations),
writes 7 tables to Gold S3 bucket as Parquet.

Triggered by: pipeline_runner.py → start_job("benmart-gold-job")
Depends on: Silver job must complete first (reads Silver output).
"""

import sys
import os
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from src.utils.config_loader import load_config
from src.aggregation.gold_processor import process_gold

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV'])
env = args['ENV']
os.environ['ENV'] = env

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

logger.info(f"Starting Gold processing for environment: {env}")

try:
    config_bucket = f"benmart-{env}-raw"
    config = load_config(s3_bucket=config_bucket)

    process_gold(spark, config, s3_bucket=config_bucket)

    logger.info("Gold Glue job completed successfully!")
except Exception as e:
    logger.error(f"Gold processing FAILED: {str(e)}")
    raise
finally:
    job.commit()