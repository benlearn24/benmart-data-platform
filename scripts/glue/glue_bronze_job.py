"""
glue_bronze_job.py — AWS Glue entry point for Bronze layer processing.

Reads raw CSV files from S3, applies schema, deduplicates, adds metadata,
writes cleaned Parquet to Bronze bucket. Processes all tables defined in config.

Triggered by: pipeline_runner.py → start_job("benmart-bronze-job")
"""

import sys
import os
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from src.utils.config_loader import load_config
from src.ingestion.bronze_processor import process_bronze

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

logger.info(f"Starting Bronze Processing - Environment: {env}")

try:
    config_bucket = f"benmart-{env}-raw"
    config = load_config(s3_bucket=config_bucket)

    for table_name in config['tables']:
        process_bronze(spark, config, table_name, s3_bucket=config_bucket)

    logger.info("All bronze processing complete!")
except Exception as e:
    logger.error(f"Bronze processing FAILED: {str(e)}")
    raise
finally:
    job.commit()
