import sys
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from src.utils.config_loader import load_config, get_s3_path, load_table_schema
from src.ingestion.bronze_processor import (
    build_schema, read_raw_data, apply_schema,
    deduplicate, add_metadata, write_bronze
)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV'])
env = args['ENV']

import os
os.environ['ENV'] = env

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"Starting Bronze Processing - Environment: {env}")

config_bucket = f"benmart-{env}-raw"
config = load_config(s3_bucket=config_bucket)

for table_name in config['tables']:
    logger.info(f"BRONZE PROCESSING: {table_name}")

    raw_path = get_s3_path(config, 'raw', table_name)
    bronze_path = get_s3_path(config, 'bronze', table_name)
    table_config = config['tables'][table_name]

    schema_json = load_table_schema(config, table_name, s3_bucket=config_bucket)

    source_format = table_config.get('source_format', 'csv')
    df = read_raw_data(spark, raw_path, source_format)

    schema = build_schema(schema_json)
    df = apply_schema(df, schema)

    df = deduplicate(df, table_config['primary_key'])
    df = add_metadata(df)

    partition_col = table_config.get('partition_column', None)
    write_bronze(df, bronze_path, partition_col)

    logger.info(f"BRONZE COMPLETE: {table_name}")

job.commit()
logger.info("All bronze processing complete!")