import sys
import logging
from awsglue.utils import getResolvedOptions

from src.utils.spark_utils import setup_logging, create_glue_spark
from src.utils.config_loader import load_config
from src.ingestion.bronze_processor import process_bronze

setup_logging()
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV'])
spark, job, env = create_glue_spark(args)

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
