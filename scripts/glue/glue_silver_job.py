import sys
import logging
from awsglue.utils import getResolvedOptions

from src.utils.spark_utils import setup_logging, create_glue_spark
from src.utils.config_loader import load_config
from src.transformation.silver_processor import process_silver

setup_logging()
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV'])
spark, job, env = create_glue_spark(args)

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
