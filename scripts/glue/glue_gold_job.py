import sys
import logging
from awsglue.utils import getResolvedOptions

from src.utils.spark_utils import setup_logging, create_glue_spark
from src.utils.config_loader import load_config
from src.aggregation.gold_processor import process_gold

setup_logging()
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV'])
spark, job, env = create_glue_spark(args)

logger.info(f"Starting Gold Processing - Environment: {env}")

try:
    config_bucket = f"benmart-{env}-raw"
    config = load_config(s3_bucket=config_bucket)

    process_gold(spark, config, s3_bucket=config_bucket)

    logger.info("Gold processing complete!")
except Exception as e:
    logger.error(f"Gold processing FAILED: {str(e)}")
    raise
finally:
    job.commit()

