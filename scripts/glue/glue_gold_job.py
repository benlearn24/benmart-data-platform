import sys
import os
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from src.utils.config_loader import load_config
from src.aggregation.gold_processor import process_gold

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV'])
env = args['ENV']
os.environ['ENV'] = env

# Initialize Spark + Glue
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Load config from S3
config_bucket = f"benmart-{env}-raw"
config = load_config(s3_bucket=config_bucket)

logger.info(f"Starting Gold processing for environment: {env}")

# Run Gold pipeline
process_gold(spark, config, s3_bucket=config_bucket)

# Done
job.commit()
logger.info("Gold Glue job completed successfully!")