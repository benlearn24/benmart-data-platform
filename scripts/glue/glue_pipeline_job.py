"""BenMart Full Pipeline — single Glue job."""

from utils.spark_utils import setup_logging, create_glue_spark
from utils.config_loader import load_config
from src.pipeline_orchestrator import PipelineOrchestrator

setup_logging()
spark, job, env = create_glue_spark(['JOB_NAME', 'ENV'])
config = load_config(env=env, s3_bucket="benmart-dev-raw")

PipelineOrchestrator(spark, config, s3_bucket="benmart-dev-raw").run()

job.commit()
