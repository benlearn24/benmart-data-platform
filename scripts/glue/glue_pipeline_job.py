"""BenMart Full Pipeline — single Glue job."""
from src.utils.spark_utils import setup_logging, create_glue_spark
from src.utils.config_loader import load_config
from src.pipeline_orchestrator import PipelineOrchestrator

setup_logging()
spark, job, env = create_glue_spark(['JOB_NAME', 'ENV'])
config = load_config(s3_bucket="benmart-dev-raw")

PipelineOrchestrator(spark, config, s3_bucket="benmart-dev-raw").run()

job.commit()
