"""BenMart full pipeline — Glue entry point."""

from src.utils.spark_utils import setup_logging, create_glue_spark
from src.utils.config_loader import load_config
from src.pipeline_orchestrator import PipelineOrchestrator

setup_logging()
spark, job, env = create_glue_spark(["JOB_NAME", "ENV"])

# Bucket comes from ENV — same code runs in dev / prod
raw_bucket = f"benmart-{env}-raw"
config = load_config(s3_bucket=raw_bucket)

# run() raises on failure → job.commit() is never reached → Glue marks FAILED
PipelineOrchestrator(spark, config, s3_bucket=raw_bucket).run()

job.commit()
