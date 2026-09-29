"""
pipeline_runner.py — Sequential Glue pipeline orchestrator for BenMart Data Platform.

Triggers Bronze → Silver → Gold Glue jobs in order from local machine (PyCharm).
Each job must SUCCEED before the next starts. Any failure stops the pipeline.

Usage:
    PyCharm: Right Click → Run
    Terminal: python pipeline_runner.py

Exit codes:
    0 = All jobs succeeded
    1 = Pipeline failed (check logs for details)
"""

import boto3
import time
import logging
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load .env file — AWS credentials + config
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration — all from .env, no hardcoded defaults
REQUIRED_ENV_VARS = ["ENV", "AWS_REGION"]

for var in REQUIRED_ENV_VARS:
    if not os.environ.get(var):
        raise EnvironmentError(f"Missing required environment variable: {var}. Set it in .env file.")

ENV = os.environ["ENV"]
AWS_REGION = os.environ["AWS_REGION"]

# Pipeline definition — ORDER MATTERS (Bronze → Silver → Gold)
PIPELINE_JOBS = [
    "benmart-bronze-job",
    "benmart-silver-job",
    "benmart-gold-job"
]

POLL_INTERVAL = 30   # seconds between status checks
MAX_WAIT_TIME = 600  # max wait per job (10 min)

# Initialize Glue client
try:
    glue_client = boto3.client('glue', region_name=AWS_REGION)
    logger.info(f"Glue client initialized for region: {AWS_REGION}")
except Exception as e:
    logger.error(f"Failed to initialize Glue client: {str(e)}")
    raise


def start_job(job_name: str) -> str:
    """Start a Glue job and return its Run ID."""
    logger.info(f"STARTING: {job_name}")

    try:
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={'--ENV': ENV}
        )
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'EntityNotFoundException':
            logger.error(f"Glue job not found: {job_name}. Check job name in AWS Console.")
        elif error_code == 'ConcurrentRunsExceededException':
            logger.error(f"Job already running: {job_name}. Wait for previous run to finish.")
        else:
            logger.error(f"AWS error starting {job_name}: {error_code} — {str(e)}")
        raise

    run_id = response['JobRunId']
    logger.info(f"STARTED: {job_name} | Run ID: {run_id}")
    return run_id


def wait_for_job(job_name: str, run_id: str) -> str:
    """Poll Glue job status every POLL_INTERVAL seconds until complete or timeout."""
    elapsed = 0

    while elapsed < MAX_WAIT_TIME:
        try:
            response = glue_client.get_job_run(
                JobName=job_name,
                RunId=run_id
            )
        except ClientError as e:
            logger.error(f"Failed to check status for {job_name}: {str(e)}")
            return 'ERROR'

        status = response['JobRun']['JobRunState']
        logger.info(f"POLLING: {job_name} | Status: {status} | Elapsed: {elapsed}s")

        if status == 'SUCCEEDED':
            duration = response['JobRun'].get('ExecutionTime', 0)
            logger.info(f"SUCCESS: {job_name} | Duration: {duration}s")
            return 'SUCCEEDED'

        if status in ('FAILED', 'ERROR', 'TIMEOUT'):
            error_message = response['JobRun'].get('ErrorMessage', 'No error message')
            logger.error(f"FAILED: {job_name} | Status: {status} | Error: {error_message}")
            return status

        if status == 'STOPPED':
            logger.warning(f"STOPPED: {job_name} | Job was manually stopped")
            return 'STOPPED'

        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    logger.error(f"TIMEOUT: {job_name} | Exceeded {MAX_WAIT_TIME}s wait time")
    return 'TIMEOUT'


def run_pipeline() -> bool:
    """Run Bronze → Silver → Gold in sequence. Stop on first failure."""
    logger.info("=" * 60)
    logger.info(f"PIPELINE STARTED | Environment: {ENV}")
    logger.info(f"Jobs to run: {PIPELINE_JOBS}")
    logger.info("=" * 60)

    results = {}
    pipeline_start = time.time()

    for job_name in PIPELINE_JOBS:
        logger.info("-" * 40)

        try:
            run_id = start_job(job_name)
        except Exception as e:
            logger.error(f"FAILED TO START: {job_name} | Error: {str(e)}")
            results[job_name] = 'FAILED_TO_START'
            break

        status = wait_for_job(job_name, run_id)
        results[job_name] = status

        if status != 'SUCCEEDED':
            logger.error(f"PIPELINE STOPPED | {job_name} returned: {status}")
            logger.error(f"Remaining jobs SKIPPED: {PIPELINE_JOBS[PIPELINE_JOBS.index(job_name)+1:]}")
            break

    # Pipeline summary
    pipeline_duration = round(time.time() - pipeline_start, 1)
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)

    for job, status in results.items():
        icon = "✅" if status == 'SUCCEEDED' else "❌"
        logger.info(f"  {icon} {job}: {status}")

    for job in PIPELINE_JOBS:
        if job not in results:
            logger.info(f"  ⏭️  {job}: SKIPPED")

    all_succeeded = all(s == 'SUCCEEDED' for s in results.values())
    pipeline_status = "SUCCESS" if all_succeeded and len(results) == len(PIPELINE_JOBS) else "FAILED"

    logger.info(f"PIPELINE {pipeline_status} | Total duration: {pipeline_duration}s")
    logger.info("=" * 60)

    return all_succeeded


if __name__ == "__main__":
    success = run_pipeline()
    if not success:
        exit(1)
