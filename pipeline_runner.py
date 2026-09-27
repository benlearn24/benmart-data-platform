import boto3
import time
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

REQUIRED_ENV_VARS = ["ENV", "AWS_REGION"]

for var in REQUIRED_ENV_VARS:
    if not os.environ.get(var):
        raise EnvironmentError(f"Missing required environment variable: {var}. Set it in .env file.")

ENV = os.environ["ENV"]
AWS_REGION = os.environ["AWS_REGION"]

PIPELINE_JOBS = [
    "benmart-bronze-job",
    "benmart-silver-job",
    "benmart-gold-job"
]

POLL_INTERVAL = 30
MAX_WAIT_TIME = 600

glue_client = boto3.client('glue', region_name=AWS_REGION)


def start_job(job_name: str) -> str:
    """Start a Glue job and return its Run ID"""
    logger.info(f"STARTING: {job_name}")

    response = glue_client.start_job_run(
        JobName=job_name,
        Arguments={'--ENV': ENV}
    )

    run_id = response['JobRunId']
    logger.info(f"STARTED: {job_name} | Run ID: {run_id}")
    return run_id


def wait_for_job(job_name: str, run_id: str) -> str:
    """Poll job status every POLL_INTERVAL seconds until done or timeout"""
    elapsed = 0

    while elapsed < MAX_WAIT_TIME:
        response = glue_client.get_job_run(
            JobName=job_name,
            RunId=run_id
        )

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
    """Run all jobs in sequence — stop on first failure"""
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