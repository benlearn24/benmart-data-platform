import boto3
import time
import logging
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineRunner:

    REQUIRED_ENV_VARS = ["ENV", "AWS_REGION"]

    PIPELINE_JOBS = [
        "benmart-bronze-job",
        "benmart-silver-job",
        "benmart-gold-job"
    ]

    def __init__(self, poll_interval=30, max_wait_time=600):
        self._validate_env()
        self.env = os.environ["ENV"]
        self.region = os.environ["AWS_REGION"]
        self.poll_interval = poll_interval
        self.max_wait_time = max_wait_time
        self.results = {}

        try:
            self.glue_client = boto3.client('glue', region_name=self.region)
            logger.info(f"Glue client initialized: {self.region}")
        except Exception as e:
            logger.error(f"Glue client FAILED: {str(e)}")
            raise

    def _validate_env(self):
        for var in self.REQUIRED_ENV_VARS:
            if not os.environ.get(var):
                raise EnvironmentError(f"Missing env variable: {var}. Set in .env file.")

    def _start_job(self, job_name):
        logger.info(f"STARTING: {job_name}")

        try:
            response = self.glue_client.start_job_run(
                JobName=job_name,
                Arguments={'--ENV': self.env}
            )
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'EntityNotFoundException':
                logger.error(f"Job not found: {job_name}. Check AWS Console.")
            elif error_code == 'ConcurrentRunsExceededException':
                logger.error(f"Already running: {job_name}. Wait for previous run.")
            else:
                logger.error(f"AWS error: {job_name} | {error_code} — {str(e)}")
            raise

        run_id = response['JobRunId']
        logger.info(f"STARTED: {job_name} | Run ID: {run_id}")
        return run_id

    def _wait_for_job(self, job_name, run_id):
        elapsed = 0

        while elapsed < self.max_wait_time:
            try:
                response = self.glue_client.get_job_run(
                    JobName=job_name,
                    RunId=run_id
                )
            except ClientError as e:
                logger.error(f"Status check FAILED: {job_name} | {str(e)}")
                return 'ERROR'

            status = response['JobRun']['JobRunState']
            logger.info(f"POLLING: {job_name} | {status} | {elapsed}s")

            if status == 'SUCCEEDED':
                duration = response['JobRun'].get('ExecutionTime', 0)
                logger.info(f"SUCCESS: {job_name} | Duration: {duration}s")
                return 'SUCCEEDED'

            if status in ('FAILED', 'ERROR', 'TIMEOUT'):
                error_msg = response['JobRun'].get('ErrorMessage', 'No error message')
                logger.error(f"FAILED: {job_name} | {status} | {error_msg}")
                return status

            if status == 'STOPPED':
                logger.warning(f"STOPPED: {job_name} | Manually stopped")
                return 'STOPPED'

            time.sleep(self.poll_interval)
            elapsed += self.poll_interval

        logger.error(f"TIMEOUT: {job_name} | Exceeded {self.max_wait_time}s")
        return 'TIMEOUT'

    def _print_summary(self, duration):
        logger.info("=" * 60)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 60)

        for job, status in self.results.items():
            icon = "✅" if status == 'SUCCEEDED' else "❌"
            logger.info(f"  {icon} {job}: {status}")

        for job in self.PIPELINE_JOBS:
            if job not in self.results:
                logger.info(f"  ⏭️  {job}: SKIPPED")

        all_succeeded = (
            all(s == 'SUCCEEDED' for s in self.results.values())
            and len(self.results) == len(self.PIPELINE_JOBS)
        )
        status = "SUCCESS" if all_succeeded else "FAILED"
        logger.info(f"PIPELINE {status} | Duration: {duration}s")
        logger.info("=" * 60)
        return all_succeeded

    def run(self):
        logger.info("=" * 60)
        logger.info(f"PIPELINE STARTED | Environment: {self.env}")
        logger.info(f"Jobs: {self.PIPELINE_JOBS}")
        logger.info("=" * 60)

        self.results = {}
        start_time = time.time()

        for job_name in self.PIPELINE_JOBS:
            logger.info("-" * 40)

            try:
                run_id = self._start_job(job_name)
            except Exception as e:
                logger.error(f"FAILED TO START: {job_name} | {str(e)}")
                self.results[job_name] = 'FAILED_TO_START'
                break

            status = self._wait_for_job(job_name, run_id)
            self.results[job_name] = status

            if status != 'SUCCEEDED':
                logger.error(f"PIPELINE STOPPED | {job_name}: {status}")
                remaining = self.PIPELINE_JOBS[self.PIPELINE_JOBS.index(job_name)+1:]
                logger.error(f"SKIPPED: {remaining}")
                break

        duration = round(time.time() - start_time, 1)
        return self._print_summary(duration)


if __name__ == "__main__":
    runner = PipelineRunner()
    success = runner.run()
    if not success:
        exit(1)
