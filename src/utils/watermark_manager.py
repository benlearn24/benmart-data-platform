import json
import boto3
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class WatermarkManager:
    """Stores and retrieves a timestamp watermark in S3 for incremental processing."""

    def __init__(self, bucket, prefix, processor_name):
        self.s3_client = boto3.client('s3')
        self.bucket = bucket
        self.key = f"{prefix}{processor_name}_watermark.json"
        self.processor_name = processor_name
        self._watermark = self._load()

    def _load(self):
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=self.key)
            data = json.loads(response['Body'].read().decode('utf-8'))
            ts = data.get('last_processed_at')
            logger.info(f"📋 Watermark loaded: {ts} for {self.processor_name}")
            return ts
        except self.s3_client.exceptions.NoSuchKey:
            logger.info(f"📋 No watermark found — first run for {self.processor_name}")
            return None

    def get_watermark(self):
        return self._watermark

    def update(self, new_watermark):
        self._watermark = new_watermark
        data = {
            "processor_name": self.processor_name,
            "last_processed_at": new_watermark,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=self.key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        logger.info(f"📋 Watermark updated: {new_watermark}")
