import json
import boto3
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class ManifestManager:
    """Tracks processed files for incremental loads."""

    def __init__(self, raw_bucket, manifest_prefix, table_name):
        self.s3_client = boto3.client('s3')
        self.bucket = raw_bucket
        self.key = f"{manifest_prefix}{table_name}_manifest.json"
        self.table_name = table_name
        self.manifest = self._load()

    def _load(self):
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=self.key)
            manifest = json.loads(response['Body'].read().decode('utf-8'))
            logger.info(f"📋 Manifest loaded: {len(manifest['processed_files'])} files already tracked")
            return manifest
        except self.s3_client.exceptions.NoSuchKey:
            logger.info(f"📋 No manifest found — first run for {self.table_name}")
            return {
                "table_name": self.table_name,
                "processed_files": [],
                "last_processed_at": None
            }

    def get_new_files(self, raw_path):
        all_files = self._list_s3_files(raw_path)
        processed = {f["file_path"] for f in self.manifest["processed_files"]}
        new_files = [f for f in all_files if f not in processed]
        logger.info(
            f"📋 Files — total: {len(all_files)}, "
            f"already processed: {len(processed)}, "
            f"new: {len(new_files)}"
        )
        return new_files

    def _list_s3_files(self, raw_path):
        prefix = raw_path.lstrip("/")
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket, Prefix=prefix
        )

        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if not obj['Key'].endswith('/') and obj['Size'] > 0:
                    files.append(f"s3://{self.bucket}/{obj['Key']}")
        return files

    def update(self, new_file_paths, row_count):
        now = datetime.now(timezone.utc).isoformat()
        for path in new_file_paths:
            self.manifest["processed_files"].append({
                "file_path": path,
                "processed_at": now,
                "row_count": row_count
            })
        self.manifest["last_processed_at"] = now

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=self.key,
            Body=json.dumps(self.manifest, indent=2),
            ContentType="application/json"
        )
        logger.info(f"📋 Manifest updated: +{len(new_file_paths)} files tracked")
