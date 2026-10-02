"""config_loader.py — Configuration and schema loading for BenMart Data Platform."""

import os
import yaml
import json
import logging
import boto3

logger = logging.getLogger(__name__)


def get_env():
    env = os.environ.get("ENV")
    if not env:
        logger.warning("ENV variable not set. Defaulting to 'dev'.")
        return "dev"
    logger.info(f"Environment loaded: {env}")
    return env


def read_s3_file(bucket, key):
    s3 = boto3.client('s3')
    try:
        logger.info(f"Reading S3 file: s3://{bucket}/{key}")
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        logger.info(f"S3 file read successful: s3://{bucket}/{key} ({len(content)} chars)")
        return content
    except s3.exceptions.NoSuchBucket:
        logger.error(f"S3 bucket does not exist: {bucket}")
        raise
    except s3.exceptions.NoSuchKey:
        logger.error(f"S3 file not found: s3://{bucket}/{key}")
        raise
    except Exception as e:
        logger.error(f"Failed to read S3 file: s3://{bucket}/{key} | Error: {e}")
        raise


def load_config(config_path=None, s3_bucket=None, s3_config_key="config/config.yaml"):
    if s3_bucket:
        config_text = read_s3_file(s3_bucket, s3_config_key)
    else:
        if config_path is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.join(current_dir, '..', '..')
            config_path = os.path.join(project_root, 'config', 'config.yaml')

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, 'r', encoding='utf-8') as f:
            config_text = f.read()

    env = get_env()

    replacements = {
        "${ENV}": env,
        "${RDS_HOST}": os.environ.get("RDS_HOST", "localhost"),
        "${RDS_USERNAME}": os.environ.get("RDS_USERNAME", "admin"),
    }

    for placeholder, value in replacements.items():
        config_text = config_text.replace(placeholder, value)

    try:
        config = yaml.safe_load(config_text)
    except yaml.YAMLError as e:
        logger.error(f"Config YAML parse FAILED: {e}")
        raise

    logger.info(f"Config loaded for env: {env}")
    return config


def get_s3_path(config, layer, table_name):
    try:
        bucket = config['s3'][f'{layer}_bucket']
        table_config = config['tables'][table_name]
        table_path = table_config.get(f'{layer}_path', f'{table_name}/')
    except KeyError as e:
        logger.error(f"Config key missing for layer='{layer}', table='{table_name}' | Missing key: {e}")
        raise

    full_path = f"s3://{bucket}/{table_path}"
    return full_path


def get_table_config(config, table_name):
    if table_name not in config['tables']:
        raise ValueError(f"Table '{table_name}' not found in config!")
    return config['tables'][table_name]


def load_table_schema(config, table_name, s3_bucket=None):
    table_config = get_table_config(config, table_name)
    schema_file = table_config['schema_file']

    if s3_bucket:
        content = read_s3_file(s3_bucket, schema_file)
        try:
            schema = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Schema JSON parse FAILED for {table_name}: {e}")
            raise
    else:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.join(current_dir, '..', '..')
        schema_path = os.path.join(project_root, schema_file)

        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        with open(schema_path, 'r', encoding='utf-8') as f:
            try:
                schema = json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Schema JSON parse FAILED for {table_name}: {e}")
                raise

    logger.info(f"Schema loaded for table: {table_name} ({len(schema.get('columns', []))} columns)")
    return schema
