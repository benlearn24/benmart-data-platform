"""
config_loader.py — Configuration and schema loading for BenMart Data Platform.

This module is the FIRST to execute in every Glue job (bronze, silver, gold).
It reads config.yaml and schema JSON files from either local filesystem (PyCharm)
or S3 (AWS Glue), replaces environment placeholders, and returns Python dicts.

Used by:
    - glue_bronze_job.py → load_config(), load_table_schema()
    - glue_silver_job.py → load_config()
    - glue_gold_job.py   → load_config()
    - pipeline_runner.py  (indirectly — triggers Glue jobs that call this)

Functions:
    get_env()           → Current environment name ("dev" / "prod")
    read_s3_file()      → S3 file content as string
    load_config()       → config.yaml → Python dict (placeholders replaced)
    get_s3_path()       → S3 full path for a table layer
    get_table_config()  → Single table config from config dict
    load_table_schema() → Schema JSON → Python dict
"""

import os
import yaml
import json
import logging
import boto3

logger = logging.getLogger(__name__)

def get_env():
    """Read ENV from environment variables, default 'dev' if not set."""
    env = os.environ.get("ENV")
    if not env:
        logger.warning("ENV variable not set. Defaulting to 'dev'. Set ENV in .env file for production.")
        return "dev"
    logger.info(f"Environment loaded: {env}")
    return env


def read_s3_file(bucket, key):
    """Read a file from S3 and return its content as string"""
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
        logger.error(f"S3 file not found: s3://{bucket}/{key}. Check if file was uploaded.")
        raise
    except Exception as e:
        logger.error(f"Failed to read S3 file: s3://{bucket}/{key} | Error: {str(e)}")
        raise


def load_config(config_path=None, s3_bucket=None, s3_config_key="config/config.yaml"):
    """Load config.yaml from local or S3, replace ${ENV} placeholders, return as dict."""
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
        logger.error(f"Config YAML parse FAILED — check config.yaml syntax | Error: {str(e)}")
        raise

    logger.info(f"Config loaded for env: {env}")
    return config


def get_s3_path(config, layer, table_name):
    """Build full S3 path from config bucket + table path for given layer."""
    try:
        bucket = config['s3'][f'{layer}_bucket']
        table_config = config['tables'][table_name]
        table_path = table_config[f'{layer}_path']
    except KeyError as e:
        logger.error(f"Config key missing for layer='{layer}', table='{table_name}' | Missing key: {str(e)}")
        raise

    full_path = f"{bucket}/{table_path}"
    return full_path


def get_table_config(config, table_name):
    if table_name not in config['tables']:
        raise ValueError(f"Table '{table_name}' not found in config!")
    return config['tables'][table_name]


def load_table_schema(config, table_name, s3_bucket=None):
    """Load schema JSON file for a table from local or S3, return as dict."""
    table_config = get_table_config(config, table_name)
    schema_file = table_config['schema_file']

    if s3_bucket:
        content = read_s3_file(s3_bucket, schema_file)
        try:
            schema = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Schema JSON parse FAILED for {table_name}: {schema_file} | Error: {str(e)}")
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
                logger.error(f"Schema JSON parse FAILED for {table_name}: {schema_path} | Error: {str(e)}")
                raise

    logger.info(f"Schema loaded for table: {table_name} ({len(schema.get('columns', []))} columns)")
    return schema


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    config = load_config()

    print("Project:", config['project']['name'])
    print("Environment:", config['project']['environment'])
    print("S3 Raw:", config['s3']['raw_bucket'])
    print("S3 Bronze:", config['s3']['bronze_bucket'])

    print("\nOrders raw path:", get_s3_path(config, 'raw', 'orders'))
    print("Orders bronze path:", get_s3_path(config, 'bronze', 'orders'))

    orders_config = get_table_config(config, "orders")
    print("\nOrders primary key:", orders_config['primary_key'])
    print("Orders partition:", orders_config['partition_column'])

    schema = load_table_schema(config, "orders")
    print("\nOrders schema columns:", [col['name'] for col in schema['columns']])
