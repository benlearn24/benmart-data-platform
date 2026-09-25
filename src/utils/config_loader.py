import os
import yaml
import json
import logging

logger = logging.getLogger(__name__)


def get_env():
    env = os.environ.get("ENV", "dev")
    logger.info(f"Environment detected: {env}")
    return env

def load_config(config_path=None):
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

    config = yaml.safe_load(config_text)
    logger.info(f"Config loaded for env: {env}")
    return config


def get_s3_path(config, layer, table_name):
    bucket = config['s3'][f'{layer}_bucket']
    table_config = config['tables'][table_name]
    table_path = table_config[f'{layer}_path']
    full_path = f"{bucket}/{table_path}"
    return full_path

def get_table_config(config, table_name):
    if table_name not in config['tables']:
        raise ValueError(f"Table '{table_name}' not found in config!")
    return config['tables'][table_name]


def load_table_schema(config, table_name):
    table_config = get_table_config(config, table_name)
    schema_file = table_config['schema_file']

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(current_dir, '..', '..')
    schema_path = os.path.join(project_root, schema_file)

    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)

    logger.info(f"Schema loaded for table: {table_name}")
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