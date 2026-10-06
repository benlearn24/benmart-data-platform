
"""Config loader — reads config.yaml + JSON schemas, validates, builds paths."""

import os
import yaml
import json
import logging
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, DecimalType, BooleanType, DateType, TimestampType
)

logger = logging.getLogger(__name__)

# JSON schema type string → Spark type object
SPARK_TYPE_MAP = {
    "string": StringType(),
    "stringtype": StringType(),
    "integer": IntegerType(),
    "integertype": IntegerType(),
    "long": LongType(),
    "longtype": LongType(),
    "double": DoubleType(),
    "doubletype": DoubleType(),
    "decimal": DecimalType(10, 2),
    "decimaltype": DecimalType(10, 2),
    "boolean": BooleanType(),
    "booleantype": BooleanType(),
    "date": DateType(),
    "datetype": DateType(),
    "timestamp": TimestampType(),
    "timestamptype": TimestampType(),
}

# Every table must have these fields in config
REQUIRED_TABLE_FIELDS = [
    "source_format", "raw_path", "bronze_path",
    "primary_key", "load_mode", "schema_file"
]


def load_config(config_path="config/config.yaml"):
    """Load pipeline config from YAML. Validates required sections and table fields."""

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config not found: {config_path}")

    with open(config_path, "r") as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {config_path}: {e}")

    if not config or not isinstance(config, dict):
        raise ValueError(f"Config is empty or not a valid dict: {config_path}")

    required_sections = ["s3", "tables", "write", "silver", "gold"]
    missing = [s for s in required_sections if s not in config]
    if missing:
        raise ValueError(f"Config missing sections: {missing}")

    _validate_table_configs(config)

    table_count = len(config["tables"])
    write_fmt = config["write"]["format"]
    logger.info(f"Config loaded: {table_count} tables, format={write_fmt}")
    return config


def _validate_table_configs(config):
    """Check every table has required fields. Fail fast with clear message."""

    for table_name, table_cfg in config["tables"].items():
        missing = [f for f in REQUIRED_TABLE_FIELDS if f not in table_cfg]
        if missing:
            raise ValueError(f"Table '{table_name}' missing fields: {missing}")

        if table_cfg.get("has_cdc") and "dms_columns" not in table_cfg:
            raise ValueError(f"DMS table '{table_name}' missing 'dms_columns'")


def load_schema(schema_path):
    """Load JSON schema file and convert to Spark StructType."""

    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema not found: {schema_path}")

    with open(schema_path, "r") as f:
        try:
            schema_json = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {schema_path}: {e}")

    fields = schema_json.get("columns", schema_json.get("fields", []))
    if not fields:
        raise ValueError(f"Schema has no columns/fields: {schema_path}")

    struct_fields = []
    for field in fields:
        name = field["name"]
        type_str = field.get("type", "string").lower()
        nullable = field.get("nullable", True)

        if type_str.startswith("decimal"):
            try:
                parts = type_str.replace("decimaltype", "").replace("decimal", "").strip("()").split(",")
                precision = int(parts[0]) if parts[0] else 10
                scale = int(parts[1]) if len(parts) > 1 else 2
                spark_type = DecimalType(precision, scale)
            except (ValueError, IndexError):
                spark_type = DecimalType(10, 2)
        else:
            spark_type = SPARK_TYPE_MAP.get(type_str, StringType())

        struct_fields.append(StructField(name, spark_type, nullable))

    logger.info(f"Schema loaded: {schema_path} ({len(struct_fields)} fields)")
    return StructType(struct_fields)


def get_s3_path(config, layer, table_name):
    """Build full S3 path: s3://bucket/path/ for a given layer and table."""

    bucket_key = f"{layer}_bucket"
    bucket = config["s3"].get(bucket_key)
    if not bucket:
        raise ValueError(f"Bucket not found for layer '{layer}': key '{bucket_key}'")

    table_cfg = config["tables"].get(table_name)
    if not table_cfg:
        raise ValueError(f"Table '{table_name}' not found in config")

    path = table_cfg.get(f"{layer}_path", table_cfg.get("bronze_path", f"{table_name}/"))
    return f"s3://{bucket}/{path}"


def get_write_config(config, table_name):
    """Collect write settings for a table — format, mode, partitions, delta options."""

    write_cfg = config["write"]
    table_cfg = config["tables"].get(table_name, {})
    load_mode = table_cfg.get("load_mode", "full")

    mode = write_cfg["mode"].get(load_mode, "overwrite")
    partitions = write_cfg.get("partition_columns", {}).get(table_name)

    return {
        "format": write_cfg["format"],
        "mode": mode,
        "partition_columns": partitions if partitions else [],
        "delta_options": write_cfg.get("delta_options", {}),
    }


def get_quality_thresholds(config, table_name):
    """Get quality gate thresholds for a table. Returns empty dict if not defined."""

    return config.get("quality", {}).get("thresholds", {}).get(table_name, {})
