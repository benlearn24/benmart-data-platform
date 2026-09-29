"""
bronze_processor.py — Bronze layer processing for BenMart Data Platform.

Reads raw CSV/JSON from S3, applies schema (STRING → correct types),
deduplicates on primary key, adds metadata columns, writes Parquet to Bronze bucket.

Called by: glue_bronze_job.py (via process_bronze orchestrator)
Input: S3 Raw bucket CSV files
Output: S3 Bronze bucket Parquet files (typed, deduped, partitioned)
"""

import logging
from pyspark.sql import functions as F
from pyspark.sql.types import *
from src.utils.config_loader import get_s3_path, load_table_schema

logger = logging.getLogger(__name__)


def build_schema(schema_json):
    """Convert schema JSON (column names + type strings) to PySpark StructType."""
    type_mapping = {
        "StringType": StringType(),
        "IntegerType": IntegerType(),
        "LongType": LongType(),
        "DoubleType": DoubleType(),
        "DecimalType": DecimalType(10, 2),
        "DateType": DateType(),
        "TimestampType": TimestampType(),
        "BooleanType": BooleanType(),
    }

    fields = []
    for col in schema_json["columns"]:
        spark_type = type_mapping.get(col["type"], StringType())
        nullable = col.get("nullable", True)
        fields.append(StructField(col["name"], spark_type, nullable))

    logger.info(f"Schema built: {len(fields)} columns")
    return StructType(fields)


def read_raw_data(spark, raw_path, source_format="csv"):
    """Read raw CSV/JSON from S3 path, return DataFrame with all STRING columns."""
    logger.info(f"Reading raw data from: {raw_path}")

    try:
        if source_format == "csv":
            df = (spark.read
                  .option("header", "true")
                  .option("inferSchema", "false")
                  .csv(raw_path))
        elif source_format == "json":
            df = spark.read.json(raw_path)
        else:
            raise ValueError(f"Unsupported format: {source_format}. Supported: csv, json")
    except Exception as e:
        logger.error(f"Failed to read raw data from: {raw_path} | Format: {source_format} | Error: {str(e)}")
        raise

    record_count = df.count()

    if record_count == 0:
        logger.warning(f"Raw data is EMPTY: {raw_path}. Check if files were uploaded to S3.")

    logger.info(f"Raw records read: {record_count}")
    return df


def apply_schema(df, schema):
    """Cast DataFrame columns from STRING to correct types using PySpark StructType."""
    logger.info(f"Applying schema: {len(schema.fields)} columns")

    try:
        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
            else:
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
                logger.warning(f"Column '{field.name}' not in raw data — added as NULL")

        schema_columns = [field.name for field in schema.fields]
        df = df.select(schema_columns)
    except Exception as e:
        logger.error(f"Schema apply FAILED: {str(e)}")
        raise

    logger.info(f"Schema applied successfully: {len(schema_columns)} columns selected")
    return df


def deduplicate(df, primary_key):
    """Remove duplicate rows based on primary key, keep first occurrence."""
    logger.info(f"Deduplicating on: {primary_key}")

    if primary_key not in df.columns:
        logger.error(f"Primary key '{primary_key}' not found in DataFrame columns: {df.columns}")
        raise ValueError(f"Primary key '{primary_key}' not found in DataFrame")

    before_count = df.count()
    df = df.dropDuplicates([primary_key])
    after_count = df.count()
    dupes_removed = before_count - after_count
    logger.info(f"Deduplication: {before_count} -> {after_count} ({dupes_removed} duplicates removed)")
    return df


def add_metadata(df):
    """Add bronze_loaded_at timestamp and bronze_source_file columns."""
    df = (df
          .withColumn("bronze_loaded_at", F.current_timestamp())
          .withColumn("bronze_source_file", F.input_file_name()))
    logger.info("Metadata columns added: bronze_loaded_at, bronze_source_file")
    return df


def write_bronze(df, bronze_path, partition_column=None):
    """Write DataFrame to Bronze S3 path as Parquet, optional partition."""
    logger.info(f"Writing bronze data to: {bronze_path}")

    row_count = df.count()
    if row_count == 0:
        logger.warning(f"Bronze DataFrame is EMPTY — nothing to write to {bronze_path}")
        return

    try:
        writer = df.write.mode("overwrite").format("parquet")

        if partition_column:
            writer = writer.partitionBy(partition_column)
            logger.info(f"Partitioning by: {partition_column}")

        writer.save(bronze_path)
    except Exception as e:
        logger.error(f"Bronze write FAILED to {bronze_path}: {str(e)}")
        raise

    logger.info(f"Bronze write complete: {bronze_path} ({row_count} rows)")


def process_bronze(spark, config, table_name, s3_bucket=None):
    """Orchestrate full Bronze processing for one table — read, schema, dedup, metadata, write."""
    logger.info(f"BRONZE PROCESSING: {table_name}")

    try:
        raw_path = get_s3_path(config, 'raw', table_name)
        bronze_path = get_s3_path(config, 'bronze', table_name)
        table_config = config['tables'][table_name]

        schema_json = load_table_schema(config, table_name, s3_bucket=s3_bucket)

        source_format = table_config.get('source_format', 'csv')
        df = read_raw_data(spark, raw_path, source_format)

        schema = build_schema(schema_json)
        df = apply_schema(df, schema)

        primary_key = table_config['primary_key']
        df = deduplicate(df, primary_key)

        df = add_metadata(df)

        partition_col = table_config.get('partition_column', None)
        write_bronze(df, bronze_path, partition_col)

    except Exception as e:
        logger.error(f"BRONZE FAILED for table '{table_name}': {str(e)}")
        raise

    logger.info(f"BRONZE COMPLETE: {table_name}")
    return df
