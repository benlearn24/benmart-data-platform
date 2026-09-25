import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV'])
env = args['ENV']

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logger.info(f"Starting Bronze Processing - Environment: {env}")



RAW_BUCKET = f"s3a://benmart-{env}-raw"
BRONZE_BUCKET = f"s3a://benmart-{env}-bronze"

TABLES = {
    "orders": {
        "primary_key": "order_id",
        "partition_column": "order_date",
        "source_format": "csv",
        "raw_path": "orders/",
        "bronze_path": "orders/",
        "schema": [
            ("order_id", "IntegerType", False),
            ("customer_id", "IntegerType", False),
            ("order_date", "DateType", False),
            ("total_amount", "DecimalType", False),
            ("status", "StringType", True),
        ]
    },
    "customers": {
        "primary_key": "customer_id",
        "partition_column": None,
        "source_format": "csv",
        "raw_path": "customers/",
        "bronze_path": "customers/",
        "schema": [
            ("customer_id", "IntegerType", False),
            ("customer_name", "StringType", False),
            ("email", "StringType", True),
            ("phone", "StringType", True),
            ("city", "StringType", False),
            ("registered_date", "DateType", False),
        ]
    },
    "products": {
        "primary_key": "product_id",
        "partition_column": None,
        "source_format": "csv",
        "raw_path": "products/",
        "bronze_path": "products/",
        "schema": [
            ("product_id", "IntegerType", False),
            ("product_name", "StringType", False),
            ("category", "StringType", False),
            ("price", "DecimalType", False),
            ("is_active", "BooleanType", False),
        ]
    }
}


TYPE_MAP = {
    "StringType": StringType(),
    "IntegerType": IntegerType(),
    "LongType": LongType(),
    "DoubleType": DoubleType(),
    "DecimalType": DecimalType(10, 2),
    "DateType": DateType(),
    "TimestampType": TimestampType(),
    "BooleanType": BooleanType(),
}


def build_schema(schema_list):
    fields = [StructField(name, TYPE_MAP[t], n) for name, t, n in schema_list]
    return StructType(fields)


def read_raw_data(spark, raw_path, source_format="csv"):
    logger.info(f"Reading raw data from: {raw_path}")
    if source_format == "csv":
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "false")
              .csv(raw_path))
    elif source_format == "json":
        df = spark.read.json(raw_path)
    else:
        raise ValueError(f"Unsupported format: {source_format}")
    logger.info(f"Raw records read: {df.count()}")
    return df


def apply_schema(df, schema):
    for field in schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
        else:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    schema_columns = [field.name for field in schema.fields]
    df = df.select(schema_columns)
    return df


def deduplicate(df, primary_key):
    before_count = df.count()
    df = df.dropDuplicates([primary_key])
    after_count = df.count()
    logger.info(f"Deduplication: {before_count} -> {after_count} ({before_count - after_count} removed)")
    return df


def add_metadata(df):
    df = (df
          .withColumn("bronze_loaded_at", F.current_timestamp())
          .withColumn("bronze_source_file", F.input_file_name()))
    return df


def write_bronze(df, bronze_path, partition_column=None):
    logger.info(f"Writing bronze data to: {bronze_path}")
    writer = df.write.mode("overwrite").format("parquet")
    if partition_column:
        writer = writer.partitionBy(partition_column)
    writer.save(bronze_path)
    logger.info(f"Bronze write complete: {bronze_path}")


for table_name, table_config in TABLES.items():
    logger.info(f"{'='*50}")
    logger.info(f"BRONZE PROCESSING: {table_name}")
    logger.info(f"{'='*50}")

    raw_path = f"{RAW_BUCKET}/{table_config['raw_path']}"
    bronze_path = f"{BRONZE_BUCKET}/{table_config['bronze_path']}"

    df = read_raw_data(spark, raw_path, table_config['source_format'])

    schema = build_schema(table_config['schema'])
    df = apply_schema(df, schema)

    df = deduplicate(df, table_config['primary_key'])

    df = add_metadata(df)

    write_bronze(df, bronze_path, table_config['partition_column'])

    logger.info(f"BRONZE COMPLETE: {table_name}")

job.commit()
logger.info("All bronze processing complete!")