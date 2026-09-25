import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from src.utils.config_loader import load_config, get_s3_path, load_table_schema

logger = logging.getLogger(__name__)


def build_schema(schema_json):
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

    record_count = df.count()
    logger.info(f"Raw records read: {record_count}")
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
    dupes_removed = before_count - after_count
    logger.info(f"Deduplication: {before_count} -> {after_count} ({dupes_removed} duplicates removed)")
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



def process_bronze(spark, config, table_name):
    logger.info(f"BRONZE PROCESSING: {table_name}")

    raw_path = get_s3_path(config, 'raw', table_name)
    bronze_path = get_s3_path(config, 'bronze', table_name)
    table_config = config['tables'][table_name]

    schema_json = load_table_schema(config, table_name)

    source_format = table_config.get('source_format', 'csv')
    df = read_raw_data(spark, raw_path, source_format)

    schema = build_schema(schema_json)
    df = apply_schema(df, schema)

    primary_key = table_config['primary_key']
    df = deduplicate(df, primary_key)

    df = add_metadata(df)

    partition_col = table_config.get('partition_column', None)
    write_bronze(df, bronze_path, partition_col)

    logger.info(f"BRONZE COMPLETE: {table_name}")
    return df


if __name__ == "__main__":
    import os

    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

    logging.basicConfig(level=logging.INFO)

    spark = (SparkSession.builder
             .master("local[*]")
             .appName("BenMart-Bronze")
             .config("spark.sql.warehouse.dir", "C:/tmp/spark-warehouse")
             .config("spark.driver.extraJavaOptions",
                     "-Dio.netty.tryReflectionSetAccessible=true -Djava.library.path=C:\\hadoop\\bin")
             .config("spark.hadoop.io.nativeio.enabled", "false")
             .getOrCreate())

    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    config = load_config()
    schema_json = load_table_schema(config, "orders")
    table_config = config['tables']['orders']

    raw_path = os.path.join(project_root, "data", "raw", "orders")
    bronze_path = os.path.join(project_root, "data", "bronze", "orders")

    print("\n===== STEP 1: Read Raw Data =====")
    df = read_raw_data(spark, raw_path, "csv")
    df.show()
    print(f"Rows: {df.count()}, Columns: {len(df.columns)}")

    print("\n===== STEP 2: Apply Schema =====")
    schema = build_schema(schema_json)
    df = apply_schema(df, schema)
    df.show()
    df.printSchema()

    print("\n===== STEP 3: Deduplicate =====")
    df = deduplicate(df, table_config['primary_key'])
    df.show()

    print("\n===== STEP 4: Add Metadata =====")
    df = add_metadata(df)
    df.show(truncate=False)

    print("\n===== STEP 5: Write Bronze =====")
    write_bronze(df, bronze_path, table_config.get('partition_column', None))
    print(f"Bronze data saved to: {bronze_path}")

    print("\n===== VERIFY: Read Back Bronze =====")
    bronze_df = spark.read.parquet(bronze_path)
    bronze_df.show(truncate=False)
    print(f"Bronze records: {bronze_df.count()}")

    spark.stop()
    print("\nBronze processing complete!")
    
    
