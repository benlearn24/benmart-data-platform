import logging
from pyspark.sql import functions as F
from src.utils.config_loader import load_config, get_s3_path, get_table_config

logger = logging.getLogger(__name__)

def read_bronze_data(spark, bronze_path):
    logger.info(f"Reading bronze data from: {bronze_path}")
    df = spark.read.parquet(bronze_path)
    logger.info(f"Bronze records read: {df.count()}")
    return df

def join_tables(orders_df, customers_df, products_df):
    logger.info("Joining orders with customers and products...")

    customers_df = customers_df.drop("bronze_loaded_at", "bronze_source_file")
    products_df = products_df.drop("bronze_loaded_at", "bronze_source_file")

    enriched_df = (orders_df
                   .join(customers_df, "customer_id", "inner")
                   .join(products_df, "product_id", "inner"))

    logger.info(f"Joined records: {enriched_df.count()}")
    return enriched_df

def apply_business_rules(df):
    logger.info("Applying business rules...")
    before_count = df.count()

    df = df.filter(F.col("status") != "cancelled")
    df = df.filter(F.col("is_active") == True)
    df = df.withColumn("total_with_gst", F.round(F.col("total_amount") * 1.18, 2))

    after_count = df.count()
    logger.info(f"Business rules applied: {before_count} -> {after_count}")
    return df

def add_metadata(df):
    df = df.withColumn("silver_loaded_at", F.current_timestamp())
    return df


def write_silver(df, silver_path):
    logger.info(f"Writing silver data to: {silver_path}")
    df.write.mode("overwrite").format("parquet").save(silver_path)
    logger.info(f"Silver write complete: {silver_path}")


def process_silver(spark, config, s3_bucket=None):
    logger.info("SILVER PROCESSING: enriched_orders")

    orders_path = get_s3_path(config, 'bronze', 'orders')
    customers_path = get_s3_path(config, 'bronze', 'customers')
    products_path = get_s3_path(config, 'bronze', 'products')
    silver_path = get_s3_path(config, 'silver', 'orders')

    orders_df = read_bronze_data(spark, orders_path)
    customers_df = read_bronze_data(spark, customers_path)
    products_df = read_bronze_data(spark, products_path)

    df = join_tables(orders_df, customers_df, products_df)
    df = apply_business_rules(df)
    df = add_metadata(df)

    write_silver(df, silver_path)

    logger.info("SILVER COMPLETE: enriched_orders")
    return df