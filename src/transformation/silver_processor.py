import logging
from pyspark.sql import functions as F
from src.utils.config_loader import get_s3_path

logger = logging.getLogger(__name__)


def read_bronze_data(spark, bronze_path):
    """Read Parquet data from Bronze layer S3 path, return DataFrame."""
    logger.info(f"Reading bronze data from: {bronze_path}")

    try:
        df = spark.read.parquet(bronze_path)
    except Exception as e:
        logger.error(f"Failed to read Bronze data from: {bronze_path} | Error: {str(e)}")
        raise

    record_count = df.count()

    if record_count == 0:
        logger.warning(f"Bronze data is EMPTY: {bronze_path}. Check if Bronze job ran successfully.")

    logger.info(f"Bronze records read: {record_count}")
    return df


def join_tables(orders_df, customers_df, products_df):
    """Join orders with customers and products on foreign keys, return enriched DataFrame."""
    logger.info("Joining orders with customers and products...")

    # Drop bronze metadata from dimension tables — prevent COLUMN_ALREADY_EXISTS error
    metadata_cols = ["bronze_loaded_at", "bronze_source_file"]

    for col in metadata_cols:
        if col in customers_df.columns:
            customers_df = customers_df.drop(col)
        if col in products_df.columns:
            products_df = products_df.drop(col)

    try:
        enriched_df = (orders_df
                       .join(customers_df, "customer_id", "inner")
                       .join(products_df, "product_id", "inner"))
    except Exception as e:
        logger.error(f"JOIN failed: {str(e)}")
        raise

    joined_count = enriched_df.count()
    orders_count = orders_df.count()

    if joined_count < orders_count:
        dropped = orders_count - joined_count
        logger.warning(f"JOIN dropped {dropped} orders (customer_id or product_id mismatch)")

    logger.info(f"Joined records: {joined_count}")
    return enriched_df


def apply_business_rules(df):
    """Filter cancelled orders + inactive products, calculate GST column."""
    logger.info("Applying business rules...")
    before_count = df.count()

    try:
        # Rule 1: Remove cancelled orders (case-insensitive check)
        df = df.filter(F.lower(F.col("status")) != "cancelled")

        # Rule 2: Remove inactive products
        df = df.filter(F.col("is_active") == True)

        # Rule 3: Calculate GST (18%)
        df = df.withColumn("total_with_gst", F.round(F.col("total_amount") * 1.18, 2))

    except Exception as e:
        logger.error(f"Business rules FAILED: {str(e)}")
        raise

    after_count = df.count()
    removed = before_count - after_count
    logger.info(f"Business rules applied: {before_count} -> {after_count} (removed {removed} rows)")
    return df


def add_metadata(df):
    """Add silver_loaded_at timestamp column for pipeline tracking."""
    df = df.withColumn("silver_loaded_at", F.current_timestamp())
    return df


def write_silver(df, silver_path):
    """Write enriched DataFrame to Silver S3 path as Parquet."""
    logger.info(f"Writing silver data to: {silver_path}")

    row_count = df.count()
    if row_count == 0:
        logger.warning(f"Silver DataFrame is EMPTY — nothing to write to {silver_path}")
        return

    try:
        df.write.mode("overwrite").format("parquet").save(silver_path)
    except Exception as e:
        logger.error(f"Silver write FAILED to {silver_path}: {str(e)}")
        raise

    logger.info(f"Silver write complete: {silver_path} ({row_count} rows)")


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
