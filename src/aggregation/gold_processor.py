from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


def read_silver_data(spark: SparkSession, silver_path: str) -> DataFrame:
    """Read enriched data from Silver layer"""
    logger.info(f"Reading silver data from: {silver_path}")
    df = spark.read.parquet(silver_path)
    record_count = df.count()
    logger.info(f"Silver data loaded: {record_count} records")
    return df


def create_fact_orders(silver_df: DataFrame) -> DataFrame:
    """Create fact_orders — measures + foreign keys only"""
    logger.info("Creating fact_orders...")

    fact_df = silver_df.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("product_id"),
        F.col("order_date"),
        F.col("total_amount"),
        F.col("total_with_gst")
    )

    logger.info(f"fact_orders created: {fact_df.count()} records")
    return fact_df


def create_dim_customers(silver_df: DataFrame) -> DataFrame:
    """Create dim_customers — one row per unique customer"""
    logger.info("Creating dim_customers...")

    dim_df = silver_df.select(
        F.col("customer_id"),
        F.col("customer_name"),
        F.col("city"),
        F.col("registered_date")
    ).dropDuplicates(["customer_id"])

    logger.info(f"dim_customers created: {dim_df.count()} unique customers")
    return dim_df


def create_dim_products(silver_df: DataFrame) -> DataFrame:
    """Create dim_products — one row per unique product"""
    logger.info("Creating dim_products...")

    dim_df = silver_df.select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("price")
    ).dropDuplicates(["product_id"])

    logger.info(f"dim_products created: {dim_df.count()} unique products")
    return dim_df


def create_dim_date(silver_df: DataFrame) -> DataFrame:
    """Create dim_date — one row per unique date with derived attributes"""
    logger.info("Creating dim_date...")

    dim_df = silver_df.select(
        F.col("order_date")
    ).dropDuplicates(["order_date"])

    dim_df = dim_df.select(
        F.col("order_date").alias("date"),
        F.dayofweek(F.col("order_date")).alias("day_of_week"),
        F.date_format(F.col("order_date"), "EEEE").alias("day_name"),
        F.month(F.col("order_date")).alias("month"),
        F.date_format(F.col("order_date"), "MMMM").alias("month_name"),
        F.quarter(F.col("order_date")).alias("quarter"),
        F.year(F.col("order_date")).alias("year"),
        F.when(
            F.dayofweek(F.col("order_date")).isin(1, 7), True
        ).otherwise(False).alias("is_weekend")
    )

    logger.info(f"dim_date created: {dim_df.count()} unique dates")
    return dim_df


def create_agg_daily_revenue(fact_df: DataFrame) -> DataFrame:
    """Pre-calculate daily revenue totals"""
    logger.info("Creating agg_daily_revenue...")

    agg_df = fact_df.groupBy("order_date").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.sum("total_with_gst").alias("total_revenue_with_gst"),
        F.avg("total_amount").alias("avg_order_value")
    ).orderBy("order_date")

    logger.info(f"agg_daily_revenue created: {agg_df.count()} days")
    return agg_df


def create_agg_city_orders(fact_df: DataFrame, dim_customers_df: DataFrame) -> DataFrame:
    """Pre-calculate city wise order summary"""
    logger.info("Creating agg_city_orders...")

    city_df = fact_df.join(
        dim_customers_df.select("customer_id", "city"),
        on="customer_id",
        how="inner"
    )

    agg_df = city_df.groupBy("city").agg(
        F.count("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value")
    ).orderBy(F.desc("total_revenue"))

    logger.info(f"agg_city_orders created: {agg_df.count()} cities")
    return agg_df


def create_agg_product_sales(fact_df: DataFrame, dim_products_df: DataFrame) -> DataFrame:
    """Pre-calculate product wise sales summary"""
    logger.info("Creating agg_product_sales...")

    product_df = fact_df.join(
        dim_products_df.select("product_id", "product_name", "category"),
        on="product_id",
        how="inner"
    )

    agg_df = product_df.groupBy("product_id", "product_name", "category").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value")
    ).orderBy(F.desc("total_revenue"))

    logger.info(f"agg_product_sales created: {agg_df.count()} products")
    return agg_df


def write_gold(df: DataFrame, gold_path: str, table_name: str) -> None:
    """Write a Gold table to S3 as Parquet"""
    output_path = f"{gold_path}/{table_name}"
    logger.info(f"Writing {table_name} to: {output_path}")

    df.write.mode("overwrite").parquet(output_path)

    logger.info(f"{table_name} written successfully: {df.count()} records")


def process_gold(spark: SparkSession, config: dict, s3_bucket: str = None) -> None:
    """Orchestrate complete Gold layer processing"""
    logger.info("=" * 50)
    logger.info("GOLD LAYER PROCESSING STARTED")
    logger.info("=" * 50)

    # Step 1 — Read Silver data
    # Step 1 — Read Silver data
    silver_input = config['gold']['silver_input_path']
    if s3_bucket:
        silver_path = f"{config['s3']['silver_bucket']}/{silver_input}"
    else:
        silver_path = f"data/silver/{silver_input}"
    silver_df = read_silver_data(spark, silver_path)

    # Step 2 — Create Fact table
    fact_df = create_fact_orders(silver_df)

    # Step 3 — Create Dimension tables
    dim_customers_df = create_dim_customers(silver_df)
    dim_products_df = create_dim_products(silver_df)
    dim_date_df = create_dim_date(silver_df)

    # Step 4 — Create Aggregation tables
    agg_daily_df = create_agg_daily_revenue(fact_df)
    agg_city_df = create_agg_city_orders(fact_df, dim_customers_df)
    agg_product_df = create_agg_product_sales(fact_df, dim_products_df)

    # Step 5 — Write all tables to Gold
    if s3_bucket:
        gold_path = config['s3']['gold_bucket']
    else:
        gold_path = "data/gold"

    gold_tables = {
        "fact_orders": fact_df,
        "dim_customers": dim_customers_df,
        "dim_products": dim_products_df,
        "dim_date": dim_date_df,
        "agg_daily_revenue": agg_daily_df,
        "agg_city_orders": agg_city_df,
        "agg_product_sales": agg_product_df
    }

    for table_name, df in gold_tables.items():
        write_gold(df, gold_path, table_name)

    logger.info("=" * 50)
    logger.info("GOLD LAYER PROCESSING COMPLETED")
    logger.info(f"Total tables written: {len(gold_tables)}")
    logger.info("=" * 50)