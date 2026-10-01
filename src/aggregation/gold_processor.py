"""Gold Processor — Star schema and aggregations."""

import logging
from pyspark.sql import functions as F
from src.utils.base_processor import BaseProcessor

logger = logging.getLogger(__name__)


class GoldProcessor(BaseProcessor):

    GOLD_TABLES = [
        "fact_orders", "dim_customers", "dim_products", "dim_date",
        "agg_daily_revenue", "agg_city_orders", "agg_product_sales"
    ]

    def __init__(self, spark, config, s3_bucket=None):
        super().__init__(spark, config, s3_bucket)

        silver_input = config['gold']['silver_input_path']
        if s3_bucket:
            self.silver_path = f"s3://{config['s3']['silver_bucket']}/{silver_input}"
            self.gold_path = f"s3://{config['s3']['gold_bucket']}"
        else:
            self.silver_path = f"data/silver/{silver_input}"
            self.gold_path = "data/gold"

    @property
    def processor_name(self):
        return "GOLD: star_schema"

    def _read_silver(self):
        logger.info(f"Reading silver: {self.silver_path}")
        try:
            df = self.spark.read.parquet(self.silver_path)
        except Exception as e:
            logger.error(f"Silver read FAILED: {self.silver_path} | {e}")
            raise
        count = df.count()
        if count == 0:
            logger.warning(f"EMPTY silver data: {self.silver_path}")
        logger.info(f"Silver records: {count}")
        return df

    @staticmethod
    def _create_fact_orders(silver_df):
        logger.info("Creating fact_orders...")
        fact_columns = ["order_id", "customer_id", "product_id", "order_date", "total_amount", "grand_total"]
        fact_df = silver_df.select([F.col(c) for c in fact_columns])
        logger.info(f"fact_orders: {fact_df.count()} records")
        return fact_df

    @staticmethod
    def _create_dim_customers(silver_df):
        logger.info("Creating dim_customers...")
        dim_columns = ["customer_id", "customer_name", "city", "registered_date"]
        dim_df = silver_df.select([F.col(c) for c in dim_columns]).dropDuplicates(["customer_id"])
        logger.info(f"dim_customers: {dim_df.count()} unique")
        return dim_df

    @staticmethod
    def _create_dim_products(silver_df):
        logger.info("Creating dim_products...")
        dim_columns = ["product_id", "product_name", "category", "price"]
        dim_df = silver_df.select([F.col(c) for c in dim_columns]).dropDuplicates(["product_id"])
        logger.info(f"dim_products: {dim_df.count()} unique")
        return dim_df

    @staticmethod
    def _create_dim_date(silver_df):
        logger.info("Creating dim_date...")
        dim_df = silver_df.select(F.col("order_date")).dropDuplicates(["order_date"])
        dim_df = dim_df.select(
            F.col("order_date").alias("date"),
            F.dayofweek(F.col("order_date")).alias("day_of_week"),
            F.date_format(F.col("order_date"), "EEEE").alias("day_name"),
            F.month(F.col("order_date")).alias("month"),
            F.date_format(F.col("order_date"), "MMMM").alias("month_name"),
            F.quarter(F.col("order_date")).alias("quarter"),
            F.year(F.col("order_date")).alias("year"),
            F.when(F.dayofweek(F.col("order_date")).isin(1, 7), True).otherwise(False).alias("is_weekend")
        )
        logger.info(f"dim_date: {dim_df.count()} unique dates")
        return dim_df

    @staticmethod
    def _create_agg_daily_revenue(fact_df):
        logger.info("Creating agg_daily_revenue...")
        agg_df = fact_df.groupBy("order_date").agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
            F.sum("grand_total").alias("total_revenue_with_gst"),
            F.avg("total_amount").alias("avg_order_value")
        ).orderBy("order_date")
        logger.info(f"agg_daily_revenue: {agg_df.count()} days")
        return agg_df

    @staticmethod
    def _create_agg_city_orders(fact_df, dim_customers_df):
        logger.info("Creating agg_city_orders...")
        city_df = fact_df.join(
            dim_customers_df.select("customer_id", "city"),
            on="customer_id", how="inner"
        )
        agg_df = city_df.groupBy("city").agg(
            F.count("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value")
        ).orderBy(F.desc("total_revenue"))
        logger.info(f"agg_city_orders: {agg_df.count()} cities")
        return agg_df

    @staticmethod
    def _create_agg_product_sales(fact_df, dim_products_df):
        logger.info("Creating agg_product_sales...")
        product_df = fact_df.join(
            dim_products_df.select("product_id", "product_name", "category"),
            on="product_id", how="inner"
        )
        agg_df = product_df.groupBy("product_id", "product_name", "category").agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value")
        ).orderBy(F.desc("total_revenue"))
        logger.info(f"agg_product_sales: {agg_df.count()} products")
        return agg_df

    def _write_table(self, df, table_name):
        output_path = f"{self.gold_path}/{table_name}"
        count = df.count()
        if count == 0:
            logger.warning(f"EMPTY — skip write: {table_name}")
            return
        try:
            df.write.mode("overwrite").parquet(output_path)
        except Exception as e:
            logger.error(f"Write FAILED: {table_name} → {output_path} | {e}")
            raise
        logger.info(f"Written: {table_name} ({count} rows)")

    def process(self):
        silver_df = self._read_silver()

        fact_df = self._create_fact_orders(silver_df)
        dim_customers_df = self._create_dim_customers(silver_df)
        dim_products_df = self._create_dim_products(silver_df)
        dim_date_df = self._create_dim_date(silver_df)

        agg_daily_df = self._create_agg_daily_revenue(fact_df)
        agg_city_df = self._create_agg_city_orders(fact_df, dim_customers_df)
        agg_product_df = self._create_agg_product_sales(fact_df, dim_products_df)

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
            self._write_table(df, table_name)

        logger.info(f"Total tables written: {len(gold_tables)}")
        return gold_tables


def process_gold(spark, config, s3_bucket=None):
    processor = GoldProcessor(spark, config, s3_bucket)
    return processor.run()
