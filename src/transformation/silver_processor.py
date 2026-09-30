import logging
from pyspark.sql import functions as F
from src.utils.base_processor import BaseProcessor
from src.utils.config_loader import get_s3_path

logger = logging.getLogger(__name__)


class SilverProcessor(BaseProcessor):

    METADATA_COLS = ["bronze_loaded_at", "bronze_source_file"]

    def __init__(self, spark, config, s3_bucket=None):
        super().__init__(spark, config, s3_bucket)
        self.orders_path = get_s3_path(config, 'bronze', 'orders')
        self.customers_path = get_s3_path(config, 'bronze', 'customers')
        self.products_path = get_s3_path(config, 'bronze', 'products')
        self.silver_path = get_s3_path(config, 'silver', 'orders')

    @property
    def processor_name(self):
        return "SILVER: enriched_orders"

    def _read_bronze(self, path):
        logger.info(f"Reading bronze: {path}")
        try:
            df = self.spark.read.parquet(path)
        except Exception as e:
            logger.error(f"Bronze read FAILED: {path} | {str(e)}")
            raise
        count = df.count()
        if count == 0:
            logger.warning(f"EMPTY bronze data: {path}")
        logger.info(f"Bronze records: {count}")
        return df

    def _drop_metadata(self, df):
        for col in self.METADATA_COLS:
            if col in df.columns:
                df = df.drop(col)
        return df

    def _join_tables(self, orders_df, customers_df, products_df):
        logger.info("Joining orders + customers + products...")

        customers_df = self._drop_metadata(customers_df)
        products_df = self._drop_metadata(products_df)

        try:
            enriched_df = (orders_df
                           .join(customers_df, "customer_id", "inner")
                           .join(products_df, "product_id", "inner"))
        except Exception as e:
            logger.error(f"JOIN failed: {str(e)}")
            raise

        joined = enriched_df.count()
        original = orders_df.count()
        if joined < original:
            logger.warning(f"JOIN dropped {original - joined} orders (key mismatch)")
        logger.info(f"Joined records: {joined}")
        return enriched_df

    @staticmethod
    def _apply_business_rules(df):
        logger.info("Applying business rules...")
        before = df.count()

        df = df.filter(F.lower(F.col("status")) != "cancelled")
        df = df.filter(F.col("is_active") == True)
        df = df.withColumn("total_with_gst", F.round(F.col("total_amount") * 1.18, 2))

        after = df.count()
        logger.info(f"Business rules: {before} → {after} ({before - after} removed)")
        return df

    @staticmethod
    def _add_metadata(df):
        return df.withColumn("silver_loaded_at", F.current_timestamp())

    def _write(self, df, path):
        count = df.count()
        if count == 0:
            logger.warning(f"EMPTY — skip write: {path}")
            return

        try:
            df.write.mode("overwrite").format("parquet").save(path)
        except Exception as e:
            logger.error(f"Silver write FAILED: {path} | {str(e)}")
            raise
        logger.info(f"Written: {path} ({count} rows)")

    def process(self):
        orders_df = self._read_bronze(self.orders_path)
        customers_df = self._read_bronze(self.customers_path)
        products_df = self._read_bronze(self.products_path)

        df = self._join_tables(orders_df, customers_df, products_df)
        df = self._apply_business_rules(df)
        df = self._add_metadata(df)
        self._write(df, self.silver_path)
        return df


def process_silver(spark, config, s3_bucket=None):
    processor = SilverProcessor(spark, config, s3_bucket)
    return processor.run()
