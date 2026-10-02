"""Silver Processor — Business logic layer with incremental support."""

import logging
from pyspark.sql import functions as F
from src.utils.base_processor import BaseProcessor
from src.utils.config_loader import get_s3_path
from src.utils.watermark_manager import WatermarkManager

logger = logging.getLogger(__name__)


class SilverProcessor(BaseProcessor):
    """Joins Bronze tables, applies business rules, writes enriched data to Silver."""

    def __init__(self, spark, config, s3_bucket=None):
        super().__init__(spark, config, s3_bucket)

        silver_config = config.get('silver', {})
        source_tables = silver_config.get('source_tables', {})

        orders_table = source_tables.get('orders', 'orders')
        customers_table = source_tables.get('customers', 'customers')
        products_table = source_tables.get('products', 'products')

        self.orders_path = get_s3_path(config, 'bronze', orders_table)
        self.customers_path = get_s3_path(config, 'bronze', customers_table)
        self.products_path = get_s3_path(config, 'bronze', products_table)

        output_path = silver_config.get('output_path', 'enriched_orders/')
        self.silver_path = f"s3://{config['s3']['silver_bucket']}/{output_path}"

        self.load_mode = silver_config.get('load_mode', 'full')
        self.watermark = None

        if self.load_mode == 'incremental' and s3_bucket:
            manifest_prefix = config['s3'].get('manifest_prefix', 'manifests/')
            self.watermark = WatermarkManager(
                config['s3']['raw_bucket'], manifest_prefix, 'silver'
            )

        logger.info(
            f"🔧 {self.processor_name} initialized | mode={self.load_mode} | "
            f"orders={orders_table}, customers={customers_table}, products={products_table}"
        )

    @property
    def processor_name(self):
        return "SILVER"

    def _read_bronze(self):
        orders = self.spark.read.parquet(self.orders_path)
        customers = self.spark.read.parquet(self.customers_path)
        products = self.spark.read.parquet(self.products_path)

        total_orders = orders.count()
        logger.info(
            f"📥 Bronze read — orders: {total_orders}, "
            f"customers: {customers.count()}, products: {products.count()}"
        )

        if self.load_mode == "incremental" and self.watermark:
            watermark_ts = self.watermark.get_watermark()
            if watermark_ts:
                orders = orders.filter(
                    F.col("bronze_loaded_at") > F.lit(watermark_ts).cast("timestamp")
                )
                new_count = orders.count()
                logger.info(
                    f"📥 Incremental filter: {total_orders} total → "
                    f"{new_count} new (since {watermark_ts})"
                )

        return orders, customers, products

    def _drop_metadata(self, df):
        for col in ["bronze_loaded_at", "bronze_source_file", "dms_operation"]:
            if col in df.columns:
                df = df.drop(col)
        return df

    def _join_tables(self, orders, customers, products):
        df = (orders
              .join(customers, "customer_id", "inner")
              .join(products, "product_id", "inner"))
        logger.info(f"🔗 Joined: {df.count()} rows")
        return df

    def _apply_business_rules(self, df):
        before = df.count()

        df = df.filter(F.col("status") != "cancelled")

        if "is_active" in df.columns:
            df = df.filter(F.col("is_active") == True)

        after = df.count()
        logger.info(f"📏 Business filter: {before} → {after} ({before - after} removed)")

        df = (df
              .withColumn("gst_amount", F.round(F.col("total_amount") * 0.18, 2))
              .withColumn("grand_total", F.round(F.col("total_amount") + F.col("gst_amount"), 2)))
        logger.info("  Applied GST calculation")

        return df

    def _add_metadata(self, df):
        return df.withColumn("silver_loaded_at", F.current_timestamp())

    def _write(self, df):
        count = df.count()
        if count == 0:
            logger.warning(f"⚠️ EMPTY — skipping write to {self.silver_path}")
            return

        write_mode = "append" if self.load_mode == "incremental" else "overwrite"
        df.write.mode(write_mode).format("parquet").save(self.silver_path)
        logger.info(f"📤 Written {count} rows → {self.silver_path} | mode={write_mode}")

    def process(self):
        orders, customers, products = self._read_bronze()

        new_watermark = None
        if self.load_mode == "incremental" and self.watermark:
            if orders.count() == 0:
                logger.info("⏭️ No new orders — skipping Silver")
                return None
            max_ts = orders.agg(F.max("bronze_loaded_at")).collect()[0][0]
            new_watermark = max_ts.isoformat() if max_ts else None

        orders = self._drop_metadata(orders)
        customers = self._drop_metadata(customers)
        products = self._drop_metadata(products)

        df = self._join_tables(orders, customers, products)
        df = self._apply_business_rules(df)
        df = self._add_metadata(df)
        self._write(df)

        if new_watermark:
            self.watermark.update(new_watermark)

        return df


def process_silver(spark, config, s3_bucket=None):
    processor = SilverProcessor(spark, config, s3_bucket)
    return processor.run()
