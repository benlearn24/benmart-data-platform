import logging
from functools import wraps
from pyspark.sql import functions as F
from pyspark.sql.types import *
from src.utils.base_processor import BaseProcessor
from src.utils.config_loader import get_s3_path, load_table_schema

logger = logging.getLogger(__name__)


def quarantine(write_path_attr="quarantine_path"):

    def decorator(validate_func):
        @wraps(validate_func)
        def wrapper(self, df):
            good_df, bad_df, report = validate_func(self, df)

            bad_count = bad_df.count()
            if bad_count > 0:
                q_path = getattr(self, write_path_attr)
                bad_df.write.mode("overwrite").format("parquet").save(q_path)
                logger.warning(f"Quarantined: {bad_count} rows → {q_path}")

            good_count = good_df.count()
            if good_count == 0:
                raise ValueError(f"QUALITY GATE FAILED: {self.table_name} — ALL rows bad!")

            logger.info(f"✅ {good_count} rowss passed quality checks")
            return good_df

        return wrapper
    return decorator


class BronzeProcessor(BaseProcessor):

    SUPPORTED_FORMATS = {"csv", "json", "parquet"}

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

    def __init__(self, spark, config, table_name, s3_bucket=None):
        super().__init__(spark, config, s3_bucket)
        self.table_name = table_name
        self.table_config = config['tables'][table_name]
        self.source_format = self.table_config.get('source_format', 'csv')
        self.read_options = self.table_config.get('read_options', {})
        self.primary_key = self.table_config['primary_key']
        self.partition_column = self.table_config.get('partition_column', None)
        self.raw_path = get_s3_path(config, 'raw', table_name)
        self.bronze_path = get_s3_path(config, 'bronze', table_name)

        if s3_bucket:
            self.quarantine_path = f"{config['s3']['raw_bucket']}/quarantine/{table_name}"
        else:
            self.quarantine_path = f"data/quarantine/{table_name}"

    @property
    def processor_name(self):
        return f"BRONZE: {self.table_name}"

    def _read_raw(self):
        logger.info(f"Reading: {self.raw_path} | Format: {self.source_format}")

        if self.source_format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.source_format}. Supported: {self.SUPPORTED_FORMATS}")

        try:
            if self.source_format == "csv":
                reader = self.spark.read
                for key, value in self.read_options.items():
                    reader = reader.option(key, value)
                df = reader.csv(self.raw_path)

            elif self.source_format == "json":
                reader = self.spark.read
                for key, value in self.read_options.items():
                    reader = reader.option(key, value)
                df = reader.json(self.raw_path)

            elif self.source_format == "parquet":
                df = self.spark.read.parquet(self.raw_path)

        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Read FAILED: {self.raw_path} | {self.source_format} | {str(e)}")
            raise

        count = df.count()
        if count == 0:
            logger.warning(f"EMPTY data: {self.raw_path}")
        logger.info(f"Raw records: {count}")
        return df

    def _clean_data(self, df):
        logger.info(f"Cleaning: {self.table_name} | Format: {self.source_format}")
        before_cols = df.columns[:]

        if "address" in df.columns:
            df = df.withColumn("city", F.col("address.city"))
            df = df.withColumn("state", F.col("address.state"))
            df = df.withColumn("pincode", F.col("address.pincode"))
            df = df.drop("address")
            logger.info("Flattened: address → city, state, pincode")

        if "phone" in df.columns:
            phone_type = df.schema["phone"].dataType
            if isinstance(phone_type, ArrayType):
                df = df.withColumn("phone", F.col("phone").getItem(0))
                logger.info("Extracted: phone[0] from array")

        for col_name in df.columns:
            if isinstance(df.schema[col_name].dataType, StringType):
                df = df.withColumn(col_name, F.trim(F.col(col_name)))

        after_cols = df.columns[:]
        logger.info(f"Clean done: {before_cols} → {after_cols}")
        return df

    def _build_schema(self):
        schema_json = load_table_schema(self.config, self.table_name, s3_bucket=self.s3_bucket)
        fields = []
        for col in schema_json["columns"]:
            spark_type = self.TYPE_MAP.get(col["type"], StringType())
            nullable = col.get("nullable", True)
            fields.append(StructField(col["name"], spark_type, nullable))
        logger.info(f"Schema built: {len(fields)} columns")
        return StructType(fields)

    def _apply_schema(self, df, schema):
        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
            else:
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
                logger.warning(f"Column missing: {field.name} — added as NULL")
        return df.select([field.name for field in schema.fields])

    @staticmethod
    def _deduplicate(df, primary_key):
        before = df.count()
        df = df.dropDuplicates([primary_key])
        after = df.count()
        logger.info(f"Dedup: {before} → {after} ({before - after} removed)")
        return df

    @staticmethod
    def _add_metadata(df):
        return (df
                .withColumn("bronze_loaded_at", F.current_timestamp())
                .withColumn("bronze_source_file", F.input_file_name()))

    @quarantine(write_path_attr="quarantine_path")
    def _validate(self, df):
        logger.info(f"Validating: {self.table_name}")

        total_rows = df.count()
        if total_rows == 0:
            raise ValueError(f"QUALITY GATE FAILED: {self.table_name} — ZERO rows!")

        schema_json = load_table_schema(self.config, self.table_name, s3_bucket=self.s3_bucket)
        critical_columns = [col["name"] for col in schema_json["columns"] if not col.get("nullable", True)]

        bad_condition = None
        for col_name in critical_columns:
            if col_name in df.columns:
                condition = F.col(col_name).isNull()
                bad_condition = condition if bad_condition is None else (bad_condition | condition)

        if bad_condition is None:
            logger.info("No critical columns — all rows pass")
            return df, df.limit(0), {"table": self.table_name, "bad_count": 0}

        bad_df = df.filter(bad_condition)
        good_df = df.filter(~bad_condition)

        bad_count = bad_df.count()
        good_count = good_df.count()

        logger.info(f"Quality Report: {self.table_name}")
        logger.info(f"  Total: {total_rows} | Good: {good_count} | Bad: {bad_count}")

        if bad_count > 0:
            for col_name in critical_columns:
                if col_name in df.columns:
                    null_count = bad_df.filter(F.col(col_name).isNull()).count()
                    if null_count > 0:
                        logger.warning(f"  ⚠️ {col_name}: {null_count} nulls")

        return good_df, bad_df, {"table": self.table_name, "total": total_rows, "good": good_count, "bad": bad_count}

    def _write(self, df, path, partition_column=None):
        count = df.count()
        if count == 0:
            logger.warning(f"EMPTY — skip write: {path}")
            return

        writer = df.write.mode("overwrite").format("parquet")
        if partition_column:
            writer = writer.partitionBy(partition_column)
        writer.save(path)
        logger.info(f"Written: {path} ({count} rows)")

    def process(self):
        df = self._read_raw()
        df = self._clean_data(df)
        schema = self._build_schema()
        df = self._apply_schema(df, schema)
        df = self._deduplicate(df, self.primary_key)
        df = self._add_metadata(df)
        df = self._validate(df)
        self._write(df, self.bronze_path, self.partition_column)
        return df


def process_bronze(spark, config, table_name, s3_bucket=None):
    processor = BronzeProcessor(spark, config, table_name, s3_bucket)
    return processor.run()
