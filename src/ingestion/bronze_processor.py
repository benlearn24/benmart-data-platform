"""Bronze Processor — Raw ingestion with multi-format support, incremental loads, and DMS CDC handling."""

import logging
from functools import wraps

import boto3
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType,
    DecimalType, DateType, TimestampType, BooleanType,
    StructType, StructField, ArrayType
)

from src.utils.base_processor import BaseProcessor
from src.utils.config_loader import get_s3_path, load_table_schema
from src.utils.manifest_manager import ManifestManager

logger = logging.getLogger(__name__)


def quarantine(write_path_attr="quarantine_path"):
    """Writes bad rows to quarantine, returns only good rows. Raises if ALL rows fail."""
    def decorator(validate_func):
        @wraps(validate_func)
        def wrapper(self, df):
            good_df, bad_df, report = validate_func(self, df)

            bad_count = bad_df.count()
            if bad_count > 0:
                q_path = getattr(self, write_path_attr)
                bad_df.write.mode("overwrite").format("parquet").save(q_path)
                logger.warning(f"🔴 Quarantined {bad_count} rows → {q_path}")

            good_count = good_df.count()
            if good_count == 0:
                raise ValueError(
                    f"QUALITY GATE FAILED: {self.table_name} — "
                    f"ALL {report.get('total', '?')} rows quarantined."
                )

            logger.info(f"✅ {good_count} rows passed quality checks")
            return good_df

        return wrapper
    return decorator


class BronzeProcessor(BaseProcessor):
    """Ingests raw data into Bronze layer with cleaning, schema enforcement, and quality checks."""

    SUPPORTED_FORMATS = {"csv", "json", "parquet"}

    TYPE_MAP = {
        "StringType":    StringType(),
        "IntegerType":   IntegerType(),
        "LongType":      LongType(),
        "DoubleType":    DoubleType(),
        "DecimalType":   DecimalType(10, 2),
        "DateType":      DateType(),
        "TimestampType": TimestampType(),
        "BooleanType":   BooleanType(),
    }

    def __init__(self, spark, config, table_name, s3_bucket=None):
        super().__init__(spark, config, s3_bucket)

        self.table_name = table_name
        self.table_config = config['tables'][table_name]

        self.source_format = self.table_config.get('source_format', 'csv')
        self.read_options = self.table_config.get('read_options', {})
        self.primary_key = self.table_config['primary_key']
        self.partition_column = self.table_config.get('partition_column', None)

        if self.source_format not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format '{self.source_format}' for {table_name}. "
                f"Supported: {self.SUPPORTED_FORMATS}"
            )

        self.raw_bucket = config['s3']['raw_bucket']
        self.raw_path = get_s3_path(config, 'raw', table_name)
        self.bronze_path = get_s3_path(config, 'bronze', table_name)

        if s3_bucket:
            self.quarantine_path = f"s3://{config['s3']['raw_bucket']}/quarantine/{table_name}"
        else:
            self.quarantine_path = f"data/quarantine/{table_name}"

        self.load_mode = self.table_config.get('load_mode', 'full')
        self.source_type = self.table_config.get('source', 'manual_upload')
        self.has_cdc = self.table_config.get('has_cdc', False)
        self.dms_columns = self.table_config.get('dms_columns', [])

        self.manifest = None
        if self.load_mode == 'incremental' and s3_bucket:
            manifest_prefix = config['s3'].get('manifest_prefix', 'manifests/')
            self.manifest = ManifestManager(
                self.raw_bucket, manifest_prefix, table_name
            )

        logger.info(
            f"🔧 {self.processor_name} initialized | "
            f"format={self.source_format} | mode={self.load_mode} | source={self.source_type}"
        )

    @property
    def processor_name(self):
        return f"BRONZE: {self.table_name}"

    def _read_raw(self, file_paths=None):
        fmt = self.source_format
        source = file_paths if file_paths else self.raw_path
        source_label = f"{len(file_paths)} new files" if file_paths else "full folder"

        try:
            if fmt == "csv":
                reader = self.spark.read.format("csv")
                for key, value in self.read_options.items():
                    reader = reader.option(key, value)
                df = reader.load(source)

            elif fmt == "json":
                reader = self.spark.read.format("json")
                for key, value in self.read_options.items():
                    reader = reader.option(key, value)
                df = reader.load(source)

            elif fmt == "parquet":
                if isinstance(source, list):
                    df = self.spark.read.parquet(*source)
                else:
                    df = self.spark.read.parquet(source)

            else:
                raise ValueError(f"Unsupported format: {fmt}")

        except ValueError:
            raise
        except Exception as e:
            logger.error(f"❌ Read FAILED: {self.raw_path} | {fmt} | {e}")
            raise RuntimeError(f"Failed to read {fmt} from {self.raw_path}: {e}") from e

        count = df.count()
        if count == 0:
            logger.warning(f"⚠️ EMPTY data from {source_label}: {self.raw_path}")
        else:
            logger.info(f"📥 Read {count} rows from {fmt} ({source_label})")

        return df

    def _read_dms_raw(self, file_paths=None):
        """Reads DMS output — separates LOAD (no op column) and CDC (op column first), renames _cN to actual names."""
        col_names = self.dms_columns

        # Step 1: Get file list
        if file_paths:
            all_files = file_paths
        else:
            s3_client = boto3.client('s3')
            prefix = self.raw_path.replace(f"s3://{self.raw_bucket}/", "")
            response = s3_client.list_objects_v2(
                Bucket=self.raw_bucket, Prefix=prefix
            )
            all_files = [
                f"s3://{self.raw_bucket}/{obj['Key']}"
                for obj in response.get('Contents', [])
                if obj['Size'] > 0 and not obj['Key'].endswith('/')
            ]

        if not all_files:
            logger.warning(f"⚠️ No DMS files found for {self.table_name}")
            return self.spark.createDataFrame([], StructType([]))

        # Step 2: Separate LOAD and CDC by filename
        load_files = [f for f in all_files if 'LOAD' in f.split('/')[-1].upper()]
        cdc_files = [f for f in all_files if 'LOAD' not in f.split('/')[-1].upper()]

        dfs = []

        # Step 3: Read LOAD files — N columns, no operation column
        if load_files:
            load_df = (self.spark.read.format("csv")
                       .option("header", "false")
                       .option("inferSchema", "false")
                       .load(load_files))

            for i, name in enumerate(col_names):
                col_id = f"_c{i}"
                if col_id in load_df.columns:
                    load_df = load_df.withColumnRenamed(col_id, name)

            load_df = load_df.withColumn("dms_operation", F.lit("L"))
            dfs.append(load_df)
            logger.info(f"📥 DMS LOAD: {load_df.count()} rows from {len(load_files)} files")

        # Step 4: Read CDC files — N+1 columns, first column = operation (I/U/D)
        if cdc_files:
            cdc_df = (self.spark.read.format("csv")
                      .option("header", "false")
                      .option("inferSchema", "false")
                      .load(cdc_files))

            cdc_df = cdc_df.withColumnRenamed("_c0", "dms_operation")
            for i, name in enumerate(col_names):
                col_id = f"_c{i + 1}"
                if col_id in cdc_df.columns:
                    cdc_df = cdc_df.withColumnRenamed(col_id, name)

            dfs.append(cdc_df)
            logger.info(f"📥 DMS CDC: {cdc_df.count()} rows from {len(cdc_files)} files")

        # Step 5: Union LOAD + CDC
        if len(dfs) == 1:
            df = dfs[0]
        else:
            df = dfs[0].unionByName(dfs[1], allowMissingColumns=True)

        logger.info(f"📥 DMS Total: {df.count()} rows for {self.table_name}")
        return df

    def _clean_data(self, df):
        logger.info(f"🧹 Cleaning: {self.table_name} | format={self.source_format}")

        if "address" in df.columns:
            df = (df
                  .withColumn("city", F.col("address.city"))
                  .withColumn("state", F.col("address.state"))
                  .withColumn("pincode", F.col("address.pincode"))
                  .drop("address"))
            logger.info("  Flattened: address → city, state, pincode")

        if "phone" in df.columns:
            phone_type = df.schema["phone"].dataType
            if isinstance(phone_type, ArrayType):
                df = df.withColumn("phone", F.col("phone").getItem(0))
                logger.info("  Extracted: phone[0] from array")

        string_cols = [
            field.name for field in df.schema.fields
            if isinstance(field.dataType, StringType)
        ]
        for col_name in string_cols:
            df = df.withColumn(col_name, F.trim(F.col(col_name)))

        if string_cols:
            logger.info(f"  Trimmed whitespace: {len(string_cols)} string columns")

        return df

    def _build_schema(self):
        schema_json = load_table_schema(
            self.config, self.table_name, s3_bucket=self.s3_bucket
        )
        fields = []
        for col in schema_json["columns"]:
            spark_type = self.TYPE_MAP.get(col["type"], StringType())
            nullable = col.get("nullable", True)
            fields.append(StructField(col["name"], spark_type, nullable))

        logger.info(f"📐 Schema built: {len(fields)} columns for {self.table_name}")
        return StructType(fields)

    def _apply_schema(self, df, schema):
        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
            else:
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
                logger.warning(f"  ⚠️ Missing column: {field.name} — added as NULL")

        select_cols = [field.name for field in schema.fields]

        # Preserve dms_operation column for DMS sources
        if "dms_operation" in df.columns:
            select_cols.append("dms_operation")

        df = df.select(select_cols)
        logger.info(f"📐 Schema applied: {len(schema.fields)} columns enforced")
        return df

    @staticmethod
    def _deduplicate(df, primary_key):
        before = df.count()
        df = df.dropDuplicates([primary_key])
        after = df.count()
        removed = before - after

        if removed > 0:
            logger.info(f"🔁 Dedup: {before} → {after} ({removed} duplicates removed)")
        else:
            logger.info(f"🔁 Dedup: {before} rows — no duplicates found")

        return df

    @staticmethod
    def _add_metadata(df):
        return (df
                .withColumn("bronze_loaded_at", F.current_timestamp())
                .withColumn("bronze_source_file", F.input_file_name()))

    @quarantine(write_path_attr="quarantine_path")
    def _validate(self, df):
        logger.info(f"🔍 Validating: {self.table_name}")

        total_rows = df.count()
        if total_rows == 0:
            raise ValueError(f"QUALITY GATE FAILED: {self.table_name} — ZERO rows!")

        schema_json = load_table_schema(
            self.config, self.table_name, s3_bucket=self.s3_bucket
        )
        critical_columns = [
            col["name"] for col in schema_json["columns"]
            if not col.get("nullable", True)
        ]

        bad_condition = None
        for col_name in critical_columns:
            if col_name in df.columns:
                condition = F.col(col_name).isNull()
                bad_condition = (
                    condition if bad_condition is None
                    else (bad_condition | condition)
                )

        if bad_condition is None:
            logger.info(f"  No critical columns — all {total_rows} rows pass")
            return df, self.spark.createDataFrame([], df.schema), {
                "table": self.table_name, "total": total_rows,
                "good": total_rows, "bad": 0
            }

        bad_df = df.filter(bad_condition)
        good_df = df.filter(~bad_condition)

        bad_count = bad_df.count()
        good_count = good_df.count()

        logger.info(f"  📊 Quality: Total={total_rows} | Good={good_count} | Bad={bad_count}")

        if bad_count > 0:
            for col_name in critical_columns:
                if col_name in df.columns:
                    null_count = bad_df.filter(F.col(col_name).isNull()).count()
                    if null_count > 0:
                        logger.warning(f"     ⚠️ {col_name}: {null_count} nulls")

        return good_df, bad_df, {
            "table": self.table_name, "total": total_rows,
            "good": good_count, "bad": bad_count
        }

    def _write(self, df):
        count = df.count()
        if count == 0:
            logger.warning(f"⚠️ EMPTY — skipping write to {self.bronze_path}")
            return

        write_mode = "append" if self.load_mode == "incremental" else "overwrite"

        writer = df.write.mode(write_mode).format("parquet")
        if self.partition_column:
            writer = writer.partitionBy(self.partition_column)
        writer.save(self.bronze_path)

        logger.info(f"📤 Written {count} rows → {self.bronze_path} | mode={write_mode}")

    def process(self):
        new_files = None

        if self.load_mode == 'incremental' and self.manifest:
            raw_prefix = self.raw_path.replace(f"s3://{self.raw_bucket}/", "")
            new_files = self.manifest.get_new_files(raw_prefix)

            if not new_files:
                logger.info(f"⏭️ No new files for {self.table_name} — skipping")
                return None

            logger.info(f"📋 Found {len(new_files)} new files to process")

        # Route to correct read method based on source type
        if self.source_type == 'dms':
            df = self._read_dms_raw(file_paths=new_files)
        else:
            df = self._read_raw(file_paths=new_files)

        df = self._clean_data(df)
        schema = self._build_schema()
        df = self._apply_schema(df, schema)
        df = self._deduplicate(df, self.primary_key)
        df = self._add_metadata(df)
        df = self._validate(df)
        self._write(df)

        if self.load_mode == 'incremental' and self.manifest and new_files:
            self.manifest.update(new_files, df.count())

        return df


def process_bronze(spark, config, table_name, s3_bucket=None):
    processor = BronzeProcessor(spark, config, table_name, s3_bucket)
    return processor.run()
