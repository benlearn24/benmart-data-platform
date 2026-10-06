
"""Bronze processor — reads raw data from any source, cleans, validates, writes to Bronze."""

import logging
from datetime import datetime
from pyspark.sql import functions as F, Window, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from src.utils.base_processor import BaseProcessor, log_step
from src.utils.config_loader import (
    load_schema, get_s3_path, get_write_config, get_quality_thresholds
)

logger = logging.getLogger(__name__)


class BronzeProcessor(BaseProcessor):
    """Config-driven processor: raw → clean → validated → Bronze layer.

    Handles CSV, JSON, Parquet, DMS CDC. Quarantines bad rows,
    enforces quality gates, deduplicates, writes Delta format.
    """

    def __init__(self, spark, config, table_name, s3_bucket=None):
        super().__init__(spark, config, s3_bucket)
        self.table_name = table_name

        # Table-specific config
        self.table_cfg = config["tables"][table_name]
        self.source_format = self.table_cfg["source_format"]
        self.primary_key = self.table_cfg["primary_key"]
        self.load_mode = self.table_cfg["load_mode"]
        self.has_cdc = self.table_cfg.get("has_cdc", False)
        self.read_options = self.table_cfg.get("read_options", {})

        # Paths
        self.raw_path = get_s3_path(config, "raw", table_name)
        self.bronze_path = get_s3_path(config, "bronze", table_name)
        quarantine_prefix = config["validation"]["quarantine_path_prefix"]
        self.quarantine_path = f"s3://{config['s3']['raw_bucket']}/{quarantine_prefix}/{table_name}/"

        # Schema, write config, quality thresholds
        self.schema = load_schema(self.table_cfg["schema_file"])
        self.write_cfg = get_write_config(config, table_name)
        self.thresholds = get_quality_thresholds(config, table_name)

        # Validation settings
        self.read_mode = config["validation"]["read_mode"]
        self.corrupt_col = config["validation"]["corrupt_record_column"]

        # Track quarantine count for quality gate
        self._quarantine_count = 0

        logger.info(
            f"Bronze init: {table_name} | format={self.source_format} | "
            f"mode={self.load_mode} | cdc={self.has_cdc}"
        )

    @property
    def processor_name(self):
        """Identifier for logging."""
        return f"BRONZE:{self.table_name}"

    # ── READ ────────────────────────────────────────────────────

    @log_step("Read raw data")
    def _read_raw(self):
        """Read raw data based on source format (CSV/JSON/Parquet). Config-driven."""
        fmt = self.source_format
        path = self.raw_path
        reader = self.spark.read

        # Apply all read options from config
        for key, value in self.read_options.items():
            reader = reader.option(key, value)

        if fmt == "csv":
            reader = reader.option("mode", self.read_mode)
            reader = reader.option("columnNameOfCorruptRecord", self.corrupt_col)
            # Add corrupt record column to schema for PERMISSIVE mode
            read_schema = self._schema_with_corrupt_col()
            df = reader.schema(read_schema).csv(path)

        elif fmt == "json":
            df = reader.schema(self.schema).json(path)

        elif fmt == "parquet":
            df = reader.parquet(path)

        else:
            raise ValueError(f"Unsupported format: {fmt}")

        self.safe_count(df, f"raw {self.table_name}")
        return df

    @log_step("Read DMS CDC data")
    def _read_dms_raw(self):
        """Read DMS CDC files — headerless CSV with operation column (I/U/D)."""
        path = self.raw_path
        dms_columns = self.table_cfg["dms_columns"]

        # DMS CDC first column = operation (I/U/D), then actual data columns
        all_columns = ["_dms_operation"] + dms_columns

        # Build schema: all string first (cast later in _apply_schema)
        dms_schema = StructType([
            StructField(col, StringType(), True) for col in all_columns
        ])

        df = (self.spark.read
              .option("header", "false")
              .option("inferSchema", "false")
              .schema(dms_schema)
              .csv(path))

        total = self.safe_count(df, f"dms raw {self.table_name}")

        # Log CDC operation distribution
        if total > 0:
            ops = df.groupBy("_dms_operation").count().collect()
            op_summary = {row["_dms_operation"]: row["count"] for row in ops}
            logger.info(f"CDC operations: {op_summary}")

        return df

    @classmethod
    def _apply_cdc(cls, df):
        """Apply CDC logic — remove deletes, flag operation for downstream."""
        before = df.count()

        # Remove delete operations
        df = df.filter(F.col("_dms_operation") != "D")

        # Rename internal column to standard name
        df = df.withColumn("dms_operation",
                           F.when(F.col("_dms_operation") == "I", "insert")
                           .when(F.col("_dms_operation") == "U", "update")
                           .otherwise("unknown"))
        df = df.drop("_dms_operation")

        after = df.count()
        logger.info(f"CDC applied: {before} → {after} ({before - after} deletes removed)")
        return df

    def _schema_with_corrupt_col(self):
        """Add _corrupt_record column to schema for PERMISSIVE read mode."""
        fields = list(self.schema.fields)
        if self.corrupt_col not in [f.name for f in fields]:
            fields.append(StructField(self.corrupt_col, StringType(), True))
        return StructType(fields)

    # ── SCHEMA ──────────────────────────────────────────────────

    @log_step("Apply schema")
    def _apply_schema(self, df):
        """Cast columns to correct types defined in JSON schema."""
        for field in self.schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
            else:
                # Column missing in data — add as null with correct type
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
                logger.warning(f"Schema drift: column '{field.name}' missing — added as NULL")
        return df

    @log_step("Detect schema drift")
    def _detect_schema_drift(self, df):
        """Log new/missing columns vs expected schema. Alert on unexpected changes."""
        expected_cols = {f.name for f in self.schema.fields}
        actual_cols = set(df.columns)

        # New columns in data not in schema
        new_cols = actual_cols - expected_cols - {self.corrupt_col, "_dms_operation", "dms_operation"}
        if new_cols:
            logger.warning(f"Schema drift — NEW columns detected: {new_cols}")

        # Missing columns (already handled in _apply_schema, just log)
        missing_cols = expected_cols - actual_cols
        if missing_cols:
            logger.warning(f"Schema drift — MISSING columns: {missing_cols}")

        return df

    # ── VALIDATE + QUARANTINE ───────────────────────────────────

    @log_step("Validate and quarantine")
    def _validate_and_quarantine(self, df):
        """Separate corrupt rows into quarantine. Return clean rows only."""
        if self.corrupt_col not in df.columns:
            logger.info("No corrupt record column — skipping quarantine split")
            return df

        # Split: corrupt vs clean
        corrupt_df = df.filter(F.col(self.corrupt_col).isNotNull())
        clean_df = df.filter(F.col(self.corrupt_col).isNull()).drop(self.corrupt_col)

        corrupt_count = corrupt_df.count()
        self._quarantine_count = corrupt_count

        if corrupt_count > 0:
            # Write corrupt rows to quarantine path
            corrupt_df.write.mode("append").json(self.quarantine_path)
            logger.warning(f"Quarantined {corrupt_count} corrupt rows → {self.quarantine_path}")
        else:
            logger.info("No corrupt rows — quarantine empty")

        self.safe_count(clean_df, "clean after quarantine")
        return clean_df

    # ── QUALITY GATE ────────────────────────────────────────────

    @log_step("Quality gate check")
    def _quality_gate(self, df):
        """Enforce quality thresholds from config. Fail pipeline if breached."""
        total = df.count()

        # Zero rows check
        if total == 0 and self.config.get("quality", {}).get("fail_on_zero_rows", True):
            raise ValueError(f"QUALITY GATE FAIL [{self.table_name}]: 0 rows!")

        issues = []

        # Quarantine percentage check
        max_q_pct = self.config.get("quality", {}).get("max_quarantine_percentage", 20)
        if self._quarantine_count > 0:
            q_pct = (self._quarantine_count / (total + self._quarantine_count)) * 100
            if q_pct > max_q_pct:
                issues.append(f"quarantine={q_pct:.1f}% > max {max_q_pct}%")

        # Per-column null checks + min row count
        for key, max_value in self.thresholds.items():
            if key == "min_row_count":
                if total < max_value:
                    issues.append(f"rows={total} < min {max_value}")
                continue

            if not key.startswith("null_"):
                continue

            col_name = key.replace("null_", "", 1)
            if col_name not in df.columns:
                continue

            null_count, null_pct = self.count_nulls(df, col_name)
            if null_pct > max_value:
                issues.append(f"{col_name} null={null_pct}% > max {max_value}%")

        if issues:
            msg = f"QUALITY GATE FAIL [{self.table_name}]: {'; '.join(issues)}"
            logger.error(msg)
            raise ValueError(msg)

        logger.info(f"Quality gate PASS: {total} rows, all checks ok")
        return df

    # ── DEDUP ───────────────────────────────────────────────────

    @log_step("Deduplicate")
    def _deduplicate(self, df):
        """Remove duplicate rows by primary key. Keep latest if timestamps available."""
        before = df.count()

        if "updated_at" in df.columns:
            # Window dedup — keep latest version per primary key
            w = Window.partitionBy(self.primary_key).orderBy(F.col("updated_at").desc())
            df = (df.withColumn("_row_num", F.row_number().over(w))
                  .filter(F.col("_row_num") == 1)
                  .drop("_row_num"))
        else:
            # Simple dedup — just drop exact duplicates
            df = df.dropDuplicates([self.primary_key])

        after = df.count()
        removed = before - after
        if removed > 0:
            logger.info(f"Dedup: {before} → {after} ({removed} duplicates removed)")
        return df

    # ── METADATA ────────────────────────────────────────────────

    @staticmethod
    def _add_metadata(df):
        """Add Bronze-layer tracking columns."""
        return (df
                .withColumn("bronze_loaded_at", F.current_timestamp())
                .withColumn("bronze_source_file", F.input_file_name()))

    # ── WRITE ───────────────────────────────────────────────────

    @log_step("Write to Bronze")
    def _write(self, df):
        """Write clean data to Bronze layer. Delta format, partitioned, config-driven."""
        count = df.count()
        if count == 0:
            logger.warning(f"EMPTY — skipping write to {self.bronze_path}")
            return

        fmt = self.write_cfg["format"]
        mode = self.write_cfg["mode"]
        partitions = self.write_cfg["partition_columns"]
        delta_opts = self.write_cfg["delta_options"]

        writer = df.coalesce(max(1, count // 500000)).write.mode(mode).format(fmt)

        # Delta options
        if fmt == "delta":
            for k, v in delta_opts.items():
                writer = writer.option(k, v)

        # Partition
        if partitions:
            writer = writer.partitionBy(*partitions)

        writer.save(self.bronze_path)
        logger.info(f"Written: {count:,} rows → {self.bronze_path} | format={fmt} | mode={mode}")

    # ── MAIN ORCHESTRATION ──────────────────────────────────────

    def process(self):
        """Full Bronze processing flow: read → schema → validate → quality → dedup → write."""

        # Step 1: Read raw data (format-specific)
        if self.has_cdc:
            df = self._read_dms_raw()
            df = self._apply_cdc(df)
        else:
            df = self._read_raw()

        # Step 2: Detect schema drift (log warnings)
        df = self._detect_schema_drift(df)

        # Step 3: Apply schema (cast types)
        df = self._apply_schema(df)

        # Step 4: Validate + quarantine bad rows
        df = self._validate_and_quarantine(df)

        # Step 5: Quality gate (fail if thresholds breached)
        df = self._quality_gate(df)

        # Step 6: Dedup
        df = self._deduplicate(df)

        # Step 7: Add metadata
        df = self._add_metadata(df)

        # Step 8: Write
        self._write(df)

        return df


def process_bronze(spark, config, table_name, s3_bucket=None):
    """Convenience function — create processor and run."""
    return BronzeProcessor(spark, config, table_name, s3_bucket).run()
