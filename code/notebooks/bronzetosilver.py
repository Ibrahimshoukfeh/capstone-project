# Databricks notebook source
# ---------------------------------------------
# Imports
# ---------------------------------------------
import sys
import importlib
from pyspark.sql import SparkSession

# If transformations.py is in your repo/package
sys.path.insert(0, insert file path here )
if 'cleaning file name' in sys.modules:
    del sys.modules['cleaning file name']
import transformations

# ---------------------------------------------
# Config
# ---------------------------------------------
metadata_table = "insert file path here"
capstone_catalog = "dbx_capstone"

# Optional: logging
sys.path.insert(0, 'insert file path here')
from dbx_capstone_framework.logger import EtlLogger

logger = EtlLogger(
    spark,
    pipeline_name="name of pipeline",
    environment="dev"
)

# ---------------------------------------------
# Read metadata
# ---------------------------------------------
metadata = spark.read.table(metadata_table)

# Get distinct tables
tables = [row["TableName"] for row in metadata.select("TableName").distinct().collect()]

# ---------------------------------------------
# Process each table
# ---------------------------------------------
for t in tables:
    try:
        # ---------------------------------------------
        # Extract metadata for this table
        # ---------------------------------------------
        metadata_dict = metadata.filter(
            metadata.TableName == t
        ).select("KeyName", "Value") \
         .toPandas() \
         .set_index("KeyName") \
         .to_dict()["Value"]

        bronze_table = metadata_dict["bronze_table_name"]
        bronze_schema = metadata_dict["bronze_target_schema"]
        silver_table = metadata_dict["silver_table_name"]
        silver_schema = metadata_dict["silver_target_schema"]

        primary_key_col = metadata_dict['primary_key_column']
        primary_key_format = metadata_dict['expected_primary_key_format']
        foreign_key_dict = metadata_dict['foreign_key_columns']

        all_keys = {primary_key_col: primary_key_format}
        if foreign_key_dict:
            for z in zip(foreign_key_dict['column_name'], foreign_key_dict['expected_format']):
                all_keys[z[0]] = z[1]

        transformation_function = metadata_dict["transformation_function"]

        bronze_path = f"{capstone_catalog}.{bronze_schema}.{bronze_table}"
        silver_path = f"{capstone_catalog}.{silver_schema}.{silver_table}"

        # ---------------------------------------------
        # Read Bronze Table
        # ---------------------------------------------
        df = spark.read.table(bronze_path)
        records_before = df.count()

        # ---------------------------------------------
        # Dynamically call transformation function
        # ---------------------------------------------
        transform_func = getattr(transformations, transformation_function)

        df_clean = transform_func(df, all_keys)
        records_after = df_clean.count()

        # ---------------------------------------------
        # Write to Silver
        # ---------------------------------------------
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {silver_path}
            USING delta
        """)

        df_clean.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(silver_path)

        # ---------------------------------------------
        # Log success
        # ---------------------------------------------
        logger.log_run(
            status="SUCCESS",
            records_processed=records_after,
            info={
                "table": t,
                "bronze_table": bronze_path,
                "silver_table": silver_path,
                "records_before": records_before,
                "records_after": records_after
            }
        )

        print(f"SUCCESS: {bronze_path} → {silver_path}")

    except Exception as e:
        # ---------------------------------------------
        # Log error
        # ---------------------------------------------
        logger.log_error(
            stage=f"processing_{t}",
            error_type=type(e).__name__,
            error_message=str(e),
            exc=e,
            extra_ctx={"table": t}
        )

        logger.log_run(status="FAILED")

        print(f"FAILED: {t}")
        raise.