# Databricks notebook source
# ---------------------------------------------
# Imports
# ---------------------------------------------
import sys
import importlib
from pyspark.sql import SparkSession

# Path to your aggregations file
sys.path.insert(0, 'file path ')
if 'aggregations' in sys.modules:
    del sys.modules['aggregations']
import aggregations

# ---------------------------------------------
# Config
# ---------------------------------------------
capstone_catalog = "dbx_capstone"

# Optional: logging
sys.path.insert(0, 'files path ')
from dbx_capstone_framework.logger import EtlLogger

logger = EtlLogger(
    spark,
    pipeline_name="silver_to_gold",
    environment="dev"
)

# ---------------------------------------------
# Run Gold Transformations
# ---------------------------------------------
try:
    # Run all gold aggregations (computes and writes all 4 tables)
    aggregations.main(spark)

    # Log each gold table
    gold_tables = ["monthly_metrics", "lapsed_customers", "store_metrics", "loyalty_metrics"]
    for table_name in gold_tables:
        count = spark.table(f"dbx_capstone.gold.{table_name}").count()
        logger.log_run(
            status="SUCCESS",
            records_processed=count,
            info={"table": table_name}
        )
        print(f"SUCCESS: {table_name}")

# ---------------------------------------------
# Error Handling
# ---------------------------------------------
except Exception as e:
    logger.log_error(
        stage="gold_pipeline",
        error_type=type(e).__name__,
        error_message=str(e),
        exc=e
    )

    logger.log_run(status="FAILED")

    print("FAILED: gold pipeline")
    raise