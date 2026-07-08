# Databricks notebook source

# ---------------------------------------------
# Define variables
# ---------------------------------------------
metadata_table_id = "dbx_capstone.config.bronze_pipeline_metadata"
raw_file_folder = "/Volumes/dbx_capstone/bronze/dataset_upload/Chocolate data set/"
capstone_catalog = "dbx_capstone"

from pyspark.sql.functions import regexp_extract, current_timestamp, col, lit, date_format

# ---------------------------------------------
# Read metadata table
# ---------------------------------------------
metadata = spark.read.table(metadata_table_id)

# ---------------------------------------------
# Get list of tables
# ---------------------------------------------
tables = [row['TableName'] for row in metadata.select("TableName").distinct().collect()]

# ---------------------------------------------
# Loop through metadata and ingest files
# ---------------------------------------------
for t in tables:

    metadata_dict = metadata.filter(
        (metadata.TableName == t) &
        (metadata.KeyName.isin(["bronze_table_name","bronze_target_schema"]))
    ).select(
        "KeyName","Value"
    ).toPandas().set_index("KeyName").to_dict()['Value']

    bronze_table = metadata_dict["bronze_table_name"]
    bronze_schema = metadata_dict["bronze_target_schema"]

    raw_file_path = f"{raw_file_folder}{t.lower()}.csv"

    # ---------------------------------------------
    # Read CSV file
    # ---------------------------------------------
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(raw_file_path)
    )

    # ---------------------------------------------
    # Add audit columns (like Script 1)
    # ---------------------------------------------
    df = (
        df.withColumn("ctrl_filepath", lit(raw_file_path))
        .withColumn(
            "ctrl_filename",
            regexp_extract(col("ctrl_filepath"), r"([^/]+)$", 1)
        )
        .withColumn(
            "ctrl_timestamp",
            date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")
        )
    )

    table_name = f"{capstone_catalog}.{bronze_schema}.{bronze_table}"

    # ---------------------------------------------
    # Create table if it doesn't exist
    # ---------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            ctrl_filename STRING,
            ctrl_filepath STRING,
            ctrl_timestamp STRING
        )
        USING delta
        TBLPROPERTIES (
            delta.enableChangeDataFeed = true,
            delta.autoOptimize.optimizeWrite = true,
            delta.autoOptimize.autoCompact = true
        )
    """)

    # ---------------------------------------------
    # Append data
    # ---------------------------------------------
    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema","true") \
        .saveAsTable(table_name)

    print(f"Loaded {raw_file_path} into {table_name}")