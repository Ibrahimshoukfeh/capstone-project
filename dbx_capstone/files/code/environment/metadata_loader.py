# Databricks notebook source


# COMMAND ----------

# MAGIC %pip install pyyaml 

# COMMAND ----------

import importlib.util
import yaml 
import json
import os
from pyspark.sql.types import *
from delta.tables import *

# Get current user dynamically
current_user = spark.sql("SELECT current_user()").first()[0]

# Load logger module from bundle-deployed path
spec = importlib.util.spec_from_file_location(
    "logger",
    f"/Workspace/Users/ibrahimshoukfeh@kubrickgroup.com/dbx_capstone/files/src/dbx_capstone_framework/logger.py"
)
logger_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logger_module)
EtlLogger = logger_module.EtlLogger

# Metadata path (bundle-deployed)
file_path = f"/Workspace/Users/{current_user}/dbx_capstone/code/files/metadata"

 

# COMMAND ----------

# Not super relevant at this stage; just hardcoding dev for now
# dbutils.widgets.text("env", "dev")
# env = dbutils.widgets.get("env")
env = 'dev'
 
logger = EtlLogger(spark,
    pipeline_name = f"dbx_capstone Environment Prep {current_user}",
    environment = f"{env}"
)
 

# COMMAND ----------

 
dbx_catalog = "dbx_capstone"
 

# COMMAND ----------

try:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {dbx_catalog}.config.bronze_pipeline_metadata
        (
            SourceName STRING,
            TableName STRING,
            KeyName STRING,
            Value STRING,
            CONSTRAINT bronze_pipeline_metadata_pk PRIMARY KEY (SourceName, TableName)
        )
    """)
 
except Exception as e:
    logger.log_error(
        stage="create_bronze_pipeline_metadata_table",
        error_type=type(e).__name__,
        error_message=f"Failed to create bronze_pipeline_metadata table: {e}",
    )
    raise
 

# COMMAND ----------

 
# Function to loop through all sources and extract details
def extract_metadata(metadata):
    table_details = []
 
    if "source" in metadata and isinstance(metadata["source"], dict):
        for source_name, source_details in metadata["source"].items():
            tables = source_details.get("tables", [])                
            if not isinstance(tables, list):
                raise ValueError(f"'tables' under source '{source_name}' must be a list.")

            for table in tables:
                if not isinstance(table, dict):
                    raise ValueError(f"Each table under tables must be a set of key-value pairs")

                table_name = str(table.get("name", table.get("table_name", "UNKNOWN_TABLE")))
                print(f"Processing table: {table_name}")  # Debugging

                if isinstance(table, dict):
                    # Map YAML keys to your expected output keys where names differ
                    # Keep your original expected keys list
                    expected_keys = [
                        'bronze_target_schema', 'bronze_table_name',
                        'silver_target_schema', 'silver_table_name',
                        'transformation_path', 'transformation_function',
                        'primary_key_column', 'expected_primary_key_format',
                        'foreign_key_columns', 'expected_foreign_key_format'
                    ]

                    # Build a value map honoring expected key names
                    value_map = {}

                    # Direct passthrough if present
                    for k in [
                        'bronze_target_schema', 'bronze_table_name',
                        'silver_target_schema', 'silver_table_name',
                        'transformation_path', 'transformation_function',
                        'primary_key_column', 'expected_primary_key_format',
                        'foreign_key_columns', 'expected_foreign_key_format'
                    ]:
                        if k in table:
                            value_map[k] = table[k]

                    # Append only expected keys that are present
                    for key in expected_keys:
                        if key in value_map:
                            table_details.append({
                            'SourceName': source_name, 
                            'TableName': table_name,
                            'KeyName': key, 
                            'Value': value_map[key]
                        })
                else:
                    print(f"Skipping table {table} as it does not contain the expected structure.")
    
        return table_details
 
    # If neither key exists, raise a helpful error
    raise ValueError("Unsupported metadata format. Expected top-level key 'source' (period-based) or 'sources' (flat).")
 

# COMMAND ----------

 
def list_files_in_directory(directory_path):
    file_list = []
    for root, dirs, files in os.walk(directory_path):
        dirs[:] = [d for d in dirs if d != 'example']  # Ignore 'example' folder
        for file in files:
            file_list.append(os.path.join(root, file))
    return file_list
 

# COMMAND ----------

 
def normalize_value(v):
            """
            Ensure Value is a string for Spark schema stability:
            - dict/list -> JSON string
            - None -> None
            - other primitives -> str(v)
            """
            if v is None:
                return None
            if isinstance(v, (dict, list)):
                return json.dumps(v, ensure_ascii=False)
            # Spark can handle numbers as strings too; keep schema uniform
            return str(v)
 

# COMMAND ----------

 
def upsert_metadata(table_details_list):
    # Convert the extracted details into a list of tuples for Spark DataFrame
    if table_details_list:
 
        data_list = [
            (
                str(item.get('SourceName', None)),
                str(item.get('TableName', None)),
                str(item.get('KeyName', None)),
                normalize_value(item.get('Value', None))
            )
            for item in table_details_list
        ]
 
        # Explicit schema so Spark doesn’t infer mixed types
        schema = StructType([
            StructField('SourceName', StringType(), True),
            StructField('TableName', StringType(), True),
            StructField('KeyName', StringType(), True),
            StructField('Value', StringType(), True),
        ])
 
        spark_df = spark.createDataFrame(data_list, schema=schema)
 
        target_table_name = f"{dbx_catalog}.config.bronze_pipeline_metadata"
 
        delta_table = DeltaTable.forName(spark, target_table_name)
 
        # Perform the merge
        (delta_table.alias("target")
            .merge(
                source=spark_df.alias("source"),
                condition="""
                    target.SourceName = source.SourceName AND
                    target.TableName = source.TableName AND
                    target.KeyName = source.KeyName
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )
 
        print("Data successfully upserted into the Unity Catalog table.")
    else:
        print("No data was extracted. Please check the YAML structure and extraction logic.")      
 

# COMMAND ----------

# DBTITLE 1,Process Ingestion Metadata
 
try:
    file_path = f"/Workspace/Users/{current_user}/dbx_capstone/code/files/metadata"
    metadata_files = list_files_in_directory(file_path)
 
    metadata_list = []
 
    for metadata_file in metadata_files:
        with open(metadata_file, 'r') as file:
            metadata = yaml.safe_load(file)
 
        metadata_list.extend(extract_metadata(metadata))
 
    upsert_metadata(metadata_list)
 
    logger.log_run(
        status = "SUCCESS",
        records_processed = len(metadata_files),
        info = {"metadata_loaded": f"{file_path}"},
    )
 
except Exception as e:
    logger.log_error(
        stage="load_touchstone_pipeline_metadata_table",
        error_type=type(e).__name__,
        error_message=f"Failed to load touchstone_pipeline_metadata table: {e}",
    )
    logger.log_run(
        status="FAILURE",
        info={"file": str(e)},
    )
 
    raise