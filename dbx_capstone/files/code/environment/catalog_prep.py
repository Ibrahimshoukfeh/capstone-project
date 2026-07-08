# Databricks notebook source

# COMMAND ----------
# Schema Prep

spark.sql("CREATE SCHEMA IF NOT EXISTS dbx_capstone.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS dbx_capstone.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS dbx_capstone.gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS dbx_capstone.log")
spark.sql("CREATE SCHEMA IF NOT EXISTS dbx_capstone.quaratine")
spark.sql("CREATE SCHEMA IF NOT EXISTS dbx_capstone.config")

# COMMAND ----------
# Volume Prep

spark.sql("""
    CREATE VOLUME IF NOT EXISTS dbx_capstone.bronze.dataset_upload
""")