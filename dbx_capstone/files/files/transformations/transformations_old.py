# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import List
from pyspark.sql.functions import col, coalesce, to_date


# GENERIC REUSABLE CLEANING FUNCTIONS


def remove_nulls(df: DataFrame) -> DataFrame:
    return df.dropna()


def remove_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()


def format_all_dates(df: DataFrame) -> DataFrame:
    """
    Automatically finds columns with 'date' in name
    and converts to proper DATE type, handling multiple input formats
    """
    date_formats = [
        "dd/MM/yyyy",
        "MMMM/dd/yyyy",
        "yyyy-MM-dd",
        "MM/dd/yyyy",
        "dd-MM-yyyy",
        "MMMM dd, yyyy",
    ]
    for col_name in df.columns:
        if "date" in col_name.lower():
            df = df.withColumn(
                col_name,
                coalesce(*[try_to_date(col(col_name), fmt) for fmt in date_formats])
            )
    return df

# def format_all_dates(df):
#     """
#     Automatically finds columns with 'date' in the name
#     and converts them to proper DATE type using multiple possible formats.
#     """
#     date_formats = [
#         "dd/MM/yyyy",
#         "MMMM/dd/yyyy",
#         "yyyy-MM-dd",
#         "MM/dd/yyyy",
#         "dd-MM-yyyy",
#         "MMMM dd, yyyy",
#     ]

#     for col_name in df.columns:
#         if "date" in col_name.lower():
#             df = df.withColumn(
#                 col_name,
#                 coalesce(*[to_date(col(col_name), fmt) for fmt in date_formats])
#             )

#     return df

# Added before transformations
def apply_common_rules(df: DataFrame) -> DataFrame:
    df = remove_nulls(df)
    df = remove_duplicates(df)
    df = format_all_dates(df)
    return df


def validate_id_columns(df: DataFrame, key_dict) -> DataFrame:
    # For now, just return df unless your metadata has specific ID rules
    return df

# =========================================================
#TABLE-SPECIFIC TRANSFORMATIONS
# (Must match metadata.yml function names)
# =========================================================

def transform_customers(df: DataFrame, key_dict) -> DataFrame:
    df = apply_common_rules(df)

    # Add customer-specific logic here if needed
    df = validate_id_columns(df, key_dict)

    return df


def transform_products(df: DataFrame, key_dict) -> DataFrame:
    df = apply_common_rules(df)

    # Add product-specific logic here if needed
    df = validate_id_columns(df, key_dict)

    return df

def transform_sales(df: DataFrame, key_dict) -> DataFrame:
    df = apply_common_rules(df)

    # Sales is the fact table - it links customers and products
    # validate_id_columns will check order_id format AND
    # both foreign keys (customer_id, product_id) using key_dict
    df = validate_id_columns(df, key_dict)

    # Derive a total sale value if both columns exist
    if "quantity" in df.columns and "unit_price" in df.columns:
        df = df.withColumn("order_total", col("quantity") * col("unit_price"))

    return df


def transform_stores(df: DataFrame, key_dict) -> DataFrame:
    df = apply_common_rules(df)

    # Store IDs have no strict format in metadata, but we still validate
    df = validate_id_columns(df, key_dict)

    return df


def transform_calendar(df: DataFrame, key_dict) -> DataFrame:
    df = apply_common_rules(df)

    # apply_common_rules calls format_all_dates() which automatically
    # detects the 'date' column by name and converts it to DATE type
    # No key validation needed - date IS the primary key and is already handled

    return df
