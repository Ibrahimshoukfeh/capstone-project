# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import List


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


