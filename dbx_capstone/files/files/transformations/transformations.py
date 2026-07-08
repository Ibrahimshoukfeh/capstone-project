from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import List
from pyspark.sql.functions import col, coalesce, to_date, expr, coalesce, regexp_replace

# GENERIC REUSABLE CLEANING FUNCTIONS


def remove_nulls(df: DataFrame) -> DataFrame:
    return df.dropna()


def remove_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()


# def format_all_dates(df: DataFrame) -> DataFrame:
#     """
#     Automatically finds columns with 'date' in name
#     and converts to proper DATE type, handling multiple input formats
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
#                 coalesce(*[try_to_date(col(col_name), fmt) for fmt in date_formats])
#             )
#     return df

def format_all_dates(df):
    """
    Finds columns containing 'date' in the name and converts them to DATE type.
    Handles multiple formats safely using try_to_date().
    Invalid rows are dropped.
    """

    for col_name in df.columns:
        if "date" in col_name.lower():

            df = df.withColumn(
                col_name,
                coalesce(
                    # Standard formats
                    expr(f"try_to_date({col_name}, 'yyyy-MM-dd')"),
                    expr(f"try_to_date({col_name}, 'MM/dd/yyyy')"),
                    expr(f"try_to_date({col_name}, 'dd/MM/yyyy')"),
                    expr(f"try_to_date({col_name}, 'dd-MM-yyyy')"),

                    # Normalize "March/04/2023" → "March 04 2023"
                    expr(f"try_to_date(regexp_replace({col_name}, '/', ' '), 'MMMM dd yyyy')"),

                    # e.g. "March 4, 2023"
                    expr(f"try_to_date({col_name}, 'MMMM dd, yyyy')")
                )
            )

            # Drop rows where parsing failed
            df = df.filter(col(col_name).isNotNull())

    return df

# Added before transformations
def apply_common_rules(df: DataFrame) -> DataFrame:
    df = remove_nulls(df)
    df = remove_duplicates(df)
    df = format_all_dates(df)
    return df


def validate_id_columns(df: DataFrame, key_dict: dict) -> DataFrame:
    """
    Validates that ID columns match their expected regex format.
    Filters out rows where any key column does not match.
    """
    for col_name, expected_format in key_dict.items():
        if col_name in df.columns and expected_format:
            df = df.filter(col(col_name).rlike(expected_format))
    return df

# =========================================================
#TABLE-SPECIFIC TRANSFORMATIONS
# (Must match metadata.yml function names)
# =========================================================

def transform_customers(df: DataFrame, key_dict) -> DataFrame:
    print("Running customer transformations")
    df = apply_common_rules(df)

    # Add customer-specific logic here if needed
    df = validate_id_columns(df, key_dict)

    return df


def transform_products(df: DataFrame, key_dict) -> DataFrame:
    print("Running product transformations")
    df = apply_common_rules(df)

    # Add product-specific logic here if needed
    df = validate_id_columns(df, key_dict)

    return df


def transform_sales(df: DataFrame, key_dict) -> DataFrame:
    print("Running sales transformations")
    df = apply_common_rules(df)

    # Add sales-specific logic here if needed
    df = validate_id_columns(df, key_dict)

    return df


def transform_stores(df: DataFrame, key_dict) -> DataFrame:
    print("Running stores transformations")
    df = apply_common_rules(df)

    # Store IDs have no strict format in metadata, but we still validate
    df = validate_id_columns(df, key_dict)

    return df


def transform_calendar(df: DataFrame, key_dict) -> DataFrame:
    print("Running calendar transformations")
    df = apply_common_rules(df)

    # Add calendar-specific logic here if needed
    df = validate_id_columns(df, key_dict)

    return df