# Databricks notebook source
from pyspark.sql import functions as F

sales_df = spark.table("dbx_capstone.silver.cleaned_sales")
products_df = spark.table("dbx_capstone.silver.cleaned_products")
customers_df = spark.table("dbx_capstone.silver.cleaned_customers")

# COMMAND ----------

print("sales rows:", sales_df.count())
print("unique sales customer_id:", sales_df.select("customer_id").distinct().count())
print("unique sales product_id:", sales_df.select("product_id").distinct().count())

print("products rows:", products_df.count())
print("unique product_id:", products_df.select("product_id").distinct().count())

print("customers rows:", customers_df.count())
print("unique customer_id:", customers_df.select("customer_id").distinct().count())

# COMMAND ----------

expected_ids = [
    "C045939",
    "P023858",
    "P023859",
    "P023860",
    "P023861",
    "P023865",
    "P023869",
    "P023870",
    "P023871",
    "P023873"
]

sales_df.filter(F.col("customer_id").isin(expected_ids)).show(truncate=False)
sales_df.filter(F.col("product_id").isin(expected_ids)).show(truncate=False)
products_df.filter(F.col("product_id").isin(expected_ids)).show(truncate=False)
customers_df.filter(F.col("customer_id").isin(expected_ids)).show(truncate=False)

# COMMAND ----------

sales_df.select("customer_id").distinct().orderBy("customer_id").show(100, truncate=False)

# COMMAND ----------

sales_df.filter(F.col("customer_id").startswith("P")) \
    .select("customer_id", "product_id", "order_id", "order_date") \
    .show(100, truncate=False)

# COMMAND ----------

products_without_sales = (
    products_df.select("product_id")
    .join(
        sales_df.select("product_id").distinct(),
        on="product_id",
        how="left_anti"
    )
)

products_without_sales.show(50, truncate=False)
print(products_without_sales.count())

# COMMAND ----------

customers_without_sales = (
    customers_df.select("customer_id")
    .join(
        sales_df.select("customer_id").distinct(),
        on="customer_id",
        how="left_anti"
    )
)

products_without_sales = (
    products_df.select(F.col("product_id").alias("customer_id"))
    .join(
        sales_df.select(F.col("product_id").alias("customer_id")).distinct(),
        on="customer_id",
        how="left_anti"
    )
)

lapsed_customers = (
    customers_without_sales
    .unionByName(products_without_sales)
    .distinct()
    .orderBy("customer_id")
)