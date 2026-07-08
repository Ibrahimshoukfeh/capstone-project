## Databricks notebook source
#  dbx_capstone_gold_metrics.py

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main(spark):

    # =========================
    # LOAD TABLES
    # =========================
    sales_df = spark.table("dbx_capstone.silver.cleaned_sales")
    products_df = spark.table("dbx_capstone.silver.cleaned_products")
    stores_df = spark.table("dbx_capstone.silver.cleaned_stores")
    customers_df = spark.table("dbx_capstone.silver.cleaned_customers")

    # =========================
    # 1. MONTHLY REVENUE + MoM GROWTH
    # =========================
    monthly_calc = (
        sales_df
        .filter(F.col("order_date").isNotNull())
        .withColumn("year", F.year("order_date"))
        .withColumn("month", F.month("order_date"))
        .groupBy("year", "month")
        .agg(F.round(F.sum("revenue"), 2).alias("monthly_revenue"))
    )

    window_spec = Window.orderBy("year", "month")

    monthly_metrics = (
        monthly_calc
        .withColumn("prev_month_revenue", F.lag("monthly_revenue").over(window_spec))
        .withColumn(
            "mom_growth_rate",
            F.when(F.col("prev_month_revenue").isNull(), None)
             .when(F.col("prev_month_revenue") == 0, None)
             .otherwise(
                 F.round(
                     ((F.col("monthly_revenue") - F.col("prev_month_revenue")) 
                      / F.col("prev_month_revenue")) * 100,
                     2
                 )
             )
        )
    )

    # =========================
    # 2. Lapsed Customers (2023 -> 2024)
    # =========================
    
    # =========================
    # 3. STORE PERFORMANCE
    # =========================
   
    # =========================
    # 4. LOYALTY ANALYSIS
    # =========================
   

    # =========================
    # OUTPUT (DISPLAY OR SAVE)
    # =========================
    print("Monthly Metrics")
    display(monthly_metrics)https://adb-693586493268953.13.azuredatabricks.net/editor/files/1996056115016906?o=693586493268953$0

    print("Lapsed Customers")
    display(lapsed_customers)

    print("Store Metrics")
    display(store_metrics)

    print("Loyalty Metrics")
    display(loyalty_metrics)

    # OPTIONAL: Save as Gold tables
    monthly_metrics.write.mode("overwrite").saveAsTable("dbx_capstone.gold.monthly_metrics")
    lapsed_customers.write.mode("overwrite").saveAsTable("dbx_capstone.gold.lapsed_customers")
    store_metrics.write.mode("overwrite").saveAsTable("dbx_capstone.gold.store_metrics")
    loyalty_metrics.write.mode("overwrite").saveAsTable("dbx_capstone.gold.loyalty_metrics")


# Entry point for Databricks job
if __name__ == "__main__":
    main(spark)