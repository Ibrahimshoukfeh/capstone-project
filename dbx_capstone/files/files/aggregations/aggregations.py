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
    # customer_2023 = (
    #     sales_df
    #     .filter(F.col("order_date").isNotNull())
    #     .filter(F.year("order_date") == 2023)
    #     .select("customer_id")
    #     .distinct()
    # )
    # customer_2024 = (
    #     sales_df
    #     .filter(F.col("order_date").isNotNull())
    #     .filter(F.year("order_date") == 2024)
    #     .select("customer_id")
    #     .distinct()
    # )
    # lapsed_customers = (
    #     customer_2023
    #     .join(customer_2024, on="customer_id", how="leftanti")
    #     .select("customer_id")
    # )
    sales_products_df = sales_df.join(products_df, on="product_id", how="left")
 
    customers_2023 = (
        sales_products_df
        .filter(F.year("order_date") == 2023)
        .select("customer_id")
        .distinct()
    )
 
    customers_2024 = (
        sales_products_df
        .filter(F.year("order_date") == 2024)
        .select("customer_id")
        .distinct()
    )
 
    lapsed_customers = (
        customers_2023
        .join(customers_2024, on="customer_id", how="left_anti")
    )
    # =========================
    # 3. STORE PERFORMANCE
    # =========================
    store_performance = (
        sales_df
        .filter(F.col("order_date").isNotNull())
        .join(
            stores_df.select("store_id", "store_name", "city", "country", "store_type"),
            on="store_id",
            how="left"
        )
        .groupBy("store_id", "store_name", "city", "country", "store_type")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("order_id").alias("total_orders")
        )
        .orderBy(F.col("total_revenue").desc())
    )
        
    # =========================
    # 4. LOYALTY ANALYSIS
    # =========================
    order_counts = (
       sales_df
       .groupBy("customer_id")
       .agg(F.countDistinct("order_id").alias("total_orders"))
   )
    loyalty_metric = (
    order_counts
    .withColumn(
        "loyalty_tier",
        F.when(F.col("total_orders") == 1, "One-Time Buyer")
         .when(F.col("total_orders") <= 3, "Occasional Buyer")
         .otherwise("Loyal Customer")
    )
    .select("customer_id", "total_orders", "loyalty_tier")
    .orderBy(F.col("total_orders").desc())
)
    # loyalty_metric = (
    #     order_counts
    #     .join(customers_df, on="customer_id", how="left")
    #     .withColumn(
    #         "loyalty_tier",
    #         F.when(F.col("total_orders") == 1, "One-Time Buyer")
    #          .when(F.col("total_orders") <= 3,  "Occasional Buyer")
    #          .otherwise("Loyal Customer")
    #     )
    #     .select("customer_id", "total_orders", "loyalty_tier")
    #     .orderBy(F.col("total_orders").desc())                 
    # )

    # =========================
    # OUTPUT (DISPLAY OR SAVE)
    # =========================
    print("Monthly Metrics")
    display(monthly_metrics)
    # https://adb-693586493268953.13.azuredatabricks.net/editor/files/1996056115016906?o=693586493268953$0

    print("Lapsed Customers")
    display(lapsed_customers)

    print("Store Performance")
    display(store_performance)

    print("Loyalty Metrics")
    display(loyalty_metric)

    # OPTIONAL: Save as Gold tables
    monthly_metrics.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbx_capstone.gold.monthly_metrics")
    lapsed_customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbx_capstone.gold.lapsed_customers")
    store_performance.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbx_capstone.gold.store_metrics")
    loyalty_metric.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbx_capstone.gold.loyalty_metrics")


# Entry point for Databricks job
if __name__ == "__main__":
    main(spark)