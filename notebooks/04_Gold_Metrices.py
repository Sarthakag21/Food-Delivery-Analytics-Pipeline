# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Importing necessary functions

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read Silver Tables

# COMMAND ----------

silver_df = spark.table("food_delivery.silver_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC #### KPI 1 - ORDER SUMMARY

# COMMAND ----------

gold_order_summary = (
    silver_df
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers"),
        countDistinct("restaurant_id").alias("total_restaurants"),
        countDistinct("delivery_partner_id").alias("total_partners"),
        round(sum("final_amount"),2).alias("total_revenue"),
        round(avg("final_amount"),2).alias("avg_order_value"),
        count(when(col("order_status")=="DELIVERED",1)).alias("delivered_orders"),
        count(when(col("order_status")=="CANCELLED",1)).alias("cancelled_orders")
    )
    .withColumn(
        "delivery_rate_pct",
        round(col("delivered_orders")*100/col("total_orders"),2)
    )
    .withColumn(
        "cancellation_rate_pct",
        round(col("cancelled_orders")*100/col("total_orders"),2)
    )
)

display(gold_order_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC #### KPI 2 - REVENUE BY CITY

# COMMAND ----------

gold_revenue_city = (
    silver_df
    .groupBy("delivery_city")
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("customers"),
        round(sum("final_amount"),2).alias("total_revenue"),
        round(avg("final_amount"),2).alias("avg_order_value")
    )
    .orderBy(col("total_revenue").desc())
)
display(gold_revenue_city)

# COMMAND ----------

# MAGIC %md
# MAGIC #### KPI 3 - RESTAURANT PERFORMANCE

# COMMAND ----------

restaurant_window = Window.orderBy(col("total_revenue").desc())

gold_restaurant_performance = (
    silver_df
    .groupBy(
        "restaurant_id",
        "restaurant_name",
        "cuisine_type",
        "delivery_city"
    )
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("final_amount"),2).alias("total_revenue"),
        round(avg("rating"),2).alias("avg_rating"),
        round(avg("delivery_time_minutes"),2).alias("avg_delivery_time")
    )
    .withColumn(
        "restaurant_rank",
        dense_rank().over(restaurant_window)
    )
)
display(gold_restaurant_performance)

# COMMAND ----------

# MAGIC %md
# MAGIC #### KPI 4 - CUSTOMER RFM

# COMMAND ----------

gold_customer_rfm = (
    silver_df
    .groupBy(
        "customer_id",
        "customer_name",
        "city",
        "loyalty_tier"
    )
    .agg(
        max("order_date").alias("last_order_date"),
        count("order_id").alias("frequency"),
        round(sum("final_amount"),2).alias("monetary")
    )
    .withColumn(
        "recency",
        datediff(current_date(),col("last_order_date"))
    )
    .withColumn(
        "recency_score",
        when(col("recency") <= 7,5)
        .when(col("recency") <= 30,4)
        .when(col("recency") <= 60,3)
        .when(col("recency") <= 120,2)
        .otherwise(1)
    )
    .withColumn(
        "frequency_score",
        ntile(5).over(
            Window.orderBy(col("frequency").desc())
        )
    )
    .withColumn(
        "monetary_score",
        ntile(5).over(
            Window.orderBy(col("monetary").desc())
        )
    )
    .withColumn(
        "rfm_score",
        col("recency_score")
        + col("frequency_score")
        + col("monetary_score")
    )
    .withColumn(
        "segment",
        when(col("rfm_score") >= 13,"Champion")
        .when(col("rfm_score") >= 10,"Loyal")
        .when(col("rfm_score") >= 7,"Potential")
        .when(col("rfm_score") >= 5,"At Risk")
        .otherwise("Lost")
    )
)
display(gold_customer_rfm)

# COMMAND ----------

# MAGIC %md
# MAGIC #### KPI 5 - TOP CUSTOMERS

# COMMAND ----------

customer_window = Window.orderBy(col("total_spend").desc())

gold_top_customers = (
    silver_df
    .groupBy(
        "customer_id",
        "customer_name",
        "city"
    )
    .agg(
        round(sum("final_amount"),2).alias("total_spend"),
        count("order_id").alias("total_orders")
    )
    .withColumn(
        "customer_rank",
        dense_rank().over(customer_window)
    )
    .filter(col("customer_rank") <= 100)
)
display(gold_top_customers.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### KPI 6 - DELIVERY PARTNER PERFORMANCE

# COMMAND ----------

gold_partner_performance = (
    silver_df
    .filter(col("order_status")=="DELIVERED")
    .groupBy(
        "delivery_partner_id",
        "partner_name",
        "vehicle_type"
    )
    .agg(
        count("order_id").alias("deliveries"),
        round(avg("delivery_time_minutes"),2).alias("avg_delivery_time"),
        round(avg("rating"),2).alias("avg_rating")
    )
    .orderBy(col("avg_rating").desc())
    .withColumn(
        "performance_tier",
        when(
            (col("avg_rating") >= 4.5) &
            (col("avg_delivery_time") <= 30),
            "STAR"
        )
        .when(
            (col("avg_rating") >= 4.0),
            "EXCELLENT"
        )
        .when(
            (col("avg_rating") >= 3.5),
            "GOOD"
        )
        .otherwise("POOR")
    )
)
display(gold_partner_performance.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### KPI 7 - PAYMENT ANALYSIS

# COMMAND ----------

gold_payment_analysis = (
    silver_df
    .groupBy("payment_method")
    .agg(
        count("order_id").alias("orders"),
        round(sum("final_amount"),2).alias("revenue"),
        round(avg("final_amount"),2).alias("avg_order_value")
    )
)
display(gold_payment_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC #### KPI 8 - PEAK HOURS

# COMMAND ----------

gold_peak_hours = (
    silver_df
    .withColumn("order_hour",hour("order_timestamp"))
    .groupBy("order_hour")
    .agg(
        count("order_id").alias("orders"),
        round(sum("final_amount"),2).alias("revenue")
    )
    .orderBy("order_hour")
)
display(gold_peak_hours)

# COMMAND ----------

# MAGIC %md
# MAGIC #### KPI 9 - MONTHLY TREND

# COMMAND ----------

monthly_window = Window.orderBy("order_month")

gold_monthly_trends = (
    silver_df
    .withColumn(
        "order_month",
        date_format("order_date","yyyy-MM")
    )
    .groupBy("order_month")
    .agg(
        count("order_id").alias("orders"),
        round(sum("final_amount"),2).alias("revenue"),
        countDistinct("customer_id").alias("customers")
    )
    .withColumn(
        "prev_month_revenue",
        lag("revenue").over(monthly_window)
    )
    .withColumn(
        "revenue_growth_pct",
        round(
            (
                (col("revenue")-col("prev_month_revenue"))
                / col("prev_month_revenue")
            )*100,
            2
        )
    )
)
display(gold_monthly_trends)

# COMMAND ----------

# MAGIC %md
# MAGIC #### KPI 10 - DELIVERY ANALYSIS

# COMMAND ----------

gold_delivery_analysis = (
    silver_df
    .groupBy(
        "delivery_city",
        "vehicle_type"
    )
    .agg(
        round(avg("delivery_time_minutes"),2)
        .alias("avg_delivery_time"),

        count(
            when(
                col("delivery_speed_category")=="FAST",
                1
            )
        ).alias("fast_deliveries"),

        count(
            when(
                col("delivery_speed_category")=="MEDIUM",
                1
            )
        ).alias("medium_deliveries"),

        count(
            when(
                col("delivery_speed_category")=="SLOW",
                1
            )
        ).alias("slow_deliveries")
    )
)
display(gold_delivery_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC #### KPI 11 - CUSTOMER RETENTION

# COMMAND ----------

gold_customer_retention = (
    silver_df
    .withColumn(
        "order_month",
        date_format("order_date","yyyy-MM")
    )
    .groupBy("customer_id")
    .agg(
        countDistinct("order_month")
        .alias("active_months")
    )
    .orderBy(col("active_months").desc())
)
display(gold_customer_retention.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI 12 - CUISINE PERFORMANCE

# COMMAND ----------

gold_cuisine_performance = (
    silver_df.groupBy("cuisine_type")
    .agg(
        count("order_id").alias("orders"),
        round(sum("final_amount"),2).alias("revenue"),
        round(avg("rating"),1).alias("avg_rating")
    )
)
display(gold_cuisine_performance)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. SAVE GOLD TABLES

# COMMAND ----------

gold_tables = {
    "gold_order_summary"            : gold_order_summary,
    "gold_revenue_city"             : gold_revenue_city,
    "gold_restaurant_performance"   : gold_restaurant_performance,
    "gold_customer_rfm"             : gold_customer_rfm,
    "gold_top_customers"            : gold_top_customers,
    "gold_partner_performance"      : gold_partner_performance,
    "gold_payment_analysis"         : gold_payment_analysis,
    "gold_peak_hours"               : gold_peak_hours,
    "gold_monthly_trends"           : gold_monthly_trends,
    "gold_delivery_analysis"        : gold_delivery_analysis,
    "gold_customer_retention"       : gold_customer_retention,
    "gold_cuisine_performance"      : gold_cuisine_performance
}

# COMMAND ----------

for table_name, df in gold_tables.items():

    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema","true")
       .saveAsTable(f"food_delivery.{table_name}"))

    print(f"✅ Saved → food_delivery.{table_name}")

# COMMAND ----------

unity_tables = {
    "/Volumes/workspace/default/project_csv/food_delivery/output/gold/order_summary"		  : gold_order_summary,
    "/Volumes/workspace/default/project_csv/food_delivery/output/gold/revenue_city"   		  : gold_revenue_city,
    "/Volumes/workspace/default/project_csv/food_delivery/output/gold/restaurant_performance" : gold_restaurant_performance,
    "/Volumes/workspace/default/project_csv/food_delivery/output/gold/customer_rfm"       	  : gold_customer_rfm,
    "/Volumes/workspace/default/project_csv/food_delivery/output/gold/top_customers"    	  : gold_top_customers,
    "/Volumes/workspace/default/project_csv/food_delivery/output/gold/partner_performance"    : gold_partner_performance,
    "/Volumes/workspace/default/project_csv/food_delivery/output/gold/payment_analysis"   	  : gold_payment_analysis,
    "/Volumes/workspace/default/project_csv/food_delivery/output/gold/peak_hours"       	  : gold_peak_hours,
    "/Volumes/workspace/default/project_csv/food_delivery/output/gold/monthly_trends"    	  : gold_monthly_trends,
	"/Volumes/workspace/default/project_csv/food_delivery/output/gold/delivery_analysis"      : gold_delivery_analysis,
    "/Volumes/workspace/default/project_csv/food_delivery/output/gold/customer_retention"     : gold_customer_retention,
    "/Volumes/workspace/default/project_csv/food_delivery/output/gold/cuisine_performance"    : gold_cuisine_performance
}

for path, tbl in unity_tables.items():
    (tbl.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )

    print(f"✅ Saved → {path}")