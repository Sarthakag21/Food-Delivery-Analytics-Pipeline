# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Importing necessary functions

# COMMAND ----------

from pyspark.sql.functions import (
    # Basics
    col, lit, when, upper, lower, trim, coalesce,
    # Type casting
    to_timestamp, to_date, cast,
    # String
    concat_ws, regexp_replace, initcap,
    # Math
    round as spark_round, abs as spark_abs,
    # Aggregation
    sum, avg, min, max, count, countDistinct,
    # Date / Time
    datediff, months_between, current_date, current_timestamp, from_utc_timestamp,
    year, month, dayofweek, hour, date_format,
    # Window
    row_number, rank, dense_rank, ntile,
    lag, lead, first, last,
    percent_rank, cume_dist,
    # Misc
    broadcast, expr, struct
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType
import logging

# COMMAND ----------

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("SilverTransformation")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read Bronze Tables

# COMMAND ----------

log.info("Reading Bronze tables...")

orders_df      = spark.table("food_delivery.bronze_orders")
customers_df   = spark.table("food_delivery.bronze_customers")
restaurants_df = spark.table("food_delivery.bronze_restaurants")
partners_df    = spark.table("food_delivery.bronze_partners")

# COMMAND ----------

print(f"  Bronze Orders      : {orders_df.count():>6} rows")
print(f"  Bronze Customers   : {customers_df.count():>6} rows")
print(f"  Bronze Restaurants : {restaurants_df.count():>6} rows")
print(f"  Bronze Partners    : {partners_df.count():>6} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Cleaning Orders

# COMMAND ----------

# MAGIC %md
# MAGIC ##### a. Dropping duplicate order ids

# COMMAND ----------

orders_df = orders_df.dropDuplicates(["order_id"])
log.info(f"After dedup: {orders_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b. Fill nulls with defaults

# COMMAND ----------

orders_df = orders_df.fillna({
    "rating"          : 0.0,
    "discount_amount" : 0.0,
    "tip_amount"      : 0.0,
})

# COMMAND ----------

# MAGIC %md
# MAGIC ##### c. Standardize city names — uppercase

# COMMAND ----------

orders_df = orders_df.withColumn(
    "delivery_city", upper(trim(col("delivery_city")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### d. Recalculate final_amount after filling nulls

# COMMAND ----------

orders_df = orders_df.withColumn(
    "final_amount",
    spark_round(
        col("order_amount") - col("discount_amount") + col("tip_amount"), 2
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### e. Cast data types correctly

# COMMAND ----------

orders_df = (orders_df
    .withColumn("order_date",      to_date(col("order_date")))
    .withColumn("order_timestamp", to_timestamp(col("order_timestamp")))
    .withColumn("order_amount",    col("order_amount").cast(DoubleType()))
    .withColumn("final_amount",    col("final_amount").cast(DoubleType()))
    .withColumn("delivery_time_minutes", col("delivery_time_minutes").cast(IntegerType()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### f. Data Quality flags

# COMMAND ----------

orders_df = orders_df.withColumn(
    "dq_flag",
    when(col("order_amount") <= 0,                  lit("INVALID_AMOUNT"))
    .when(col("final_amount") < 0,                  lit("NEGATIVE_FINAL"))
    .when(col("delivery_time_minutes") > 180,       lit("EXCESS_DELIVERY_TIME"))
    .when(col("rating") > 5,                        lit("INVALID_RATING"))
    .when(~col("order_status").isin(
        "DELIVERED", "CANCELLED", "RETURNED"),      lit("UNKNOWN_STATUS"))
    .otherwise(lit("OK"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Quarantine & Clean rows

# COMMAND ----------

clean_orders = orders_df.filter(col("dq_flag") == "OK").drop("dq_flag")
quarantine   = orders_df.filter(col("dq_flag") != "OK")
print(f"  Clean rows     : {clean_orders.count()}")
print(f"  Quarantine rows: {quarantine.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Saving Quarantine rows in DB table & unity catalog

# COMMAND ----------

(quarantine.write.format("delta").mode("overwrite")
    .saveAsTable("food_delivery.quarantine_orders"))

# COMMAND ----------

(quarantine.write
    .format("delta")
    .mode("overwrite")
    .save("/Volumes/workspace/default/project_csv/food_delivery/output/silver/quarantine_orders"))
    
log.info("✅  Quarantine orders saved → /Volumes/workspace/default/project_csv/food_delivery/output/silver/quarantine_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Joins

# COMMAND ----------

# MAGIC %md
# MAGIC ##### a. Orders ↔ Customers

# COMMAND ----------

silver_df = clean_orders.join(
    broadcast(customers_df.select(
        "customer_id", "customer_name", "gender", "age",
        "city", "signup_date", "loyalty_tier"
    )),
    on="customer_id",
    how="left"
)
display(silver_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b. Orders ↔ Restaurants

# COMMAND ----------

silver_df = silver_df.join(
    broadcast(restaurants_df.select(
        "restaurant_id", "restaurant_name", "cuisine_type", "avg_preparation_time"
    )),
    on="restaurant_id",
    how="left"
)
display(silver_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### c. Orders ↔ Delivery

# COMMAND ----------

silver_df = silver_df.join(
    broadcast(partners_df.select(
        col("delivery_partner_id"), "partner_name", "vehicle_type", "joining_date"
    )),
    on="delivery_partner_id",
    how="left"
)
display(silver_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Delivery speed category

# COMMAND ----------

silver_df = (silver_df
    .withColumn(
        "delivery_speed_category",
        when(col("delivery_time_minutes") <= 30,  lit("FAST"))
        .when(col("delivery_time_minutes") <= 60, lit("MEDIUM"))
        .otherwise(lit("SLOW"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. High value order

# COMMAND ----------

silver_df = (silver_df
    .withColumn(
        "high_value_order",
        when(col("final_amount") >= 1000, lit(1)).otherwise(lit(0))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Is weekend - True/False

# COMMAND ----------

silver_df = (silver_df
.withColumn(
        "is_weekend",
        when(dayofweek(col("order_date")).isin(1, 7), lit(True)).otherwise(lit(False))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Window Functions

# COMMAND ----------

customer_window = (
    Window.partitionBy("customer_id")
          .orderBy("order_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### a. Customer order number

# COMMAND ----------

silver_df = silver_df.withColumn(
    "customer_order_number",
    row_number().over(customer_window)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b. Previous order amount

# COMMAND ----------

silver_df = silver_df.withColumn(
    "previous_order_amount",
    lag("final_amount").over(customer_window)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### c. Running customer revenue

# COMMAND ----------

silver_df = silver_df.withColumn(
    "running_customer_revenue",
    sum("final_amount").over(customer_window)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Current Silver load time
# MAGIC

# COMMAND ----------

silver_df = silver_df.withColumn(
    "silver_load_time",
    from_utc_timestamp(current_timestamp(), "Asia/Kolkata")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Summary Records

# COMMAND ----------

dq_summary = spark.createDataFrame([
    ("total_records", orders_df.count()),
    ("valid_records", clean_orders.count()),
    ("quarantine_records", quarantine.count())
], ["metric","value"])

dq_summary.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13. Save as table in DB -> Save to delta table

# COMMAND ----------

(silver_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("delivery_city")
    .saveAsTable("food_delivery.silver_orders")
)

# COMMAND ----------

(silver_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("delivery_city")
    .save("/Volumes/workspace/default/project_csv/food_delivery/output/silver/delivery"))
    
log.info("✅  Silver layer saved → /Volumes/workspace/default/project_csv/food_delivery/output/silver/delivery")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14. Silver Layer Summary

# COMMAND ----------

count = spark.table("food_delivery.silver_orders").count()
print(f"\n{'='*50}")
print(f"  SILVER COMPLETE — {count} rows written")
print(f"  Partitioned by : delivery_city")
print(f"  Format         : Delta Lake")
print(f"  Quarantined    : {quarantine.count()} rows")
print(f"{'='*50}")

# COMMAND ----------

display(silver_df.select(
    "order_id", "customer_name", "restaurant_name", "partner_name",
    "delivery_city", "final_amount", "order_status",
    "delivery_speed_category", "high_value_order", "is_weekend", "silver_load_time"
).limit(10))