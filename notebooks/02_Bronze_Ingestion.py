# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Importing necessary functions

# COMMAND ----------

from pyspark.sql.functions import (
    current_timestamp, lit, input_file_name,
    to_timestamp, to_date, col
)
import logging

# COMMAND ----------

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("BronzeIngestion")

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Unity catalog paths

# COMMAND ----------

BASE_PATH = "/Volumes/workspace/default/project_csv/food_delivery/input"
ORDERS_PATH      = f"{BASE_PATH}/orders.csv"
CUSTOMERS_PATH   = f"{BASE_PATH}/customers.csv"
RESTAURANTS_PATH = f"{BASE_PATH}/restaurants.csv"
PARTNERS_PATH    = f"{BASE_PATH}/partners.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Create database

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS food_delivery")
log.info("Database 'food_delivery' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ingest Orders -> Add ingestion metadata -> Inspect -> Save to delta table -> Save as table in DB

# COMMAND ----------

log.info(f"Reading orders from {ORDERS_PATH}")
 
orders_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ORDERS_PATH)
)

# COMMAND ----------

orders_df = (orders_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file",         col("_metadata.file_path"))
    .withColumn("bronze_layer",        lit("bronze"))
)

# COMMAND ----------

log.info(f"Orders schema:")
orders_df.printSchema()
log.info(f"Orders row count: {orders_df.count()}")
display(orders_df.limit(5))

# COMMAND ----------

(orders_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_date")
    .save("/Volumes/workspace/default/project_csv/food_delivery/output/bronze/orders"))
    
log.info("✅  Bronze layer saved → /Volumes/workspace/default/project_csv/food_delivery/output/bronze/orders")

# COMMAND ----------

bronze_orders_df = (
    spark.read
        .format("delta")
        .load("/Volumes/workspace/default/project_csv/food_delivery/output/bronze/orders")
)

(bronze_orders_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("food_delivery.bronze_orders"))

log.info("✅ food_delivery.bronze_orders written")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Ingest Customers -> Add ingestion metadata -> Inspect -> Save to delta table -> Save as table in DB

# COMMAND ----------

log.info(f"Reading customers from {CUSTOMERS_PATH}")
 
customers_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(CUSTOMERS_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file",         col("_metadata.file_path"))
    .withColumn("bronze_layer",        lit("bronze"))
)

# COMMAND ----------

log.info(f"Customers schema:")
customers_df.printSchema()
log.info(f"Customers row count: {customers_df.count()}")
display(customers_df.limit(5))

# COMMAND ----------

(customers_df.write
    .format("delta")
    .mode("overwrite")
    .save("/Volumes/workspace/default/project_csv/food_delivery/output/bronze/customers")
)

log.info("✅  Bronze layer saved → /Volumes/workspace/default/project_csv/food_delivery/output/bronze/customers")

# COMMAND ----------

bronze_customers_df = (
    spark.read
        .format("delta")
        .load("/Volumes/workspace/default/project_csv/food_delivery/output/bronze/customers")
)

(bronze_customers_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("food_delivery.bronze_customers")
)
log.info("✅ food_delivery.bronze_customers written")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Ingest Restaurants -> Add ingestion metadata -> Inspect -> Save to delta table -> Save as table in DB

# COMMAND ----------

log.info(f"Reading restaurants from {RESTAURANTS_PATH}")

restaurants_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(RESTAURANTS_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file",         col("_metadata.file_path"))
    .withColumn("bronze_layer",        lit("bronze"))
)

# COMMAND ----------

log.info(f"Restaurants schema:")
restaurants_df.printSchema()
log.info(f"Restaurants row count: {restaurants_df.count()}")
display(restaurants_df.limit(5))

# COMMAND ----------

(restaurants_df.write
    .format("delta")
    .mode("overwrite")
    .save("/Volumes/workspace/default/project_csv/food_delivery/output/bronze/restaurants"))
log.info("✅  Bronze layer saved → /Volumes/workspace/default/project_csv/food_delivery/output/bronze/restaurants")

# COMMAND ----------

bronze_restaurants_df = (
    spark.read
        .format("delta")
        .load("/Volumes/workspace/default/project_csv/food_delivery/output/bronze/restaurants")
)

(bronze_restaurants_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("food_delivery.bronze_restaurants")
)
log.info("✅ food_delivery.bronze_restaurants written")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Ingest Delivery partners -> Add ingestion metadata -> Inspect -> Save to delta table -> Save as table in DB

# COMMAND ----------

log.info(f"Reading delivery partners from {PARTNERS_PATH}")

partners_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(PARTNERS_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file",         col("_metadata.file_path"))
    .withColumn("bronze_layer",        lit("bronze"))
)

# COMMAND ----------

log.info(f"Partners schema:")
partners_df.printSchema()
log.info(f"Partners row count: {partners_df.count()}")
display(partners_df.limit(5))

# COMMAND ----------

(partners_df.write
    .format("delta")
    .mode("overwrite")
    .save("/Volumes/workspace/default/project_csv/food_delivery/output/bronze/partners"))
log.info("✅  Bronze layer saved → /Volumes/workspace/default/project_csv/food_delivery/output/bronze/partners")

# COMMAND ----------

bronze_partners_df = (
    spark.read
        .format("delta")
        .load("/Volumes/workspace/default/project_csv/food_delivery/output/bronze/partners")
)

(bronze_partners_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("food_delivery.bronze_partners")
)
log.info("✅ food_delivery.bronze_partners written")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Verify all bronze tables

# COMMAND ----------

print("\n" + "="*60)
print("  BRONZE INGESTION SUMMARY")
print("="*60)
tables = ["bronze_orders", "bronze_customers", "bronze_restaurants", "bronze_partners"]
for t in tables:
    count = spark.table(f"food_delivery.{t}").count()
    print(f"  {t:<30} → {count:>5} rows")
print("="*60)
print("  ✅ Bronze Layer Complete")
print("="*60)

# COMMAND ----------

