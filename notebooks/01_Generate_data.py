# Databricks notebook source
pip install faker

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Import necessary libraries

# COMMAND ----------

from faker import Faker
import pandas as pd
import random
import os

fake = Faker("en_IN")
random.seed(42)
Faker.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Master lists

# COMMAND ----------

cities = [
    "Delhi", "Mumbai", "Bangalore", "Hyderabad",
    "Pune", "Chennai", "Jaipur", "Chandigarh",
    "Kanpur", "Kolkata", "Ahmedabad"
]

# COMMAND ----------

payment_methods = ["UPI", "CARD", "COD", "WALLET"]
order_statuses = ["DELIVERED", "CANCELLED", "RETURNED"]
loyalty_tiers = ["GOLD", "SILVER", "PLATINUM", "REGULAR"]
vehicle_types = ["BIKE", "SCOOTER", "CYCLE"]
cuisine_types = ["Indian", "Chinese", "Italian", "Fast Food", "Desserts"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Weights for realistic distribution

# COMMAND ----------

STATUS_WEIGHTS = [75, 18, 7]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Output Path

# COMMAND ----------

OUTPUT_PATH = "/Volumes/workspace/default/project_csv/food_delivery/input"

os.makedirs(OUTPUT_PATH, exist_ok=True)

print(f"Saving files to: {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. DATASET 1 — CUSTOMERS  (1000 rows)

# COMMAND ----------

print("Generating Customers...")

customers = []

for i in range(1, 1001):

    customers.append({
        "customer_id": i,
        "customer_name": fake.name(),
        "gender": random.choice(["M", "F"]),
        "age": random.randint(18, 60),
        "city": random.choice(cities),
        "signup_date": fake.date_between(
            start_date="-2y",
            end_date="today"
        ),
        "loyalty_tier": random.choice(loyalty_tiers)
    })

customers_df = pd.DataFrame(customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. DATASET 2 — RESTAURANTS  (200 rows)

# COMMAND ----------

print("Generating Restaurants...")

restaurants = []

for i in range(1, 201):

    restaurants.append({
        "restaurant_id": i,
        "restaurant_name": fake.company(),
        "cuisine_type": random.choice(cuisine_types),
        "city": random.choice(cities),
        "avg_preparation_time": random.randint(10, 45),
        "restaurant_rating": round(
            random.uniform(1, 5),
            1
        )
    })

restaurants_df = pd.DataFrame(restaurants)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. DATASET 3 — DELIVERY PARTNERS  (500 rows)

# COMMAND ----------

print("Generating Delivery Partners...")

partners = []

for i in range(1, 501):

    partners.append({
        "delivery_partner_id": i,
        "partner_name": fake.name(),
        "vehicle_type": random.choice(vehicle_types),
        "joining_date": fake.date_between(
            start_date="-3y",
            end_date="today"
        ),
        "city": random.choice(cities)
    })

partners_df = pd.DataFrame(partners)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. DATASET 4 — ORDERS  (5000 rows)

# COMMAND ----------

print("Generating Orders...")

orders = []

for i in range(1, 5001):

    amount = round(
        random.uniform(100, 1500),
        2
    )

    discount = round(
        random.uniform(0, 300),
        2
    )

    tip = round(
        random.uniform(10, 100),
        2
    )

    if random.random() < 0.04:
        tip = None

    if random.random() < 0.03:
        discount = None

    orders.append({
        "order_id": i,
        "customer_id": random.randint(1, 1000),
        "restaurant_id": random.randint(1, 200),
        "delivery_partner_id": random.randint(1, 500),
        "order_date": fake.date_between(
            start_date="-1y",
            end_date="today"
        ),
        "order_timestamp": fake.date_time_this_year(),
        "delivery_city": random.choice(cities),
        "payment_method": random.choice(payment_methods),
        "order_status": random.choices(
            order_statuses,
            weights=STATUS_WEIGHTS
        )[0],
        "delivery_time_minutes": random.randint(15, 90),
        "order_amount": amount,
        "tip_amount": tip,
        "discount_amount": discount,
        "final_amount": round(
            amount - (discount or 0) + (tip or 0),
            2
        ),
        "rating": (
            round(random.uniform(1, 5), 1)
            if random.random() > 0.05
            else None
        )
    })

orders_df = pd.DataFrame(orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. SAVE FILES

# COMMAND ----------

customers_df.to_csv(
    f"{OUTPUT_PATH}/customers.csv",
    index=False
)

restaurants_df.to_csv(
    f"{OUTPUT_PATH}/restaurants.csv",
    index=False
)

partners_df.to_csv(
    f"{OUTPUT_PATH}/partners.csv",
    index=False
)

orders_df.to_csv(
    f"{OUTPUT_PATH}/orders.csv",
    index=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. SUMMARY

# COMMAND ----------

print("\n" + "=" * 60)
print("DATA GENERATION COMPLETED")
print("=" * 60)

print(f"Customers    : {len(customers_df):,}")
print(f"Restaurants  : {len(restaurants_df):,}")
print(f"Partners     : {len(partners_df):,}")
print(f"Orders       : {len(orders_df):,}")

print("\nFiles saved successfully:")

print(f"{OUTPUT_PATH}/customers.csv")
print(f"{OUTPUT_PATH}/restaurants.csv")
print(f"{OUTPUT_PATH}/partners.csv")
print(f"{OUTPUT_PATH}/orders.csv")

print("\nSUCCESS")