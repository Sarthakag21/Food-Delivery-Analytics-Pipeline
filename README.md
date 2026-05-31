# 🍕 Food Delivery Analytics Platform
### End-to-End Data Engineering Pipeline | Medallion Architecture

<p align="center">
  <img src="https://img.shields.io/badge/PySpark-3.5-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Databricks-Platform-FF3621?style=for-the-badge&logo=databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/Delta_Lake-Storage-00ADD8?style=for-the-badge&logo=delta&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache_Airflow-2.9-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/Unity_Catalog-Governance-1B3A57?style=for-the-badge&logo=databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/Python-3.10-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Architecture-Medallion-gold?style=flat-square"/>
  <img src="https://img.shields.io/badge/Layers-Bronze%20%7C%20Silver%20%7C%20Gold-blue?style=flat-square"/>
  <img src="https://img.shields.io/badge/Gold_Tables-12-green?style=flat-square"/>
  <img src="https://img.shields.io/badge/Orders-5000%2B-orange?style=flat-square"/>
  <img src="https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square"/>
</p>

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Business Problem](#-business-problem)
- [Technology Stack](#-technology-stack)
- [Architecture](#-architecture)
- [Dataset Information](#-dataset-information)
- [Medallion Architecture](#-medallion-architecture)
- [Databricks Workflow](#-databricks-workflow)
- [Gold KPI Tables](#-gold-kpi-tables)
- [Airflow Orchestration](#-airflow-orchestration)
- [Project Structure](#-project-structure)
- [How to Run](#-how-to-run)
- [Pipeline Monitoring](#-pipeline-monitoring)
- [Future Enhancements](#-future-enhancements)
- [Author](#-author)

---

## 🎯 Project Overview

This project implements a **production-grade, end-to-end Data Engineering pipeline** for a Food Delivery Analytics Platform. It processes high-volume transactional data across multiple business domains — customers, restaurants, delivery partners, and orders — and transforms raw data into business-ready KPIs.

The solution is built on **Databricks + Delta Lake + Unity Catalog**, orchestrated with **Apache Airflow**, and follows the **Medallion Architecture** (Bronze → Silver → Gold).

---

## 💼 Business Problem

Food delivery companies generate millions of transactional records daily. Raw data alone provides no actionable insight. This pipeline solves that by:

| Challenge | Solution |
|-----------|----------|
| Raw unstructured data from multiple sources | Unified Bronze ingestion layer |
| Inconsistent data quality and nulls | Silver cleaning & validation |
| No visibility into KPIs | 12 Gold analytics tables |
| Manual pipeline runs | Automated Airflow DAG |
| No audit trail | Delta Lake versioning + audit columns |
| Scattered storage | Unity Catalog Volumes (governed) |

---

## 🛠️ Technology Stack

| Category | Technology |
|----------|-----------|
| **Data Processing** | PySpark, Spark SQL |
| **Data Platform** | Databricks (Serverless) |
| **Storage Format** | Delta Lake |
| **Data Governance** | Unity Catalog |
| **Orchestration** | Apache Airflow 2.9 |
| **Raw Storage** | Unity Catalog Volumes |
| **Language** | Python 3.10 |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                                │
│   customers.csv  restaurants.csv  partners.csv  orders.csv      │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              UNITY CATALOG VOLUME (Raw Storage)                 │
│   /Volumes/workspace/default/project_csv/food_delivery/input/   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   🥉 BRONZE LAYER                               │
│              Raw Delta Tables — No Transforms                   │
│   bronze_customers │ bronze_restaurants │ bronze_partners        │
│                    │ bronze_orders      │                        │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   🥈 SILVER LAYER                               │
│        Cleaned · Standardized · Joined · Validated              │
│                     silver_orders                               │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   🥇 GOLD LAYER  (12 KPI Tables)                │
│  gold_order_summary    │ gold_revenue_city    │ gold_peak_hours  │
│  gold_restaurant_perf  │ gold_customer_rfm    │ gold_top_cust.   │
│  gold_partner_perf     │ gold_payment_analysis│ gold_monthly     │
│  gold_delivery_analysis│ gold_retention       │ gold_cuisine     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                  📊 BUSINESS ANALYTICS                          │
│         Reports │ Dashboards │ KPIs │ Executive Reporting       │
└─────────────────────────────────────────────────────────────────┘
                           ▲
                           │
┌─────────────────────────────────────────────────────────────────┐
│               ✈️  APACHE AIRFLOW (WSL Ubuntu)                   │
│   DAG: project_food_delivery │ DatabricksRunNowOperator          │
│   Retries │ Email Alerts │ Pipeline Summary │ Monitoring         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 Dataset Information

The pipeline processes **4 datasets** with **5000+ order records**.

<details>
<summary><b>📦 Orders (5000+ rows) — Fact Table</b></summary>

| Column | Type | Description |
|--------|------|-------------|
| `order_id` | INT | Unique order identifier |
| `customer_id` | INT | FK → Customers |
| `restaurant_id` | INT | FK → Restaurants |
| `delivery_partner_id` | INT | FK → Partners |
| `order_amount` | DOUBLE | Base order value |
| `discount_amount` | DOUBLE | Discount applied |
| `tip_amount` | DOUBLE | Tip given |
| `final_amount` | DOUBLE | order_amount - discount + tip |
| `order_status` | STRING | DELIVERED / CANCELLED / RETURNED |
| `rating` | DOUBLE | Customer rating (1–5) |

</details>

<details>
<summary><b>👤 Customers (1000+ rows)</b></summary>

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | INT | Primary key |
| `customer_name` | STRING | Full name |
| `gender` | STRING | M / F |
| `age` | INT | Age in years |
| `city` | STRING | Home city |
| `signup_date` | DATE | Account creation date |
| `loyalty_tier` | STRING | GOLD / SILVER / PLATINUM / REGULAR |

</details>

<details>
<summary><b>🍽️ Restaurants (200+ rows)</b></summary>

| Column | Type | Description |
|--------|------|-------------|
| `restaurant_id` | INT | Primary key |
| `restaurant_name` | STRING | Restaurant name |
| `cuisine_type` | STRING | Indian / Chinese / Italian etc. |
| `city` | STRING | Operating city |
| `restaurant_rating` | DOUBLE | Platform rating (1–5) |

</details>

<details>
<summary><b>🚴 Delivery Partners (500+ rows)</b></summary>

| Column | Type | Description |
|--------|------|-------------|
| `delivery_partner_id` | INT | Primary key |
| `partner_name` | STRING | Partner full name |
| `vehicle_type` | STRING | BIKE / SCOOTER / CYCLE |
| `joining_date` | DATE | Onboarding date |

</details>

---

## 🥇 Medallion Architecture

### 🥉 Bronze Layer — Raw Ingestion

> **Purpose:** Preserve raw data exactly as received. No transformations.

| What happens | Detail |
|---|---|
| Read CSV from Unity Catalog Volume | `/Volumes/.../food_delivery/input/` |
| Add audit columns | `ingestion_timestamp`, `source_file` |
| Write as Delta tables | Overwrite mode |

**Output Tables:**
```
food_delivery.bronze_customers
food_delivery.bronze_restaurants
food_delivery.bronze_partners
food_delivery.bronze_orders
```

---

### 🥈 Silver Layer — Cleaning & Transformation

> **Purpose:** Single clean, enriched, business-ready orders table.

| Operation | Detail |
|---|---|
| **Deduplication** | `dropDuplicates(["order_id"])` |
| **Null handling** | `fillna` on rating, discount_amount, tip_amount |
| **Type casting** | order_date → DATE, timestamps → TIMESTAMP |
| **Standardization** | `upper(trim(delivery_city))` |
| **Data Quality** | DQ flag + quarantine table for invalid rows |
| **LEFT JOINs** | Orders ↔ Customers, Restaurants, Partners (BROADCAST) |
| **Window Functions** | `dense_rank`, `lag`, running `sum` per customer |
| **Business columns** | `delivery_speed_category`, `high_value_order`, `is_weekend`, `meal_period`, `spend_trend` |

**Output Table:**
```
food_delivery.silver_orders   (partitioned by delivery_city)
```

---

### 🥇 Gold Layer — Business KPIs

> **Purpose:** 12 analytics-ready tables. Each function reads `silver_orders` independently.

| # | Table | Business Use |
|---|-------|-------------|
| 1 | `gold_order_summary` | Daily order volumes, revenue totals, status breakdown |
| 2 | `gold_revenue_city` | City-wise revenue, MoM trends, growth rates |
| 3 | `gold_restaurant_performance` | Restaurant rankings, completion rates, cuisine analysis |
| 4 | `gold_customer_rfm` | RFM scoring (Recency · Frequency · Monetary) |
| 5 | `gold_top_customers` | High-value customer leaderboard |
| 6 | `gold_partner_performance` | Partner speed, ratings, efficiency tiers |
| 7 | `gold_payment_analysis` | Payment method split (UPI / CARD / COD / WALLET) |
| 8 | `gold_peak_hours` | Order heatmap by hour and meal period |
| 9 | `gold_monthly_trends` | Month-over-month revenue and order trends |
| 10 | `gold_delivery_analysis` | Delivery time distribution, FAST/MEDIUM/SLOW breakdown |
| 11 | `gold_customer_retention` | Repeat vs first-time buyer cohorts |
| 12 | `gold_cuisine_performance` | Cuisine revenue, popularity, discount patterns |

---

## ⚙️ Databricks Workflow

**Workflow Name:** `food_delivery_pipeline`

```
┌────────────────────────┐
│   Task 1               │
│   bronze_ingestion_job  │  Notebook: 02_bronze_ingestion
│   Serverless Compute   │  → bronze_customers, bronze_restaurants,
└──────────┬─────────────┘    bronze_partners, bronze_orders
           │
           ▼
┌────────────────────────┐
│   Task 2               │
│   silver_transform_job  │  Notebook: 03_silver_transformation
│   Serverless Compute   │  → silver_orders
└──────────┬─────────────┘
           │
           ▼
┌────────────────────────┐
│   Task 3               │
│   gold_metrics_job      │  Notebook: 04_gold_metrics
│   Serverless Compute   │  → 12 Gold KPI tables
└────────────────────────┘
```

All tasks use **Serverless Compute** for cost efficiency and zero cluster management.

---

## ✈️ Airflow Orchestration

**DAG Name:** `project_food_delivery`
**Schedule:** Daily at 05:30 am IST
**Operator:** `DatabricksRunNowOperator`
**Job ID:** `529533761315639`

```
start
  │
  ▼
trigger_databricks_workflow   ← DatabricksRunNowOperator
  │                             Triggers food_delivery_pipeline
  ▼                             Waits for all 3 tasks to complete
pipeline_summary              ← Collects run metadata via XCom
  │
  ▼
email_notification            ← Sends HTML success/failure email
  │
  ▼
end
```

**Key Features:**
- `retries = 3` with exponential backoff
- Email alerts on failure and success
- Pipeline summary report in each run
- Task timeout protection
- XCom-based run metadata collection

---

## 📁 Project Structure

```
food_delivery_medallion/
│
├── README.md
│
├── notebooks/
|   ├── 01_generate_data.py          ← Generate 4 source CSVs
│   ├── 02_bronze_ingestion.py       ← Bronze: CSV → Delta
│   ├── 03_silver_transformation.py  ← Silver: clean + join + window fns
│   └── 04_gold_metrics.py           ← Gold: 12 KPI tables (function-per-table)
│
├── airflow/
│   └── project_food_delivery.py     ← Airflow DAG
│
├── config/
│   └── databricks_config.py         ← Workspace URL, job ID, paths
│
├── data/
|   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
|
├── screenshots/
│
└── requirements.txt
```

---

## 🚀 How to Run

### Step 1 — Generate Source Data

```python
# Run in Databricks notebook or locally
python 01_generate_data.py
```

Output location (Unity Catalog Volume):
```
/Volumes/workspace/default/project_csv/food_delivery/input/
├── customers.csv
├── restaurants.csv
├── partners.csv
└── orders.csv
```

---

### Step 2 — Upload Notebooks to Databricks

Import notebooks into your Databricks Workspace:
```
Workspace → Create folder → food_delivery_medallion/notebooks/
Import: 02_bronze_ingestion.py
Import: 03_silver_transformation.py
Import: 04_gold_metrics.py
```

---

### Step 3 — Create Databricks Workflow

1. Go to **Workflows** in Databricks UI
2. Click **Create Job** → Name: `food_delivery_pipeline`
3. Add tasks:

| Task Name | Notebook | Compute |
|-----------|----------|---------|
| `bronze_ingestion_job` | `02_bronze_ingestion` | Serverless |
| `silver_transformation_job` | `03_silver_transformation` | Serverless |
| `gold_metrics_job` | `04_gold_metrics` | Serverless |

4. Set dependencies: Bronze → Silver → Gold

---

### Step 4 — WSL Ubuntu + Airflow Setup

```bash
# In VSCode terminal (WSL Ubuntu) — run once
chmod +x scripts/setup_wsl_airflow.sh
bash scripts/setup_wsl_airflow.sh
```

This installs Airflow 2.9, the Databricks provider, creates the virtual environment, and initialises the Airflow database.

```bash
# Activate in every new terminal
source ~/activate_medallion.sh
```

---

### Step 5 — Configure Databricks Connection

```bash
airflow connections add databricks_default \
  --conn-type databricks \
  --conn-host https://adb-<workspace-id>.azuredatabricks.net \
  --conn-password <personal-access-token>
```

> **Get your token:** Databricks UI → Profile (top right) → User Settings → Developer → Access Tokens

---

### Step 6 — Start Airflow

```bash
# Terminal 1 — Scheduler
airflow scheduler

# Terminal 2 — API Server
airflow api-server

# Or from VSCode: Ctrl+Shift+P → Tasks: Run Task → Start Airflow
```

---

### Step 7 — Trigger the Pipeline

```bash
# Manual trigger
airflow dags trigger project_food_delivery

# Test run (dry run, no actual Databricks call)
airflow dags test project_food_delivery 2026-05-31

# View in UI
open http://localhost:8080
# Login: admin / admin123
```

---

### Step 8 — Verify Results

```sql
-- Check all tables created
SHOW TABLES IN food_delivery;

-- Bronze verification
SELECT COUNT(*) FROM food_delivery.bronze_orders;

-- Silver verification
SELECT delivery_city, COUNT(*) as orders
FROM food_delivery.silver_orders
GROUP BY delivery_city;

-- Gold KPIs
SELECT * FROM food_delivery.gold_order_summary;
SELECT * FROM food_delivery.gold_customer_rfm ORDER BY rfm_score DESC LIMIT 10;
SELECT * FROM food_delivery.gold_top_customers LIMIT 10;
SELECT * FROM food_delivery.gold_revenue_city ORDER BY total_revenue DESC;
SELECT * FROM food_delivery.gold_partner_performance ORDER BY global_speed_rank;
```

---

## 📈 Pipeline Monitoring

| Tool | What to Monitor |
|------|----------------|
| **Airflow UI** `localhost:8080` | DAG status, task logs, run history, XCom values |
| **Databricks Workflow UI** | Bronze / Silver / Gold execution logs, cluster metrics |
| **Email Notifications** | Success alerts with run summary, failure alerts with error trace |
| **Delta Lake History** | `DESCRIBE HISTORY food_delivery.silver_orders` |
| **Unity Catalog** | Table lineage, data governance, access audit |

---

## 👨‍💻 Author

**Sarthak Agarwal**

*Data Engineering Project*

<p>
  <a href="https://github.com/Sarthakag21">
    <img src="https://img.shields.io/badge/GitHub-sarthak--agarwal-181717?style=flat-square&logo=github"/>
  </a>
  <a href="https://www.linkedin.com/in/sarthak-agarwal-171281200/">
    <img src="https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=flat-square&logo=linkedin"/>
  </a>
</p>

---

<p align="center">
  <i>Built with ❤️ using Databricks · PySpark · Delta Lake · Apache Airflow</i>
</p>
