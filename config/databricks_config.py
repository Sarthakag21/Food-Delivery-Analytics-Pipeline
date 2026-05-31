# Databricks Connection

DATABRICKS_CONN_ID = "databricks_default"

JOB_ID = 529533761315639

NOTEBOOKS = {

    "generate_data":
        "/Workspace/Project/3_Food_Delivery/01_Generate_data",

    "bronze_ingestion":
        "/Workspace/Project/3_Food_Delivery/02_Bronze_Ingestion",

    "silver_transformation":
        "/Workspace/Project/3_Food_Delivery/03_Silver_Transformation",

    "gold_metrices":
        "/Workspace/Project/3_Food_Delivery/04_Gold_Metrices"
}

DAG_CONFIG = {
    "dag_id": "project_food_delivery",
    "owner": "sarthak",
    "alert_email": "sarthakag2002@gmail.com",
    "retries": 2,
    "retry_delay_mins": 5,
    "timeout_hours": 2,
    "schedule_interval": "@daily",
    "tags": ["databricks", "pyspark", "medallion"]
}
