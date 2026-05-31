import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.utils.email import send_email

from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator
)

PROJECT_DIR = os.getenv(
    "PROJECT_DIR",
    str(Path(__file__).parent.parent)
)

if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from config.databricks_config import (
    JOB_ID,
    DAG_CONFIG,
    DATABRICKS_CONN_ID
)

log = logging.getLogger("FoodDelivery")

# ==========================================================
# Default Arguments
# ==========================================================

default_args = {
    "owner": DAG_CONFIG["owner"],
    "depends_on_past": False,
    "email": [DAG_CONFIG["alert_email"]],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": DAG_CONFIG["retries"],
    "retry_delay": timedelta(
        minutes=DAG_CONFIG["retry_delay_mins"]
    ),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(
        hours=DAG_CONFIG["timeout_hours"]
    ),
}

# ==========================================================
# Helper Functions
# ==========================================================

def pipeline_summary_fn(**context):

    ti = context["ti"]
    ds = context["ds"]
    ist = ZoneInfo("Asia/Kolkata")

    start_time = context["dag_run"].start_date.astimezone(ist)
    end_time = datetime.now(ist)

    duration = end_time - start_time

    summary = f"""
=========================================================
 FOOD DELIVERY MEDALLION PIPELINE SUMMARY
=========================================================
Execution Date : {ds}

Pipeline Status : SUCCESS

Databricks Job ID : {JOB_ID}

Start Time (UTC) : {start_time}
End Time   (UTC) : {end_time}

Total Duration : {duration}

Pipeline Completed Successfully

Layers Executed:
---------------------------------------------------------
1. Bronze Ingestion
2. Silver Transformation
3. Gold Metrics
---------------------------------------------------------

Gold Tables Generated:
---------------------------------------------------------
food_delivery.gold_order_summary
food_delivery.gold_revenue_city
food_delivery.gold_restaurant_performance
food_delivery.gold_customer_rfm
food_delivery.gold_top_customers
food_delivery.gold_partner_performance
food_delivery.gold_payment_analysis
food_delivery.gold_peak_hours
food_delivery.gold_monthly_trends
food_delivery.gold_delivery_analysis
food_delivery.gold_customer_retention
food_delivery.gold_cuisine_performance
=========================================================
"""

    log.info(summary)
    print(summary)

    ti.xcom_push(
        key="summary",
        value=summary
    )


def send_notification_fn(**context):

    summary = context["ti"].xcom_pull(
        task_ids="pipeline_summary",
        key="summary"
    )

    send_email(
        to=DAG_CONFIG["alert_email"],
        subject="✅ Food Delivery Pipeline Success",
        html_content=f"""
        <h2>Food Delivery Medallion Pipeline Completed Successfully</h2>
        <pre>{summary}</pre>
        """
    )

    log.info("Success email sent")

# ==========================================================
# DAG
# ==========================================================

with DAG(
    dag_id=DAG_CONFIG["dag_id"],
    description="Food Delivery Medallion Architecture",
    schedule=DAG_CONFIG["schedule_interval"],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=DAG_CONFIG["tags"]
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    databricks_pipeline = DatabricksRunNowOperator(
        task_id="databricks_food_delivery_pipeline",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=JOB_ID,
        do_xcom_push=True
    )

    summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary_fn
    )

    notify = PythonOperator(
        task_id="send_success_notification",
        python_callable=send_notification_fn
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    (
        start
        >> databricks_pipeline
        >> summary
        >> notify
        >> end
    )