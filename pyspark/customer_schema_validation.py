from pyspark.sql import SparkSession
import requests
import traceback
from datetime import datetime

WEBHOOK_URL = "YOUR_N8N_WEBHOOK_URL"

spark = SparkSession.builder \
    .appName("Schema_Validation") \
    .getOrCreate()

try:

    expected_columns = [
        "customer_id",
        "name",
        "city"
    ]

    df = spark.read \
        .option("header", True) \
        .csv("customer.csv")

    missing_columns = [
        c for c in expected_columns
        if c not in df.columns
    ]

    if missing_columns:

        raise Exception(
            f"Schema Drift Error: Missing columns {missing_columns}"
        )

except Exception as e:

    payload = {
        "pipeline_name": "customer_master_pipeline",
        "job_name": "customer_schema_validation",
        "job_type": "pyspark",
        "environment": "dev",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "error": str(e),
        "stacktrace": traceback.format_exc()
    }

    requests.post(
        WEBHOOK_URL,
        json=payload
    )

finally:
    spark.stop()
