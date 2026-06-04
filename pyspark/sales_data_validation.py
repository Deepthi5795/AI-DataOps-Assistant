from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import requests
import traceback

WEBHOOK_URL = "YOUR_N8N_WEBHOOK_URL"

spark = SparkSession.builder \
    .appName("AI_DataOps_Demo") \
    .getOrCreate()

try:

    print("Reading CSV...")

    df = spark.read \
        .option("header", True) \
        .csv("sales.csv")

    print("Casting amount column...")

    df = df.withColumn(
        "amount",
        col("amount").cast("int")
    )

    invalid_count = df.filter(
        col("amount").isNull()
    ).count()

    if invalid_count > 0:

        raise Exception(
            f"Data Quality Error: {invalid_count} invalid amount values found"
        )

    print("Data validation successful")

    df.show()

except Exception as e:

    error_message = str(e)

    print("Failure detected")
    print(error_message)

    from datetime import datetime

    from datetime import datetime

    payload = {
        "pipeline_name": "customer_sales_pipeline",
        "job_name": "sales_data_validation",
        "job_type": "pyspark",
        "environment": "dev",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "error": error_message,
        "stacktrace": traceback.format_exc()
    }    

    try:

        requests.post(
            WEBHOOK_URL,
            json=payload,
            timeout=10
        )

        print("Error sent to n8n")

    except Exception as webhook_error:

        print(
            f"Failed to send webhook: {webhook_error}"
        )

finally:

    spark.stop()
